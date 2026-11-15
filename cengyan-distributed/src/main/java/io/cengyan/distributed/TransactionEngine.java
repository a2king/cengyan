package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

/**
 * Storage 端事务协调器。所有可恢复状态都使用 Data Raft 条件批次持久化。
 * 已提交版本不立即物化删除，读者对每个事务固定一次决定，旧版本仍可读取。
 */
public final class TransactionEngine {
    /** 单份事务计划的默认上限。提交时新旧记录各带一份，因此 Storage 的 RPC 上限至少要到两倍加 64KB。 */
    public static final int DEFAULT_PLAN_MAX_BYTES = 16 * 1024 * 1024;
    public static final int MIN_PLAN_MAX_BYTES = 1024 * 1024;
    public static final int MAX_PLAN_MAX_BYTES = 64 * 1024 * 1024;
    private static volatile int planMaxBytes = DEFAULT_PLAN_MAX_BYTES;
    public static final ByteString TX_PREFIX = text("__tx/");
    /** 已结束事务的完整计划。键不在 {@link #TX_PREFIX} 下，扫描协调记录时不会读到它。 */
    public static final ByteString TX_PLAN_PREFIX = text("__txplan/");
    /** 还没做过版本压缩的已结束事务。恢复只扫这些小记录，到期后再点读计划。 */
    public static final ByteString TX_GC_PREFIX = text("__txgc/");
    private final StorageGateway storage;
    private final Executor participants;
    /** 本进程正在执行的事务。恢复线程跳过它们，避免把进行中的提交再跑一遍。 */
    private static final java.util.Set<String> ACTIVE = java.util.concurrent.ConcurrentHashMap.newKeySet();
    public TransactionEngine(StorageGateway storage) { this(storage, Runnable::run); }
    public TransactionEngine(StorageGateway storage, Executor participants) {
        this.storage = storage; this.participants = participants;
    }
    public static boolean active(String id) { return ACTIVE.contains(id); }
    /** @return false 表示其他线程仍在执行该事务 */
    public static boolean acquire(String id) {
        for (int i = 0; i < 200; i++) {
            if (ACTIVE.add(id)) return true;
            try { Thread.sleep(2); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "事务执行被中断", ex); }
        }
        return false;
    }
    public static void release(String id) { ACTIVE.remove(id); }

    /** Drain every submitted operation before deciding or returning: cancellation is not rollback. */
    private void phase(TransactionPlan plan, java.util.function.Consumer<ParticipantPlan> action) {
        RuntimeException failure = null;
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        for (ParticipantPlan participant : plan.getParticipantsList()) {
            try {
                pending.add(CompletableFuture.runAsync(StorageGateway.contextual(() -> action.accept(participant)), participants));
            } catch (RejectedExecutionException ex) {
                failure = new ClusterException("BUSY", "分片写入队列已满，事务等待恢复", ex); break;
            }
        }
        for (var future : pending) {
            try { future.get(); }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new ClusterException("QUERY_CANCELED", "事务执行中断，等待持久化事务恢复", ex);
            } catch (ExecutionException ex) {
                RuntimeException cause = ex.getCause() instanceof RuntimeException runtime ? runtime
                        : new ClusterException("FAILED", "分片写入失败", ex.getCause());
                if (failure == null || !deterministic(cause)) failure = cause;
            }
        }
        if (failure != null) throw failure;
    }
    private static boolean deterministic(RuntimeException ex) {
        return ex instanceof ClusterException cluster
                && Set.of("WRITE_CONFLICT", "INVALID_ARGUMENT", "REQUEST_CONFLICT", "TRANSACTION_TIMEOUT").contains(cluster.code());
    }
    public static ByteString text(String value) { return ByteString.copyFromUtf8(value); }
    /** 由 Storage 与 Compute 启动时按 transaction_plan_max_bytes 设置，两边必须一致。 */
    public static void setPlanMaxBytes(int bytes) {
        if (bytes < MIN_PLAN_MAX_BYTES || bytes > MAX_PLAN_MAX_BYTES) {
            throw new IllegalArgumentException("事务计划上限必须在 1 MiB 到 64 MiB 之间：" + bytes);
        }
        planMaxBytes = bytes;
    }
    public static int planMaxBytes() { return planMaxBytes; }
    public static ByteString transactionKey(String id) { return text("__tx/" + id); }
    public static ByteString transactionPlanKey(String id) { return text("__txplan/" + id); }
    public static ByteString transactionGcKey(String id) { return text("__txgc/" + id); }
    private static ByteString lockKey(ByteString key) { return text("__lock/").concat(key); }
    private static ByteString participantKey(String id) { return text("__participant/" + id); }

    public TransactionRecord execute(TransactionPlan input) {
        TransactionPlan plan = normalize(input);
        if (!acquire(plan.getTransactionId())) throw new ClusterException("BUSY", "事务正在执行，请稍后用同一 requestId 重试");
        try { return executeHeld(plan); }
        finally { release(plan.getTransactionId()); }
    }
    private TransactionRecord executeHeld(TransactionPlan plan) {
        long graph = plan.getGraphId(); int coordinator = plan.getCoordinatorPart();
        ByteString key = transactionKey(plan.getTransactionId());
        ByteString encoded = storage.get(graph, coordinator, key);
        if (encoded == null && fastEligible(plan) && System.currentTimeMillis() < plan.getDeadlineMs()) {
            try { return fastCommit(plan); }
            catch (ClusterException ex) {
                if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
                encoded = storage.get(graph, coordinator, key);
                if (encoded != null) {
                    TransactionRecord winner = parseRecord(encoded);
                    if (!winner.getPlan().getContentDigest().equals(plan.getContentDigest())) throw new ClusterException("REQUEST_CONFLICT", "相同事务 ID 对应不同写入计划");
                    if (winner.getFinalized()) return winner;
                }
            }
        }
        TransactionRecord record;
        if (encoded == null) {
            record = TransactionRecord.newBuilder().setPlan(plan).setState(TransactionState.PREPARING).build();
            try {
                storage.write(graph, coordinator, StorageGateway.batch("tx.begin/" + plan.getTransactionId())
                        .addConditions(StorageGateway.condition(key, null)).addMutations(put(key, record.toByteString())).build());
            } catch (ClusterException ex) {
                if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
                encoded = storage.get(graph, coordinator, key);
            }
        }
        if (encoded != null) record = parseRecord(encoded);
        else record = record(graph, coordinator, plan.getTransactionId());
        if (!record.getPlan().getContentDigest().equals(plan.getContentDigest())) throw new ClusterException("REQUEST_CONFLICT", "相同事务 ID 对应不同写入计划");
        return resume(record);
    }
    /** 单分片、无并发预留时一次条件写提交。语义与两阶段提交相同：整批同时可见，冲突则退回两阶段。 */
    private static boolean fastEligible(TransactionPlan plan) {
        return plan.getParticipantsCount() == 1 && !plan.getSchemaMigration() && !plan.getCompensation();
    }
    private TransactionRecord fastCommit(TransactionPlan plan) {
        long started = System.nanoTime();
        ParticipantPlan participant = plan.getParticipants(0);
        long graph = plan.getGraphId(); int part = participant.getPartId();
        List<ConditionalMutation> changes = participant.getChangesList();
        List<ByteString> locks = storage.getMany(graph, part, changes.stream().map(change -> lockKey(change.getKey())).toList());
        for (ByteString lock : locks) if (lock != null) throw new ClusterException("WRITE_CONFLICT", "键已被其他事务预留");
        long decidedAt = plan.getDeadlineMs();
        TransactionRecord record = TransactionRecord.newBuilder().setPlan(plan).setState(TransactionState.COMMITTED)
                .setFinalized(true).setDecidedAtMs(decidedAt).build();
        ByteString txKey = transactionKey(plan.getTransactionId());
        var command = StorageGateway.batch("tx.fast/" + plan.getTransactionId());
        command.addConditions(StorageGateway.condition(text("__schema/writeGate"), null));
        command.addConditions(StorageGateway.condition(text("__schema/generation"), plan.getGeneration() == 0 ? null : text(Long.toString(plan.getGeneration()))));
        command.addConditions(StorageGateway.condition(text("__schema/writeEpoch"), plan.getWriteEpoch() == 0 ? null : text(Long.toString(plan.getWriteEpoch()))));
        command.addConditions(StorageGateway.condition(txKey, null));
        command.addMutations(put(txKey, record.toByteString()));
        int position = 0;
        for (ConditionalMutation change : changes) {
            ByteString expected = change.getExpectedPresent() ? change.getExpectedCell() : null;
            command.addConditions(StorageGateway.condition(change.getKey(), expected));
            command.addConditions(StorageGateway.condition(lockKey(change.getKey()), locks.get(position++)));
            if (change.getReadOnly()) continue;
            VersionedCell old = expected == null ? VersionedCell.getDefaultInstance() : parseCell(expected);
            var version = CellVersion.newBuilder().setTransactionId(plan.getTransactionId()).setCoordinatorPart(plan.getCoordinatorPart())
                    .setValue(change.getValue()).setDeleted(change.getDelete()).setCommittedAtMs(decidedAt);
            command.addMutations(put(change.getKey(), old.toBuilder().addVersions(version).build().toByteString()));
        }
        BatchResult result = storage.write(graph, part, command.build());
        if (!result.getCode().equals("OK")) throw new ClusterException(result.getCode(), result.getMessage());
        WriteTimings.add("fastCommitMs", System.nanoTime() - started);
        return record;
    }

    public TransactionRecord resume(TransactionRecord initial) {
        TransactionRecord record = initial;
        TransactionPlan plan = record.getPlan();
        if (record.getState() == TransactionState.PREPARING) {
            String code = "", reason = "";
            try {
                if (plan.getCompensation()) {
                    for (ParticipantPlan participant : plan.getParticipantsList()) prepare(plan, participant);
                } else phase(plan, participant -> prepare(plan, participant));
            } catch (ClusterException ex) {
                if (!deterministic(ex)) throw ex;
                code = ex.code(); reason = ex.getMessage();
            }
            TransactionState decision = code.isEmpty() && System.currentTimeMillis() < plan.getDeadlineMs()
                    ? TransactionState.COMMITTED : TransactionState.ABORTED;
            if (decision == TransactionState.ABORTED && code.isEmpty()) { code = "TRANSACTION_TIMEOUT"; reason = "事务准备超过原始截止时间"; }
            record = decide(record, decision, code, reason);
        }
        if (!record.getFinalized()) {
            TransactionRecord decided = record;
            phase(plan, participant -> finalizeParticipant(decided, participant));
            ByteString key = transactionKey(plan.getTransactionId());
            ByteString planKey = transactionPlanKey(plan.getTransactionId());
            ByteString gcKey = transactionGcKey(plan.getTransactionId());
            var complete = record.toBuilder().setFinalized(true).build();
            // 协调键只留决定。完整参与者计划挪走，否则下一次批量写入的读视图会把全部历史计划再解析一遍。
            var summary = complete.toBuilder().setPlan(complete.getPlan().toBuilder().clearParticipants()).build();
            try {
                storage.write(plan.getGraphId(), plan.getCoordinatorPart(), StorageGateway.batch("tx.finalized/" + plan.getTransactionId())
                        .addConditions(StorageGateway.condition(key, record.toByteString()))
                        .addConditions(StorageGateway.condition(planKey, null))
                        .addConditions(StorageGateway.condition(gcKey, null))
                        .addMutations(put(key, summary.toByteString()))
                        .addMutations(put(planKey, complete.toByteString()))
                        .addMutations(put(gcKey, summary.toByteString())).build());
            } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
            record = record(plan.getGraphId(), plan.getCoordinatorPart(), plan.getTransactionId());
        }
        return record;
    }

    private void prepare(TransactionPlan plan, ParticipantPlan participant) {
        if (System.currentTimeMillis() >= plan.getDeadlineMs()) throw new ClusterException("TRANSACTION_TIMEOUT", "事务准备超过原始截止时间");
        List<ConditionalMutation> chunk = new ArrayList<>();
        int bytes = 0, chunkId = 0;
        for (ConditionalMutation change : participant.getChangesList()) {
            int size = change.getSerializedSize();
            if (size > 512 * 1024) throw new ClusterException("INVALID_ARGUMENT", "单条记录超过准备日志大小限制");
            if (!chunk.isEmpty() && bytes + size > 512 * 1024) {
                prepareChunk(plan, participant.getPartId(), chunkId++, chunk);
                chunk = new ArrayList<>(); bytes = 0;
            }
            chunk.add(change); bytes += size;
        }
        if (!chunk.isEmpty()) prepareChunk(plan, participant.getPartId(), chunkId, chunk);
    }

    private void prepareChunk(TransactionPlan plan, int part, int chunkId, List<ConditionalMutation> changes) {
        long graph = plan.getGraphId();
        ByteString chunkKey = text("__chunk/" + plan.getTransactionId() + "/" + chunkId);
        if (storage.get(graph, part, chunkKey) != null) return;
        ReservationOwner owner = ReservationOwner.newBuilder().setTransactionId(plan.getTransactionId()).setCoordinatorPart(plan.getCoordinatorPart()).build();
        var command = StorageGateway.batch("tx.prepare/" + plan.getTransactionId() + "/" + part + "/" + chunkId);
        command.addConditions(StorageGateway.condition(text("__schema/writeGate"), plan.getSchemaMigration() ? text("MIGRATING") : null));
        if (!plan.getSchemaMigration()) command.addConditions(StorageGateway.condition(text("__schema/generation"),
                plan.getGeneration() == 0 ? null : text(Long.toString(plan.getGeneration()))));
        if (!plan.getSchemaMigration()) command.addConditions(StorageGateway.condition(text("__schema/writeEpoch"),
                plan.getWriteEpoch() == 0 ? null : text(Long.toString(plan.getWriteEpoch()))));
        command.addConditions(StorageGateway.condition(participantKey(plan.getTransactionId()), null));
        command.addConditions(StorageGateway.condition(chunkKey, null));
        command.addMutations(put(chunkKey, text("PREPARED")));
        List<ByteString> locks = storage.getMany(graph, part, changes.stream().map(change -> lockKey(change.getKey())).toList());
        int position = 0;
        for (ConditionalMutation change : changes) {
            ByteString expected = change.getExpectedPresent() ? change.getExpectedCell() : null;
            ByteString lock = lockKey(change.getKey());
            command.addConditions(StorageGateway.condition(change.getKey(), expected));
            ByteString encodedLock = locks.get(position++);
            ReservationState state = reservation(encodedLock);
            if (state.hasWriter() && !state.getWriter().equals(owner)
                    || !change.getReadOnly() && state.getReadersList().stream().anyMatch(reader -> !reader.equals(owner))) {
                throw new ClusterException("WRITE_CONFLICT", "写入对象已被其他事务预留");
            }
            var next = state.toBuilder();
            if (change.getReadOnly()) { if (!state.getReadersList().contains(owner)) next.addReaders(owner); }
            else next.setWriter(owner);
            command.addConditions(StorageGateway.condition(lock, encodedLock));
            command.addMutations(put(lock, next.build().toByteString()));
            if (change.getReadOnly()) continue;
            VersionedCell old = expected == null ? VersionedCell.getDefaultInstance() : parseCell(expected);
            var version = CellVersion.newBuilder().setTransactionId(plan.getTransactionId()).setCoordinatorPart(plan.getCoordinatorPart())
                    .setValue(change.getValue()).setDeleted(change.getDelete()).setCompensation(plan.getCompensation());
            command.addMutations(put(change.getKey(), old.toBuilder().addVersions(version).build().toByteString()));
        }
        // 共享读预留的快照可能在并发恢复中变化；物理尝试用内容摘要区分，逻辑块用持久化 marker 去重。
        command.setRequestId(command.getRequestId() + "/" + hex(command.build().toBuilder().clearExpiresAtMs().build().toByteString()));
        BatchResult result;
        try { result = storage.write(graph, part, command.build()); }
        catch (ClusterException ex) {
            if (ex.code().equals("WRITE_CONFLICT") && storage.get(graph, part, chunkKey) != null) return;
            throw ex;
        }
        if (!result.getCode().equals("OK")) throw new ClusterException(result.getCode(), result.getMessage());
    }

    private TransactionRecord decide(TransactionRecord previous, TransactionState decision, String code, String reason) {
        TransactionPlan plan = previous.getPlan(); ByteString key = transactionKey(plan.getTransactionId());
        // 不依赖某个 Compute 是否仍然存活。并发恢复器通过同一 Raft 条件决定唯一结果。
        TransactionRecord next = previous.toBuilder().setState(decision).setErrorCode(code).setErrorMessage(reason)
                .setDecidedAtMs(System.currentTimeMillis()).build();
        try {
            storage.write(plan.getGraphId(), plan.getCoordinatorPart(), StorageGateway.batch("tx.decide/" + plan.getTransactionId() + "/" + decision)
                    .addConditions(StorageGateway.condition(key, previous.toByteString())).addMutations(put(key, next.toByteString())).build());
        } catch (ClusterException ex) { if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT").contains(ex.code())) throw ex; }
        return record(plan.getGraphId(), plan.getCoordinatorPart(), plan.getTransactionId());
    }

    private void finalizeParticipant(TransactionRecord record, ParticipantPlan participant) {
        List<ConditionalMutation> chunk = new ArrayList<>();
        int bytes = 0;
        for (ConditionalMutation change : participant.getChangesList()) {
            if (!chunk.isEmpty() && bytes + change.getSerializedSize() > 512 * 1024) {
                finalizeChunk(record, participant.getPartId(), chunk);
                chunk.clear(); bytes = 0;
            }
            chunk.add(change); bytes += change.getSerializedSize();
        }
        if (!chunk.isEmpty()) finalizeChunk(record, participant.getPartId(), chunk);
    }

    private void finalizeChunk(TransactionRecord record, int part, List<ConditionalMutation> changes) {
        TransactionPlan plan = record.getPlan();
        ReservationOwner owner = ReservationOwner.newBuilder().setTransactionId(plan.getTransactionId()).setCoordinatorPart(plan.getCoordinatorPart()).build();
        List<ByteString> keys = new ArrayList<>();
        for (var change : changes) { keys.add(lockKey(change.getKey())); keys.add(change.getKey()); }
        ByteString stateKey = participantKey(plan.getTransactionId());
        for (int attempt = 0; attempt < 32; attempt++) {
            List<ByteString> values = storage.getMany(plan.getGraphId(), part, keys);
            var command = StorageGateway.batch("tx.release.v3/" + plan.getTransactionId() + "/" + part);
            // 终态栅栏和释放锁在同一批，迟到的 PREPARE 看不到锁已释放而栅栏还没写上的窗口。
            command.addMutations(put(stateKey, text(record.getState().name())));
            int position = 0;
            for (ConditionalMutation change : changes) {
                ByteString lock = lockKey(change.getKey());
                ByteString currentLock = values.get(position++), current = values.get(position++);
                ReservationState state = reservation(currentLock);
                if (!state.getWriter().equals(owner) && !state.getReadersList().contains(owner)) continue;
                var next = state.toBuilder().clearReaders();
                for (ReservationOwner reader : state.getReadersList()) if (!reader.equals(owner)) next.addReaders(reader);
                if (state.getWriter().equals(owner)) next.clearWriter();
                command.addConditions(StorageGateway.condition(lock, currentLock));
                if (!next.hasWriter() && next.getReadersCount() == 0) command.addMutations(Mutation.newBuilder().setKey(lock).setDelete(true));
                else command.addMutations(put(lock, next.build().toByteString()));
                if (record.getState() == TransactionState.ABORTED && !change.getReadOnly()) {
                    var cell = parseCell(current).toBuilder();
                    cell.clearVersions();
                    for (CellVersion version : parseCell(current).getVersionsList()) if (!version.getTransactionId().equals(plan.getTransactionId())) cell.addVersions(version);
                    command.addConditions(StorageGateway.condition(change.getKey(), current));
                    if (cell.getVersionsCount() == 0) command.addMutations(Mutation.newBuilder().setKey(change.getKey()).setDelete(true));
                    else command.addMutations(put(change.getKey(), cell.build().toByteString()));
                } else if (record.getState() == TransactionState.COMMITTED && !change.getReadOnly()) {
                    var cell = parseCell(current).toBuilder().clearVersions();
                    for (CellVersion version : parseCell(current).getVersionsList()) {
                        cell.addVersions(version.getTransactionId().equals(plan.getTransactionId())
                                ? version.toBuilder().setCommittedAtMs(record.getDecidedAtMs()).build() : version);
                    }
                    command.addConditions(StorageGateway.condition(change.getKey(), current));
                    command.addMutations(put(change.getKey(), cell.build().toByteString()));
                }
            }
            if (command.getMutationsCount() == 0) return;
            // Historical versions may have grown since planning. Bound the complete CAS body.
            if (command.build().getSerializedSize() > 1024 * 1024 && changes.size() > 1) {
                int middle = changes.size() / 2;
                finalizeChunk(record, part, changes.subList(0, middle));
                finalizeChunk(record, part, changes.subList(middle, changes.size()));
                return;
            }
            // Both the reservation and entity CAS snapshots participate in the physical identity.
            // A changed cell with the same reservation is a NEW CAS attempt; an identical body
            // remains idempotent. Never hide a genuine conflict by retrying with a random ID.
            command.setRequestId(command.getRequestId() + "/" + hex(command.build().toBuilder()
                    .clearRequestId().clearExpiresAtMs().clearControllerEpoch().build().toByteString()));
            try { storage.write(plan.getGraphId(), part, command.build()); return; }
            catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
            if (attempt == 31) throw new ClusterException("BUSY", "参与者预留清理冲突，稍后恢复");
        }
    }

    public TransactionRecord record(long graph, int coordinator, String id) {
        ByteString value = storage.get(graph, coordinator, transactionKey(id));
        if (value == null) throw new ClusterException("TRANSACTION_UNKNOWN", "事务协调记录不可用");
        return parseRecord(value);
    }

    /** QueryScope 在整条查询中共享，禁止同一事务在不同分片被解释为不同状态。 */
    public final class QueryScope {
        private final long graph;
        private final Map<String, TransactionState> decisions = new HashMap<>();
        private final boolean frozen;
        public QueryScope(long graph) { this.graph = graph; this.frozen = false; }
        /**
         * 所有数据读取前先收集协调决定，之后不再把新提交事务纳入本次读视图。
         * COMMIT 前所有参与分片已持久化 PREPARE，因此在此后读取不会出现某侧没有准备版本。
         * 未收集到的事务统一视为不可见；不是每个分片各自临时查询决定。
         */
        public QueryScope(long graph, int partitions) {
            this.graph = graph; this.frozen = true;
            for (int part = 0; part < partitions; part++) {
                ByteString after = ByteString.EMPTY;
                String cursor = "";
                do {
                    ScanPage page = storage.scan(graph, part, TX_PREFIX, after, 256, cursor, true);
                    for (var entry : page.getEntriesList()) {
                        TransactionRecord record = parseRecord(entry.getValue());
                        // 墓碑也保留其决定；summary 不携带完整计划，不能用参与者数量判断墓碑。
                        StorageGateway.charge(record.getPlan().getTransactionId().length() * 2L + 96);
                        decisions.put(record.getPlan().getTransactionId(), record.getState());
                    }
                    if (!page.getMore()) break;
                    after = page.getLastKey();
                    cursor = page.getCursorId();
                } while (true);
            }
        }
        public synchronized ByteString value(ByteString encoded) {
            if (encoded == null) return null;
            List<CellVersion> versions = parseCell(encoded).getVersionsList();
            for (int i = versions.size() - 1; i >= 0; i--) {
                CellVersion version = versions.get(i);
                if (version.getBaseline()) return version.getDeleted() ? null : version.getValue();
                TransactionState state = decisions.computeIfAbsent(version.getTransactionId(), id -> frozen ? TransactionState.PREPARING
                        : record(graph, version.getCoordinatorPart(), id).getState());
                // 普通边补偿允许准备中的已写版本可见，原子批次则仍只接受 COMMITTED。
                if (version.getCompensation() && state == TransactionState.PREPARING) {
                    TransactionState actual = record(graph, version.getCoordinatorPart(), version.getTransactionId()).getState();
                    if (actual == TransactionState.ABORTED) continue;
                    return version.getDeleted() ? null : version.getValue();
                }
                if (state == TransactionState.COMMITTED) return version.getDeleted() ? null : version.getValue();
            }
            return null;
        }
    }

    public static TransactionPlan normalize(TransactionPlan input) {
        if (input.getTransactionId().isBlank() || input.getTransactionId().length() > 120 || input.getGraphId() <= 0
                || input.getParticipantsCount() == 0 || input.getDeadlineMs() <= 0) {
            throw new ClusterException("INVALID_ARGUMENT", "事务计划无效");
        }
        if (input.getSerializedSize() > planMaxBytes || input.getParticipantsCount() > 65535) {
            throw new ClusterException("BATCH_TOO_LARGE", "事务计划超过允许大小：" + input.getSerializedSize() + " > " + planMaxBytes);
        }
        var participants = new ArrayList<>(input.getParticipantsList());
        participants.sort(Comparator.comparingInt(ParticipantPlan::getPartId));
        Set<Integer> parts = new HashSet<>();
        for (ParticipantPlan participant : participants) {
            if (!parts.add(participant.getPartId()) || participant.getChangesCount() == 0) throw new ClusterException("INVALID_ARGUMENT", "参与分片重复或为空");
            Set<ByteString> keys = new HashSet<>();
            for (ConditionalMutation change : participant.getChangesList()) if (!keys.add(change.getKey()) || change.getKey().isEmpty()) {
                throw new ClusterException("INVALID_ARGUMENT", "参与分片变更键重复或为空");
            }
        }
        // Persisted plans retain their coordinator across replay and upgrades.
        int coordinator = parts.contains(input.getCoordinatorPart()) ? input.getCoordinatorPart() : participants.getFirst().getPartId();
        if (input.getCompensation()) {
            coordinator = input.getCompensationCoordinator();
            if (!parts.contains(coordinator)) throw new ClusterException("INVALID_ARGUMENT", "普通边协调者不在参与分片中");
            int selected = coordinator;
            participants.sort(Comparator.<ParticipantPlan>comparingInt(part -> part.getPartId() == selected ? 0 : 1).thenComparingInt(ParticipantPlan::getPartId));
        }
        var normalized = input.toBuilder().clearParticipants().addAllParticipants(participants)
                .setCoordinatorPart(coordinator).clearContentDigest().build();
        try {
            byte[] bytes = new byte[normalized.getSerializedSize()];
            var output = com.google.protobuf.CodedOutputStream.newInstance(bytes);
            output.useDeterministicSerialization(); normalized.writeTo(output); output.checkNoSpaceLeft();
            return normalized.toBuilder().setContentDigest(ByteString.copyFrom(sha256(bytes))).build();
        } catch (java.io.IOException ex) { throw new IllegalStateException(ex); }
    }
    private static VersionedCell parseCell(ByteString bytes) {
        try { return VersionedCell.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "事务版本记录损坏", ex); }
    }
    private static TransactionRecord parseRecord(ByteString bytes) {
        try { return TransactionRecord.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "事务记录损坏", ex); }
    }
    private static ReservationState reservation(ByteString bytes) {
        if (bytes == null) return ReservationState.getDefaultInstance();
        try { return ReservationState.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "实体预留记录损坏", ex); }
    }
    public static Mutation put(ByteString key, ByteString value) { return Mutation.newBuilder().setKey(key).setValue(value).build(); }
    private static String hex(ByteString bytes) { return HexFormat.of().formatHex(sha256(bytes.toByteArray())); }
    private static byte[] sha256(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (Exception ex) { throw new IllegalStateException(ex); }
    }
}
