package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RocksStateMachine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Rebuild-only replicas. Metadata serializes small decisions; payloads never enter a recovery log. */
final class RebuildData implements AutoCloseable {
    private static final Duration RPC = Duration.ofSeconds(15);
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final String SEAL_FILE = "seal.json";
    private final StorageConfig config;
    private final StorageGateway storage;
    private final String boot = UUID.randomUUID().toString();
    private final Path root;
    private record Part(long graph, int part) { }
    /** 维护期间封存的单个分片：Metadata 位置 + 本地全量内容摘要。 */
    public record SealedPart(long graph, int part, long sequence, byte[] digest, long count, byte[] checksum) { }
    public record Seal(String maintenanceId, String bootId, List<SealedPart> parts) { }
    /** 上一个进程留下、尚未比对的封存；比对完成后清空。 */
    private volatile Seal carried;
    private final Set<Part> resolved = ConcurrentHashMap.newKeySet();
    /** 本进程从封存目录重新打开的分片；只有它们可以在比对失败时被删除。 */
    private final Set<Part> reopened = ConcurrentHashMap.newKeySet();
    private volatile boolean resumed;
    private volatile int untrusted;
    /** 本进程已为该维护 id 写过封存文件。 */
    private volatile String sealedFor = "";
    private volatile Set<Part> sealedParts = Set.of();
    private long lastIdempotencyCleanup;
    private final Map<Part, Local> locals = new ConcurrentHashMap<>();
    private static final class Local implements AutoCloseable {
        final RocksStateMachine db;
        long sequence;
        ByteString digest = ByteString.EMPTY;
        WriteBatch pending;
        String transfer = "";
        ByteString last = ByteString.EMPTY;
        MessageDigest checksum = sha();
        long count;
        boolean imported;
        BatchResult lastResult;
        Local(RocksStateMachine db) { this.db = db; }
        @Override public void close() throws IOException { db.close(); }
    }
    RebuildData(StorageConfig config, StorageGateway storage) throws IOException {
        this.config = config; this.storage = storage;
        root = config.directory().resolve("rebuild-data");
        // Ownership checked by RaftNode before this point. Directories are reopened only when a
        // maintenance seal lists them, and only after their full content matches the seal.
        Seal seal = readSeal(root.resolve(SEAL_FILE));
        if (seal == null) delete(root);
        else {
            Set<String> keep = new HashSet<>();
            for (SealedPart part : seal.parts()) keep.add(part.graph() + "-" + part.part());
            try (var entries = Files.list(root)) {
                for (Path entry : entries.toList()) {
                    String name = entry.getFileName().toString();
                    if (!name.equals(SEAL_FILE) && !keep.contains(name)) delete(entry);
                }
            }
            carried = seal;
        }
        Files.createDirectories(root);
        resumed = carried == null;
    }
    private static Seal readSeal(Path file) {
        if (!Files.isRegularFile(file)) return null;
        try { return JSON.readValue(file.toFile(), Seal.class); }
        catch (IOException ex) { return null; }
    }
    static ByteString key(long graph, int part) { return TransactionEngine.text("rebuild/head/" + graph + "/" + part); }
    private RebuildHead head(long graph, int part) {
        ByteString bytes = storage.get(0, 0, key(graph, part));
        if (bytes == null) throw new ClusterException("DATA_LOST", "分片没有可信数据状态，必须重新导入：" + graph + "/" + part);
        try { return RebuildHead.parseFrom(bytes); }
        catch (IOException ex) { throw new ClusterException("CORRUPT_DATA", "分片提交状态损坏", ex); }
    }
    private boolean restarted(RebuildHead current) {
        if (current.getReceiptsCount() == 0) return false;
        for (var receipt : current.getReceiptsList()) {
            Node holder = all(current.getSpec()).stream().filter(n -> n.getNodeId().equals(receipt.getNodeId())).findFirst().orElse(null);
            if (holder == null) return false;
            try {
                if (call(holder, "rebuild.identity", message(current).build()).getBody().toStringUtf8().equals(receipt.getBootId())) return false;
            } catch (Exception ex) { return false; }
        }
        return true;
    }
    private void cas(RebuildHead old, RebuildHead next) { cas(old, next, false); }
    /** @param outsideMaintenance 额外要求集群不在维护模式；数据写入以此和维护模式的开启互斥。 */
    private void cas(RebuildHead old, RebuildHead next, boolean outsideMaintenance) {
        var spec = next.getSpec();
        var batch = StorageGateway.batch("rebuild.head/" + UUID.randomUUID())
                .addConditions(StorageGateway.condition(key(spec.getGraphId(), spec.getPartId()), old == null ? null : old.toByteString()));
        if (outsideMaintenance) batch.addConditions(StorageGateway.condition(ClusterMaintenance.KEY, null));
        storage.write(0, 0, batch.addMutations(TransactionEngine.put(key(spec.getGraphId(), spec.getPartId()), next.toByteString())).build());
    }
    private static List<Node> all(DataGroupSpec spec) {
        List<Node> nodes = new ArrayList<>(spec.getMembersList()); nodes.addAll(spec.getLearnersList()); return nodes;
    }
    private Local fresh(long graph, int part) throws IOException {
        Part id = new Part(graph, part);
        Local previous = locals.remove(id);
        if (previous != null) synchronized (previous) { previous.close(); }
        Path directory = root.resolve(graph + "-" + part);
        delete(directory);
        Local created = new Local(RocksStateMachine.freshReplica(directory)); locals.put(id, created); return created;
    }
    void add(DataGroupSpec spec) throws IOException {
        if (!spec.getDataMode().equals("rebuild")) throw new ClusterException("CONFIG_MISMATCH", "图分片使用旧 Raft 模式，请新建图后导入");
        if (all(spec).stream().noneMatch(n -> n.getNodeId().equals(config.nodeId()))) throw new ClusterException("INVALID_ARGUMENT", "节点不在分片成员中");
        if (!spec.getControlTaskId().isBlank()) return; // Created only by the fenced full-copy protocol.
        RebuildHead initial = RebuildHead.newBuilder().setSpec(spec).setState("READY").build();
        if (storage.get(0, 0, key(spec.getGraphId(), spec.getPartId())) == null) {
            try { cas(null, initial); } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        RebuildHead current = head(spec.getGraphId(), spec.getPartId());
        // Empty creation only. A node restart must never register an empty replica for existing data.
        if (current.getSequence() != 0 || !current.getState().equals("READY")) return;
        synchronized (locals) {
            if (current.getReceiptsList().stream().anyMatch(r -> r.getNodeId().equals(config.nodeId()) && !r.getBootId().equals(boot))) return;
            Local local = locals.get(new Part(spec.getGraphId(), spec.getPartId()));
            if (local == null) local = fresh(spec.getGraphId(), spec.getPartId());
            for (int attempt = 0; attempt < 16; attempt++) {
                current = head(spec.getGraphId(), spec.getPartId());
                if (!current.getState().equals("READY") || current.getSequence() != 0) return;
                if (current.getReceiptsList().stream().anyMatch(r -> r.getNodeId().equals(config.nodeId()))) return;
                try { cas(current, current.toBuilder().addReceipts(receipt(local)).build()); return; }
                catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
            }
        }
    }
    private ReplicaReceipt receipt(Local local) {
        return ReplicaReceipt.newBuilder().setNodeId(config.nodeId()).setBootId(boot).setSequence(local.sequence).setDigest(local.digest).build();
    }
    private Local local(long graph, int part) {
        Local local = locals.get(new Part(graph, part));
        if (local == null) throw new ClusterException("REPLICA_UNTRUSTED", "本次启动没有可信分片，等待全量重建");
        return local;
    }
    private boolean certified(RebuildHead head) {
        return head.getReceiptsList().stream().anyMatch(r -> r.getNodeId().equals(config.nodeId()) && r.getBootId().equals(boot));
    }
    private void materialize(Local local, RebuildHead head) throws IOException {
        if (!certified(head)) throw new ClusterException("REPLICA_UNTRUSTED", "当前进程不是已确认的数据持有者");
        if (local.sequence == head.getSequence() && local.digest.equals(head.getDigest())) return;
        if (local.sequence + 1 == head.getSequence() && local.pending != null && digest(local.pending.toByteString()).equals(head.getDigest())) {
            local.lastResult = local.db.applyReplica(local.pending, head.getSequence());
            local.sequence = head.getSequence(); local.digest = head.getDigest(); local.pending = null; return;
        }
        throw new ClusterException("REPLICA_UNTRUSTED", "数据版本落后，需要全量同步");
    }
    private RpcResponse call(Node node, String method, RebuildMessage message) {
        RpcResponse response = node.getNodeId().equals(config.nodeId()) ? internal(method, message)
                : storage.direct(DistributedCatalog.address(node), method, message.toByteString(), RPC);
        if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
        return response;
    }
    private static RebuildMessage.Builder message(RebuildHead head) {
        return RebuildMessage.newBuilder().setGraphId(head.getSpec().getGraphId()).setPartId(head.getSpec().getPartId()).setHead(head);
    }
    private List<ReplicaReceipt> holders(RebuildHead current, boolean pending) {
        List<ReplicaReceipt> result = new ArrayList<>();
        for (Node node : all(current.getSpec())) {
            try {
                var response = call(node, pending ? "rebuild.pending" : "rebuild.probe", message(current).build());
                result.add(ReplicaReceipt.parseFrom(response.getBody()));
            } catch (Exception ignored) { }
        }
        return result;
    }
    private int votes(RebuildHead current, List<ReplicaReceipt> receipts) {
        Set<String> ids = new HashSet<>(); for (var r : receipts) ids.add(r.getNodeId());
        return (int) current.getSpec().getMembersList().stream().filter(n -> ids.contains(n.getNodeId())).count();
    }
    private RebuildHead settle(RebuildHead current) {
        if (!current.getState().equals("WRITING")) return current;
        if (System.currentTimeMillis() < current.getOperationDeadlineMs()) return current;
        List<ReplicaReceipt> staged = holders(current, true);
        RebuildHead next;
        if (votes(current, staged) >= current.getSpec().getMembersCount() / 2 + 1) {
            next = current.toBuilder().setState("READY").setSequence(current.getSequence() + 1)
                    .clearReceipts().addAllReceipts(staged).clearOperationId().clearStaged().build();
        } else {
            // Cannot guess a new decision without a reachable majority of the old configuration.
            var old = current.toBuilder().setDigest(current.getReceiptsCount() == 0 ? ByteString.EMPTY : current.getReceipts(0).getDigest()).build();
            List<ReplicaReceipt> available = holders(old, false);
            if (votes(current, available) < current.getSpec().getMembersCount() / 2 + 1)
                throw new ClusterException("GROUP_UNAVAILABLE", "分片写入未确认，等待多数成员");
            next = current.toBuilder().setState("READY").setDigest(old.getDigest()).clearOperationId().build();
        }
        try { cas(current, next); return next; }
        catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; return head(current.getSpec().getGraphId(), current.getSpec().getPartId()); }
    }
    ByteString read(long graph, int part, ByteString body) throws IOException {
        RebuildHead current = head(graph, part);
        if (current.getState().equals("WRITING")) current = settle(current);
        if (current.getState().equals("WRITING")) current = current.toBuilder()
                .setDigest(current.getReceiptsCount() == 0 ? ByteString.EMPTY : current.getReceipts(0).getDigest()).build();
        if (current.getState().equals("LOST")) throw new ClusterException("DATA_LOST", "所有可信副本已丢失，需要重新导入");
        Local local = local(graph, part);
        synchronized (local) {
            if (locals.get(new Part(graph, part)) != local) throw new ClusterException("REPLICA_UNTRUSTED", "分片已被替换");
            materialize(local, current);
            return ByteString.copyFrom(local.db.query(RocksStateMachine.message(body.toByteArray())).join().getContent().toByteArray());
        }
    }
    BatchResult write(long graph, int part, WriteBatch command) throws IOException {
        ByteString hash = digest(command.toByteString());
        RebuildHead claimed = null;
        for (int attempt = 0; attempt < 1000; attempt++) {
            RebuildHead current = head(graph, part);
            if (current.getState().equals("WRITING")) {
                settle(current);
                try { Thread.sleep(10); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "写入等待中断"); }
                continue;
            }
            if (current.getState().equals("LOST")) throw new ClusterException("DATA_LOST", "所有可信副本已丢失，需要重新导入");
            if (!current.getState().equals("READY")) throw new ClusterException("PARTITION_FROZEN", "分片全量同步中，写入暂停");
            if (current.getSpec().getMembersList().stream().noneMatch(n -> n.getNodeId().equals(config.nodeId())))
                throw new ClusterException("REPLICA_UNTRUSTED", "节点已不属于分片");
            var next = current.toBuilder().setState("WRITING").setOperationId(UUID.randomUUID().toString())
                    .setOperationDeadlineMs(System.currentTimeMillis() + 30_000).setDigest(hash).build();
            try { cas(current, next, true); claimed = next; break; }
            catch (ClusterException ex) {
                if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
                if (storage.get(0, 0, ClusterMaintenance.KEY) != null) throw ClusterMaintenance.rejected();
            }
        }
        if (claimed == null) throw new ClusterException("BUSY", "分片并发写入繁忙");
        List<ReplicaReceipt> staged = new ArrayList<>();
        for (Node node : all(claimed.getSpec())) {
            try { staged.add(ReplicaReceipt.parseFrom(call(node, "rebuild.stage", message(claimed).setCommand(command).build()).getBody())); }
            catch (Exception ignored) { }
        }
        if (votes(claimed, staged) < claimed.getSpec().getMembersCount() / 2 + 1) {
            throw new ClusterException("FAILED", "数据尚未获得多数副本确认，提交状态未知");
        }
        RebuildHead committed = claimed.toBuilder().setState("READY").setSequence(claimed.getSequence() + 1)
                .clearReceipts().addAllReceipts(staged).clearOperationId().build();
        try { cas(claimed, committed); }
        catch (ClusterException ex) {
            if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
            RebuildHead actual = head(graph, part);
            if (actual.getSequence() != committed.getSequence() || !actual.getDigest().equals(hash))
                throw new ClusterException("FAILED", "并发写入状态已改变，请使用原请求重试");
        }
        // At least one certified holder must materialize and return the deterministic CAS result.
        for (Node node : all(committed.getSpec())) {
            try { return BatchResult.parseFrom(call(node, "rebuild.result", message(committed).setCommand(command).build()).getBody()); }
            catch (Exception ignored) { }
        }
        throw new ClusterException("FAILED", "提交已决定但响应丢失，请使用原请求重试");
    }
    RpcResponse internal(String method, RebuildMessage request) {
        try {
            if (method.equals("rebuild.identity")) return ok(TransactionEngine.text(boot));
            RebuildHead current = head(request.getGraphId(), request.getPartId());
            if (method.equals("rebuild.beginImport")) {
                synchronized (locals) {
                    current = head(request.getGraphId(), request.getPartId());
                    if (!current.getState().equals("FROZEN") || !current.getOperationId().equals(request.getTransferId())
                            || all(current.getTarget()).stream().noneMatch(n -> n.getNodeId().equals(config.nodeId())))
                        throw new ClusterException("STALE_CONTROLLER", "导入任务已失效");
                    if (current.getReceiptsList().stream().anyMatch(r -> r.getNodeId().equals(config.nodeId()) && r.getBootId().equals(boot)))
                        throw new ClusterException("INVALID_ARGUMENT", "禁止覆盖当前可信源副本");
                    Local local = locals.get(new Part(request.getGraphId(), request.getPartId()));
                    local = fresh(request.getGraphId(), request.getPartId()); local.transfer = request.getTransferId();
                    return ok(TransactionEngine.text(boot));
                }
            }
            Local local = local(request.getGraphId(), request.getPartId());
            synchronized (local) {
                if (locals.get(new Part(request.getGraphId(), request.getPartId())) != local) throw new ClusterException("REPLICA_UNTRUSTED", "本地分片已替换");
                current = head(request.getGraphId(), request.getPartId());
                if (method.equals("rebuild.stage") || method.equals("rebuild.pending")) {
                    if (!current.equals(request.getHead()) || !current.getState().equals("WRITING")) throw new ClusterException("STALE_CONTROLLER", "准备请求已过期");
                    if (!certified(current)) throw new ClusterException("REPLICA_UNTRUSTED", "当前进程不属于可信持有者");
                    if (method.equals("rebuild.stage")) {
                        ByteString previousDigest = current.getReceiptsCount() == 0 ? ByteString.EMPTY : current.getReceipts(0).getDigest();
                        materialize(local, current.toBuilder().setDigest(previousDigest).build());
                        if (!digest(request.getCommand().toByteString()).equals(current.getDigest())) throw new ClusterException("REQUEST_CONFLICT", "写入摘要不匹配");
                        local.pending = request.getCommand();
                    }
                    if (local.sequence != current.getSequence() || local.pending == null || !digest(local.pending.toByteString()).equals(current.getDigest())) throw new ClusterException("NOT_PREPARED", "本节点没有该准备操作");
                    return ok(receipt(local).toBuilder().setSequence(current.getSequence() + 1).setDigest(current.getDigest()).build().toByteString());
                }
                if (method.equals("rebuild.import") || method.equals("rebuild.finishImport")) {
                    if (!current.getState().equals("FROZEN") || !current.getOperationId().equals(request.getTransferId())
                            || !local.transfer.equals(request.getTransferId()) || !request.getBootId().equals(boot)) throw new ClusterException("STALE_CONTROLLER", "导入任务已过期");
                    if (method.equals("rebuild.import")) {
                        if (!local.last.equals(request.getAfterKey()) || local.imported) throw new ClusterException("INVALID_ARGUMENT", "导入页顺序错误");
                        for (var entry : request.getPage().getEntriesList()) {
                            if (!local.last.isEmpty() && ByteString.unsignedLexicographicalComparator().compare(entry.getKey(), local.last) <= 0) throw new ClusterException("INVALID_ARGUMENT", "导入键顺序错误");
                            hashEntry(local.checksum, entry); local.count++; local.last = entry.getKey();
                        }
                        local.db.importReplica(request.getPage()); return ok(ByteString.EMPTY);
                    }
                    if (local.count != request.getCount() || !ByteString.copyFrom(local.checksum.digest()).equals(request.getDigest())) throw new ClusterException("CORRUPT_DATA", "全量同步校验失败");
                    local.sequence = current.getSequence(); local.digest = current.getDigest(); local.imported = true;
                    return ok(receipt(local).toByteString());
                }
                if (current.getState().equals("WRITING")) current = current.toBuilder()
                        .setDigest(current.getReceiptsCount() == 0 ? ByteString.EMPTY : current.getReceipts(0).getDigest()).build();
                materialize(local, current);
                if (method.equals("rebuild.probe")) return ok(receipt(local).toByteString());
                if (method.equals("rebuild.export")) {
                    if (!current.getState().equals("FROZEN") || !current.getOperationId().equals(request.getTransferId())) throw new ClusterException("STALE_CONTROLLER", "分片未冻结");
                    return ok(local.db.exportReplica(request.getAfterKey()).toByteString());
                }
                if (method.equals("rebuild.result")) {
                    if (local.lastResult != null && local.digest.equals(digest(request.getCommand().toByteString()))) return ok(local.lastResult.toByteString());
                    // Idempotency is in the same database as entity mutations.
                    ReadKey read = ReadKey.newBuilder().setScan(true).setScanIdempotency(true)
                            .setKey(TransactionEngine.text(request.getCommand().getRequestId())).setLimit(1).setMaxBytes(1024 * 1024).build();
                    ScanPage page = ScanPage.parseFrom(local.db.query(RocksStateMachine.message(read.toByteArray())).join().getContent().toByteArray());
                    if (page.getEntriesCount() != 1 || !page.getEntries(0).getKey().toStringUtf8().equals(request.getCommand().getRequestId())) throw new ClusterException("FAILED", "幂等结果尚不可用");
                    IdempotencyRecord saved = IdempotencyRecord.parseFrom(page.getEntries(0).getValue());
                    ByteString identity = digest(request.getCommand().toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteString());
                    BatchResult result = saved.getDigest().equals(identity) ? saved.getResult()
                            : BatchResult.newBuilder().setCode("REQUEST_CONFLICT").setMessage("相同 requestId 对应不同内容").build();
                    return ok(result.toByteString());
                }
                throw new ClusterException("UNSUPPORTED", "未知重建操作");
            }
        } catch (ClusterException ex) { return RpcResponse.newBuilder().setCode(ex.code()).setMessage(ex.getMessage()).build(); }
        catch (Exception ex) { return RpcResponse.newBuilder().setCode("FAILED").setMessage("分片重建失败：" + ex.getClass().getSimpleName()).build(); }
    }
    /** Freeze through the same Metadata CAS as writes, stream ordered KV, verify, then atomically publish membership. */
    synchronized void configure(DataGroupSpec target) throws IOException {
        if (!target.getDataMode().equals("rebuild")) throw new ClusterException("CONFIG_MISMATCH", "迁移目标不是 rebuild 分片");
        RebuildHead current = settle(head(target.getGraphId(), target.getPartId()));
        if (current.getState().equals("LOST")) throw new ClusterException("DATA_LOST", "无可信副本，请重新导入");
        if (current.getState().equals("READY")) {
            // Do not freeze a healthy majority indefinitely just because a replacement is still offline.
            for (Node targetNode : all(target)) call(targetNode, "rebuild.identity", message(current).build());
            var frozen = current.toBuilder().setState("FROZEN").setOperationId(UUID.randomUUID().toString()).setTarget(target).build();
            cas(current, frozen); current = frozen;
        }
        if (!current.getState().equals("FROZEN") || !current.getTarget().equals(target)) throw new ClusterException("TOPOLOGY_CONFLICT", "已有另一全量同步任务");
        RebuildHead fenced = current.toBuilder().setOperationId(UUID.randomUUID().toString()).build();
        cas(current, fenced); current = fenced;
        List<ReplicaReceipt> sources = holders(current, false);
        if (sources.isEmpty()) throw new ClusterException("DATA_LOST", "没有可用的可信数据源");
        RebuildHead frozen = current;
        Node source = all(current.getSpec()).stream().filter(n -> n.getNodeId().equals(sources.getFirst().getNodeId())).findFirst().orElseThrow();
        List<ReplicaReceipt> next = new ArrayList<>();
        for (Node node : all(target)) {
            ReplicaReceipt present = sources.stream().filter(r -> r.getNodeId().equals(node.getNodeId())).findFirst().orElse(null);
            if (present != null) { next.add(present); continue; }
            String targetBoot = call(node, "rebuild.beginImport", message(frozen).setTransferId(frozen.getOperationId()).build()).getBody().toStringUtf8();
            ByteString after = ByteString.EMPTY; MessageDigest checksum = sha(); long count = 0;
            do {
                ScanPage page = ScanPage.parseFrom(call(source, "rebuild.export", message(frozen).setTransferId(frozen.getOperationId()).setAfterKey(after).build()).getBody());
                for (var entry : page.getEntriesList()) { hashEntry(checksum, entry); count++; }
                call(node, "rebuild.import", message(frozen).setTransferId(frozen.getOperationId()).setBootId(targetBoot).setAfterKey(after).setPage(page).build());
                if (!page.getMore()) break; after = page.getLastKey();
            } while (true);
            next.add(ReplicaReceipt.parseFrom(call(node, "rebuild.finishImport", message(frozen).setTransferId(frozen.getOperationId()).setBootId(targetBoot)
                    .setCount(count).setDigest(ByteString.copyFrom(checksum.digest())).build()).getBody()));
        }
        for (var receipt : next) {
            Node targetNode = all(target).stream().filter(n -> n.getNodeId().equals(receipt.getNodeId())).findFirst().orElseThrow();
            if (!call(targetNode, "rebuild.identity", message(frozen).build()).getBody().toStringUtf8().equals(receipt.getBootId()))
                throw new ClusterException("REPLICA_UNTRUSTED", "目标在同步完成后重启，保持冻结并重新复制");
        }
        cas(frozen, frozen.toBuilder().setState("READY").setSpec(target.toBuilder().clearControlTaskId()).clearTarget().clearOperationId().clearReceipts().addAllReceipts(next).build());
    }
    void reconcile() {
        var catalog = new DistributedCatalog(storage);
        Map<String, ClusterMaintenance.NodeMarker> markers = maintenanceMarkers(catalog);
        for (GraphDefinition graph : catalog.graphs()) {
            if (!graph.getState().equals("ACTIVE")) continue;
            for (int part = 0; part < graph.getPartitions(); part++) {
                ByteString activeKey = TransactionEngine.text("tasks/activeReplica/" + graph.getId() + "/" + part);
                ByteString owner = TransactionEngine.text("rebuild/" + graph.getId() + "/" + part);
                boolean owned = false;
                try {
                    ByteString active = storage.get(0, 0, activeKey);
                    if (active != null && !active.equals(owner)) continue;
                    if (active == null) storage.write(0, 0, StorageGateway.batch("rebuild.claim/" + UUID.randomUUID())
                            .addConditions(StorageGateway.condition(activeKey, null)).addMutations(TransactionEngine.put(activeKey, owner)).build());
                    owned = true;
                    RebuildHead current = head(graph.getId(), part);
                    if (current.getState().equals("LOST")) continue;
                    // A holder restarted inside maintenance is still comparing its sealed files.
                    if (markers != null && pending(current, markers)) continue;
                    if (restarted(current)) { cas(current, current.toBuilder().setState("LOST").build()); continue; }
                    current = settle(current);
                    if (current.getState().equals("WRITING")) continue;
                    if (current.getState().equals("FROZEN")) { configure(current.getTarget()); continue; }
                    var available = holders(current, false);
                    if (available.size() == all(current.getSpec()).size()) continue;
                    // A network outage is not proof of loss. Mark LOST only when every recorded holder has restarted.
                    if (available.isEmpty()) {
                        continue;
                    }
                    configure(current.getSpec());
                    return;
                } catch (Exception ignored) { /* frozen task and commit decision survive in Metadata */ }
                finally {
                    if (owned) try {
                        if (!head(graph.getId(), part).getState().equals("FROZEN")) storage.write(0, 0,
                                StorageGateway.batch("rebuild.release/" + UUID.randomUUID()).addConditions(StorageGateway.condition(activeKey, owner))
                                        .addMutations(Mutation.newBuilder().setKey(activeKey).setDelete(true)).build());
                    } catch (Exception ignored) { }
                }
            }
        }
        cleanupRemoved();
    }
    // ---- Cluster maintenance: seal before a planned restart, compare after it ----
    private Map<String, ClusterMaintenance.NodeMarker> maintenanceMarkers(DistributedCatalog catalog) {
        ClusterMaintenance.State state = ClusterMaintenance.read(storage);
        if (state == null) return null;
        Map<String, ClusterMaintenance.NodeMarker> markers = new HashMap<>();
        for (Node node : catalog.nodes()) {
            var marker = ClusterMaintenance.marker(storage.get(0, 0, ClusterMaintenance.nodeKey(node.getNodeId())));
            if (marker != null && marker.id().equals(state.id())) markers.put(node.getNodeId(), marker);
        }
        return markers;
    }
    /** True when a recorded holder restarted (or is down) after sealing and has not finished comparing. */
    private boolean pending(RebuildHead current, Map<String, ClusterMaintenance.NodeMarker> markers) {
        for (var receipt : current.getReceiptsList()) {
            var marker = markers.get(receipt.getNodeId());
            if (marker == null) continue;
            String live = identity(current.getSpec(), receipt.getNodeId());
            if (live.equals(receipt.getBootId())) continue;
            boolean downAfterSeal = marker.bootId().equals(receipt.getBootId()) && marker.sealedBoot().equals(receipt.getBootId());
            boolean comparing = !marker.bootId().equals(receipt.getBootId()) && !marker.resumed();
            if (downAfterSeal || comparing) return true;
        }
        return false;
    }
    private String identity(DataGroupSpec spec, String nodeId) {
        Node holder = all(spec).stream().filter(n -> n.getNodeId().equals(nodeId)).findFirst().orElse(null);
        if (holder == null) return "";
        try { return call(holder, "rebuild.identity", RebuildMessage.newBuilder().setGraphId(spec.getGraphId()).setPartId(spec.getPartId()).build()).getBody().toStringUtf8(); }
        catch (Exception ex) { return ""; }
    }
    /** Runs on every Storage about once per second. */
    void maintain() {
        try {
            ByteString raw = storage.get(0, 0, ClusterMaintenance.KEY);
            ClusterMaintenance.State state = ClusterMaintenance.state(raw);
            if (!resumed) { resume(state, raw); return; }
            if (state == null) {
                if (!sealedFor.isEmpty()) { Files.deleteIfExists(root.resolve(SEAL_FILE)); sealedFor = ""; }
                return;
            }
            if (!sealedFor.equals(state.id()) || sealOutdated()) { if (seal(state, raw)) sealedFor = state.id(); }
        } catch (Exception ex) {
            org.slf4j.LoggerFactory.getLogger(RebuildData.class).debug("维护状态处理等待重试：{}", ex.toString());
        }
    }
    private void resume(ClusterMaintenance.State state, ByteString raw) throws IOException {
        Seal seal = carried;
        if (seal != null) {
            boolean valid = state != null && state.id().equals(seal.maintenanceId());
            if (valid) marker(state, raw, "", false, 0);
            for (SealedPart sealed : seal.parts()) {
                Part id = new Part(sealed.graph(), sealed.part());
                if (resolved.contains(id)) continue;
                if (!valid || !recertify(seal, sealed)) {
                    // Never touch a replica that something else installed after this restart.
                    if (reopened.contains(id) || !locals.containsKey(id)) discard(id);
                    untrusted++;
                }
                resolved.add(id);
            }
            Files.deleteIfExists(root.resolve(SEAL_FILE));
            carried = null;
            org.slf4j.LoggerFactory.getLogger(RebuildData.class).info("维护重启比对完成：封存分片 {} 个，未通过 {} 个", seal.parts().size(), untrusted);
        }
        resumed = true;
        if (state != null) marker(state, raw, "", true, untrusted);
    }
    /**
     * Trust a sealed directory again only when Metadata still names this node as holder at the
     * sealed position and the full local content matches the seal. Metadata errors propagate so
     * the comparison is retried; a definite mismatch returns false.
     */
    private boolean recertify(Seal seal, SealedPart sealed) throws IOException {
        Part id = new Part(sealed.graph(), sealed.part());
        RebuildHead current;
        try { current = head(sealed.graph(), sealed.part()); }
        catch (ClusterException ex) { if (ex.code().equals("DATA_LOST")) return false; throw ex; }
        if (certified(current)) return true;
        if (current.getState().equals("LOST") || current.getState().equals("WRITING")) return false;
        ByteString digest = ByteString.copyFrom(sealed.digest());
        if (current.getSequence() != sealed.sequence() || !current.getDigest().equals(digest)) return false;
        ReplicaReceipt old = current.getReceiptsList().stream()
                .filter(r -> r.getNodeId().equals(config.nodeId()) && r.getBootId().equals(seal.bootId())).findFirst().orElse(null);
        if (old == null || old.getSequence() != sealed.sequence() || !old.getDigest().equals(digest)) return false;
        Local local = locals.get(id);
        if (local != null && !reopened.contains(id)) return false;
        if (local == null) {
            Local opened;
            try { opened = new Local(RocksStateMachine.reopenSealedReplica(root.resolve(sealed.graph() + "-" + sealed.part()))); }
            catch (IOException ex) { return false; }
            Checksum actual;
            try { actual = checksum(opened); }
            catch (IOException ex) { opened.close(); return false; }
            if (actual.count() != sealed.count() || !Arrays.equals(actual.digest(), sealed.checksum())) { opened.close(); return false; }
            opened.sequence = sealed.sequence(); opened.digest = digest;
            synchronized (locals) {
                if (locals.putIfAbsent(id, opened) != null) { opened.close(); return false; }
                reopened.add(id);
            }
        }
        for (int attempt = 0; attempt < 16; attempt++) {
            current = head(sealed.graph(), sealed.part());
            if (certified(current)) return true;
            if (current.getSequence() != sealed.sequence() || !current.getDigest().equals(digest)) return false;
            var receipts = new ArrayList<ReplicaReceipt>();
            boolean found = false;
            for (var receipt : current.getReceiptsList()) {
                if (receipt.getNodeId().equals(config.nodeId()) && receipt.getBootId().equals(seal.bootId())) {
                    receipts.add(receipt.toBuilder().setBootId(boot).build()); found = true;
                } else receipts.add(receipt);
            }
            if (!found) return false;
            try { cas(current, current.toBuilder().clearReceipts().addAllReceipts(receipts).build()); return true; }
            catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        throw new ClusterException("BUSY", "分片可信状态并发更新繁忙");
    }
    private void discard(Part id) throws IOException {
        Local local;
        synchronized (locals) { local = locals.remove(id); }
        if (local != null) synchronized (local) { local.close(); }
        delete(root.resolve(id.graph() + "-" + id.part()));
    }
    private record Checksum(long count, byte[] digest) { }
    private static Checksum checksum(Local local) throws IOException {
        MessageDigest hash = sha(); long count = 0; ByteString after = ByteString.EMPTY;
        do {
            ScanPage page = local.db.exportReplica(after);
            for (var entry : page.getEntriesList()) { hashEntry(hash, entry); count++; }
            if (!page.getMore()) break;
            after = page.getLastKey();
        } while (true);
        return new Checksum(count, hash.digest());
    }
    /** Local parts this process holds as a certified replica, with their heads. */
    private Map<Part, RebuildHead> certifiedParts() {
        Map<Part, RebuildHead> result = new HashMap<>();
        for (Part part : List.copyOf(locals.keySet())) {
            RebuildHead current;
            try { current = head(part.graph, part.part); }
            catch (ClusterException ex) { if (ex.code().equals("DATA_LOST")) continue; throw ex; }
            if (certified(current)) result.put(part, current);
        }
        return result;
    }
    /**
     * Flush and fingerprint every certified local part. Data writes are already blocked by the
     * maintenance key, so the content cannot change after its partition leaves WRITING.
     * @return false when a partition is still WRITING; the next tick retries
     */
    private boolean seal(ClusterMaintenance.State state, ByteString raw) throws IOException {
        Map<Part, RebuildHead> heads = certifiedParts();
        for (RebuildHead current : heads.values()) if (current.getState().equals("WRITING")) { settle(current); return false; }
        List<SealedPart> sealed = new ArrayList<>();
        for (var entry : heads.entrySet()) {
            Part part = entry.getKey(); RebuildHead current = entry.getValue();
            if (!current.getState().equals("READY") && !current.getState().equals("FROZEN")) continue;
            Local local = locals.get(part);
            if (local == null) continue;
            long sequence; ByteString digest;
            synchronized (local) {
                if (locals.get(part) != local) continue;
                try { materialize(local, current); }
                catch (ClusterException ex) { continue; }
                local.db.flushReplica();
                sequence = local.sequence; digest = local.digest;
            }
            Checksum sum = checksum(local);
            sealed.add(new SealedPart(part.graph, part.part, sequence, digest.toByteArray(), sum.count(), sum.digest()));
        }
        writeSeal(new Seal(state.id(), boot, sealed));
        sealedParts = new HashSet<>();
        for (SealedPart part : sealed) sealedParts.add(new Part(part.graph(), part.part()));
        marker(state, raw, boot, true, untrusted);
        org.slf4j.LoggerFactory.getLogger(RebuildData.class).info("维护封存完成：{} 个分片", sealed.size());
        return true;
    }
    /** A part became trusted (e.g. finished a full copy) after the last seal. */
    private boolean sealOutdated() {
        for (Part part : certifiedParts().keySet()) if (!sealedParts.contains(part)) return true;
        return false;
    }
    private void writeSeal(Seal seal) throws IOException {
        Path target = root.resolve(SEAL_FILE), temporary = root.resolve(SEAL_FILE + ".tmp");
        try (var channel = java.nio.channels.FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(JSON.writeValueAsBytes(seal)));
            channel.force(true);
        }
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        try (var directory = java.nio.channels.FileChannel.open(root, StandardOpenOption.READ)) { directory.force(true); }
        catch (IOException ignored) { /* Directory fsync is unavailable on some platforms. */ }
    }
    /** The marker is written only while the same maintenance is still active. */
    private void marker(ClusterMaintenance.State state, ByteString raw, String sealedBoot, boolean done, int failed) {
        var value = new ClusterMaintenance.NodeMarker(state.id(), boot, sealedBoot, done, failed);
        storage.write(0, 0, StorageGateway.batch("maintenance.marker/" + UUID.randomUUID())
                .addConditions(StorageGateway.condition(ClusterMaintenance.KEY, raw))
                .addMutations(TransactionEngine.put(ClusterMaintenance.nodeKey(config.nodeId()), ClusterMaintenance.encode(value))).build());
    }
    /** Leader-side summary for Studio's rolling-restart gate. */
    com.fasterxml.jackson.databind.node.ObjectNode maintenanceStatus() {
        var catalog = new DistributedCatalog(storage);
        ClusterMaintenance.State state = ClusterMaintenance.read(storage);
        var result = JSON.createObjectNode();
        result.put("active", state != null).put("id", state == null ? "" : state.id()).put("since", state == null ? 0 : state.since());
        Map<String, String> identities = new HashMap<>();
        for (Node node : catalog.nodes()) {
            String identity;
            if (node.getNodeId().equals(config.nodeId())) identity = boot;
            else try {
                var response = storage.direct(DistributedCatalog.address(node), "rebuild.identity", RebuildMessage.getDefaultInstance().toByteString(), Duration.ofSeconds(2));
                identity = response.getCode().equals("OK") ? response.getBody().toStringUtf8() : "";
            } catch (Exception ex) { identity = ""; }
            identities.put(node.getNodeId(), identity);
        }
        Map<String, Integer> untrustedReplicas = new HashMap<>();
        int total = 0, ready = 0, writing = 0, frozen = 0, lost = 0;
        for (GraphDefinition graph : catalog.graphs()) {
            if (!graph.getState().equals("ACTIVE")) continue;
            for (int part = 0; part < graph.getPartitions(); part++) {
                total++;
                ByteString bytes = storage.get(0, 0, key(graph.getId(), part));
                if (bytes == null) { lost++; continue; }
                RebuildHead current;
                try { current = RebuildHead.parseFrom(bytes); } catch (IOException ex) { lost++; continue; }
                switch (current.getState()) {
                    case "READY" -> ready++;
                    case "WRITING" -> writing++;
                    case "FROZEN" -> frozen++;
                    default -> lost++;
                }
                for (Node member : all(current.getSpec())) {
                    String live = identities.getOrDefault(member.getNodeId(), "");
                    boolean trusted = !live.isEmpty() && current.getReceiptsList().stream()
                            .anyMatch(r -> r.getNodeId().equals(member.getNodeId()) && r.getBootId().equals(live));
                    if (!trusted) untrustedReplicas.merge(member.getNodeId(), 1, Integer::sum);
                }
            }
        }
        result.putObject("partitions").put("total", total).put("ready", ready).put("writing", writing).put("frozen", frozen).put("lost", lost);
        var nodes = result.putArray("nodes");
        for (Node node : catalog.nodes()) {
            String live = identities.get(node.getNodeId());
            var marker = state == null ? null : ClusterMaintenance.marker(storage.get(0, 0, ClusterMaintenance.nodeKey(node.getNodeId())));
            if (marker != null && !marker.id().equals(state.id())) marker = null;
            boolean current = marker != null && marker.bootId().equals(live);
            nodes.addObject().put("nodeId", node.getNodeId()).put("reachable", !live.isEmpty()).put("bootId", live)
                    .put("sealed", current && marker.sealedBoot().equals(live))
                    .put("resumed", current && marker.resumed())
                    .put("untrustedReplicas", untrustedReplicas.getOrDefault(node.getNodeId(), 0));
        }
        return result;
    }
    /** Idempotent: returns the active maintenance id. */
    String enterMaintenance() {
        for (int attempt = 0; attempt < 16; attempt++) {
            ClusterMaintenance.State existing = ClusterMaintenance.read(storage);
            if (existing != null) return existing.id();
            var state = new ClusterMaintenance.State(UUID.randomUUID().toString(), System.currentTimeMillis());
            try {
                storage.write(0, 0, StorageGateway.batch("maintenance.enter/" + state.id())
                        .addConditions(StorageGateway.condition(ClusterMaintenance.KEY, null))
                        .addMutations(TransactionEngine.put(ClusterMaintenance.KEY, ClusterMaintenance.encode(state))).build());
                return state.id();
            } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        throw new ClusterException("BUSY", "维护模式状态并发更新繁忙");
    }
    void exitMaintenance() {
        var batch = StorageGateway.batch("maintenance.exit/" + UUID.randomUUID())
                .addMutations(Mutation.newBuilder().setKey(ClusterMaintenance.KEY).setDelete(true));
        for (Node node : new DistributedCatalog(storage).nodes())
            batch.addMutations(Mutation.newBuilder().setKey(ClusterMaintenance.nodeKey(node.getNodeId())).setDelete(true));
        storage.write(0, 0, batch.build());
    }
    private void cleanupRemoved() {
        // Every Storage runs this, not only the Metadata leader.
        for (Part part : List.copyOf(locals.keySet())) {
            try {
                RebuildHead current = head(part.graph, part.part);
                if (current.getState().equals("READY") && all(current.getSpec()).stream().noneMatch(n -> n.getNodeId().equals(config.nodeId()))) remove(current.getSpec());
            } catch (Exception ignored) { }
        }
    }
    GroupStatus status(long graph, int part) {
        RebuildHead current = head(graph, part);
        var result = GroupStatus.newBuilder().setAppliedIndex(current.getSequence());
        current.getSpec().getMembersList().forEach(n -> result.addMembers(n.getNodeId()));
        var available = holders(current, false);
        if (!available.isEmpty()) result.setLeaderId(available.getFirst().getNodeId());
        return result.build();
    }
    void remove(DataGroupSpec spec) throws IOException {
        Local local = locals.remove(new Part(spec.getGraphId(), spec.getPartId()));
        if (local != null) synchronized (local) { local.close(); }
        delete(root.resolve(spec.getGraphId() + "-" + spec.getPartId()));
    }
    void recoverTransactions(TransactionEngine transactions) {
        cleanupRemoved();
        // Idempotency records are written on every replica by applyReplica, but the Raft apply path
        // (which purges them) is not used in rebuild mode, so purge here instead.
        boolean purgeDue = System.currentTimeMillis() - lastIdempotencyCleanup > TimeUnit.MINUTES.toMillis(5);
        long cutoff = System.currentTimeMillis();
        for (Part part : locals.keySet()) {
            try {
                ByteString after = ByteString.EMPTY; String cursor = "";
                do {
                    var read = ReadKey.newBuilder().setKey(TransactionEngine.TX_PREFIX).setScan(true).setLimit(32).setMaxBytes(7 * 1024 * 1024)
                            .setAfterKey(after).setCursorId(cursor).build();
                    ScanPage page = ScanPage.parseFrom(read(part.graph, part.part, read.toByteString()));
                    for (var entry : page.getEntriesList()) {
                        var tx = TransactionRecord.parseFrom(entry.getValue());
                        if (!tx.getFinalized()) transactions.resume(tx);
                        else if (tx.getPlan().getParticipantsCount() > 0) new VersionMaintenance(storage).collect(tx, System.currentTimeMillis());
                    }
                    if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
                } while (true);
                after = ByteString.EMPTY; cursor = "";
                do {
                    ScanPage page = ScanPage.parseFrom(read(part.graph, part.part, ReadKey.newBuilder().setScan(true)
                            .setKey(TransactionEngine.text("__delete/")).setLimit(32).setMaxBytes(7 * 1024 * 1024)
                            .setAfterKey(after).setCursorId(cursor).build().toByteString()));
                    for (var entry : page.getEntriesList()) {
                        DeleteTask task = DeleteTask.parseFrom(entry.getValue());
                        if (!task.getCompleted()) new DistributedGraph(storage).resumeDelete(task);
                    }
                    if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
                } while (true);
                if (purgeDue) {
                    Local local = locals.get(part);
                    if (local != null) synchronized (local) {
                        if (locals.get(part) == local) {
                            int removed = local.db.purgeExpiredIdempotency(cutoff);
                            if (removed > 0) org.slf4j.LoggerFactory.getLogger(RebuildData.class)
                                    .info("分片 {}/{} 清理过期幂等记录 {} 条", part.graph, part.part, removed);
                        }
                    }
                }
            } catch (Exception ignored) { }
        }
        if (purgeDue) lastIdempotencyCleanup = cutoff;
    }
    private static RpcResponse ok(ByteString body) { return RpcResponse.newBuilder().setCode("OK").setBody(body).build(); }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch (Exception ex) { throw new IllegalStateException(ex); } }
    private static ByteString digest(ByteString value) { return ByteString.copyFrom(sha().digest(value.toByteArray())); }
    private static void hashEntry(MessageDigest hash, KeyValue entry) {
        hash.update(ByteBuffer.allocate(8).putInt(entry.getKey().size()).putInt(entry.getValue().size()).array());
        hash.update(entry.getKey().toByteArray()); hash.update(entry.getValue().toByteArray());
    }
    private static void delete(Path root) throws IOException {
        if (!Files.exists(root)) return;
        if (Files.isSymbolicLink(root)) throw new IOException("拒绝删除符号链接目录");
        try (var paths = Files.walk(root)) { for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
    }
    @Override public void close() throws IOException { for (Local local : locals.values()) local.close(); locals.clear(); }
}
