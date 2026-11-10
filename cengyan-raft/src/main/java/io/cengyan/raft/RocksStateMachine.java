package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.BatchResult;
import io.cengyan.protocol.proto.ClusterProtocol.IdempotencyRecord;
import io.cengyan.protocol.proto.ClusterProtocol.ReadKey;
import io.cengyan.protocol.proto.ClusterProtocol.StoredValue;
import io.cengyan.protocol.proto.ClusterProtocol.ScanPage;
import io.cengyan.protocol.proto.ClusterProtocol.KeyValue;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.TransactionRecord;
import io.cengyan.protocol.proto.ClusterProtocol.BatchValues;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * 分片状态机基础：数据、去重结果、应用位置在同一个 RocksDB 批次内提交。
 * 查询仅由 Ratis 的线性一致读 API 进入，不自行绕过读屏障。
 */
public final class RocksStateMachine extends BaseStateMachine {
    static { RocksDB.loadLibrary(); }
    private static final byte[] APPLIED = {0};
    private static final byte[] CONTROLLER_EPOCH = {0, 1};
    private static final String MANIFEST = "cengyan-snapshot.properties";
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final Object snapshotLock = new Object();
    private Options options;
    private WriteOptions writes;
    private RocksDB db;
    private Path stateDirectory;
    private boolean rebuildOnly;
    private record ScanCursor(Snapshot snapshot, ByteString query, long expiresAt, ByteString lastKey) { }
    private final java.util.Map<String, ScanCursor> scans = new java.util.HashMap<>();
    private final long snapshotBytesPerSecond;
    public RocksStateMachine() { this(64L * 1024 * 1024); }
    public RocksStateMachine(long snapshotBytesPerSecond) {
        if (snapshotBytesPerSecond < 1) throw new IllegalArgumentException("快照带宽必须为正数");
        this.snapshotBytesPerSecond = snapshotBytesPerSecond;
    }

    /** New process-owned database: never reopen this directory after a restart. */
    public static RocksStateMachine freshReplica(Path directory) throws IOException {
        var machine = new RocksStateMachine();
        machine.rebuildOnly = true;
        try {
            Files.createDirectories(directory);
            machine.options = new Options().setCreateIfMissing(true).setWriteBufferSize(4L * 1024 * 1024);
            machine.writes = new WriteOptions().setDisableWAL(true);
            machine.db = RocksDB.open(machine.options, directory.toString());
            EntityCountStore.initialize(machine.db, machine.writes);
            return machine;
        } catch (RocksDBException ex) { machine.closeDatabase(); throw new IOException(ex); }
    }

    /**
     * Reopen a replica sealed during cluster maintenance. The caller must verify the full
     * content against the seal and Metadata before trusting it.
     */
    public static RocksStateMachine reopenSealedReplica(Path directory) throws IOException {
        if (!Files.isDirectory(directory) || !Files.exists(directory.resolve("CURRENT"))) throw new IOException("封存的分片目录不存在：" + directory);
        var machine = new RocksStateMachine();
        machine.rebuildOnly = true;
        try {
            machine.options = new Options().setCreateIfMissing(false).setWriteBufferSize(4L * 1024 * 1024);
            machine.writes = new WriteOptions().setDisableWAL(true);
            machine.db = RocksDB.open(machine.options, directory.toString());
            EntityCountStore.initialize(machine.db, machine.writes);
            return machine;
        } catch (RocksDBException ex) { machine.closeDatabase(); throw new IOException(ex); }
    }

    /** Data replicas run without WAL; flush memtables before the files are trusted across a restart. */
    public synchronized void flushReplica() throws IOException {
        if (!rebuildOnly) throw new IOException("不是重建分片");
        try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) { db.flush(flush); }
        catch (RocksDBException ex) { throw new IOException(ex); }
    }

    /**
     * Rebuild replicas bypass the Raft apply path, so nothing else purges their idempotency
     * records. Delete records whose expiry has passed, keeping the ones that belong to
     * transactions, schema changes and deletes (they have their own lifecycle).
     * @return number of deleted records
     */
    public synchronized int purgeExpiredIdempotency(long cutoff) throws IOException {
        if (!rebuildOnly) throw new IOException("不是重建分片");
        byte[] prefix = namespace((byte) 1, new byte[0]);
        int deleted = 0;
        try (RocksIterator iterator = db.newIterator(); org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (iterator.seek(prefix); iterator.isValid() && startsWith(iterator.key(), prefix); iterator.next()) {
                String id = new String(iterator.key(), prefix.length, iterator.key().length - prefix.length, StandardCharsets.UTF_8);
                if (id.startsWith("tx.") || id.startsWith("schema.") || id.startsWith("delete.")) continue;
                if (IdempotencyRecord.parseFrom(iterator.value()).getExpiresAtMs() > cutoff) continue;
                batch.delete(iterator.key());
                deleted++;
            }
            iterator.status();
            if (deleted > 0) db.write(writes, batch);
        } catch (RocksDBException ex) { throw new IOException(ex); }
        return deleted;
    }

    public synchronized BatchResult applyReplica(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command, long sequence) throws IOException {
        if (!rebuildOnly) throw new IOException("不是重建分片");
        try { return BatchResult.parseFrom(applyCommand(command, sequence, 1).join().getContent().toByteArray()); }
        catch (RuntimeException ex) { throw new IOException("分片应用失败", ex); }
    }

    /** Caller has frozen writes in Metadata; no RocksDB Snapshot/Checkpoint is created. */
    public synchronized ScanPage exportReplica(ByteString after) throws IOException {
        if (!rebuildOnly) throw new IOException("不是重建分片");
        var page = ScanPage.newBuilder(); int bytes = 0;
        try (RocksIterator iterator = db.newIterator()) {
            if (after.isEmpty()) iterator.seekToFirst();
            else { iterator.seek(after.toByteArray()); if (iterator.isValid() && java.util.Arrays.equals(iterator.key(), after.toByteArray())) iterator.next(); }
            while (iterator.isValid()) {
                byte[] key = iterator.key(), value = iterator.value();
                if (page.getEntriesCount() > 0 && bytes + key.length + value.length > 1024 * 1024) { page.setMore(true); break; }
                if (key.length + value.length > 24 * 1024 * 1024) throw new IOException("单条迁移记录过大");
                page.addEntries(KeyValue.newBuilder().setKey(ByteString.copyFrom(key)).setValue(ByteString.copyFrom(value)));
                page.setLastKey(ByteString.copyFrom(key)); bytes += key.length + value.length; iterator.next();
            }
            iterator.status(); return page.build();
        } catch (RocksDBException ex) { throw new IOException(ex); }
    }

    public synchronized void importReplica(ScanPage page) throws IOException {
        if (!rebuildOnly) throw new IOException("不是重建分片");
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (var entry : page.getEntriesList()) batch.put(entry.getKey().toByteArray(), entry.getValue().toByteArray());
            db.write(writes, batch);
        } catch (RocksDBException ex) { throw new IOException(ex); }
    }

    @Override public synchronized void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        stateDirectory = raftStorage.getStorageDir().getStateMachineDir().toPath();
        Files.createDirectories(stateDirectory);
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }

    @Override public synchronized void pause() {
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSING);
        closeDatabase();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSED);
    }
    @Override public synchronized void reinitialize() throws IOException {
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }
    @Override public StateMachineStorage getStateMachineStorage() { return storage; }

    private void restore() throws IOException {
        closeDatabase();
        // 工作库可重建：已发布 Checkpoint + 后续 Raft 日志是恢复来源。
        Path next = stateDirectory.resolve("work-" + UUID.randomUUID());
        Files.createDirectory(next);
        var snapshot = storage.loadLatestSnapshot();
        if (snapshot == null) {
            try (var files = Files.list(stateDirectory)) {
                if (files.anyMatch(file -> SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(file.getFileName().toString()).matches())) {
                    throw new IOException("存在已发布快照但无法加载，拒绝从空数据库恢复");
                }
            }
        }
        try {
            if (snapshot != null) {
                Path file = snapshot.getFile().getPath();
                var expected = snapshot.getFile().getFileDigest();
                if (expected != null && !expected.equals(MD5FileUtil.computeMd5ForFile(file.toFile()))) {
                    throw new IOException("Raft 快照校验失败");
                }
                unpack(file, next);
                java.util.Properties manifest = new java.util.Properties();
                try (var input = Files.newInputStream(next.resolve(MANIFEST))) { manifest.load(input); }
                if (!"1".equals(manifest.getProperty("format"))
                        || !getGroupId().getUuid().toString().equals(manifest.getProperty("group_id"))
                        || !Long.toString(snapshot.getIndex()).equals(manifest.getProperty("index"))
                        || !Long.toString(snapshot.getTerm()).equals(manifest.getProperty("term"))) {
                    throw new IOException("快照身份、格式或应用位置不匹配");
                }
                Files.delete(next.resolve(MANIFEST));
            }
            options = new Options().setCreateIfMissing(true).setWriteBufferSize(4L * 1024 * 1024);
            writes = new WriteOptions().setSync(true);
            db = RocksDB.open(options, next.toString());
            byte[] applied = db.get(APPLIED);
            TermIndex position = applied == null ? TermIndex.INITIAL_VALUE
                    : decodePosition(applied);
            if (snapshot != null && !snapshot.getTermIndex().equals(position)) {
                throw new IOException("快照数据与 Raft 应用位置不一致");
            }
            EntityCountStore.initialize(db, writes);
            setLastAppliedTermIndex(position);
            // 只删除本状态机工作目录，不触及用户数据目录或 Raft 日志。
            try (var files = Files.list(stateDirectory)) {
                for (Path old : files.filter(p -> p.getFileName().toString().startsWith("work-")).toList()) {
                    if (!old.equals(next)) deleteOwnedTree(old);
                }
            }
        } catch (RocksDBException | IOException | RuntimeException ex) {
            closeDatabase();
            throw new IOException("无法恢复分片状态机", ex);
        }
    }

    @Override public synchronized CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var entry = trx.getLogEntry();
        try { return applyCommand(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch.parseFrom(
                entry.getStateMachineLogEntry().getLogData().toByteArray()), entry.getIndex(), entry.getTerm()); }
        catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
    }

    private CompletableFuture<Message> applyCommand(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command, long index, long term) {
        try {
            if (index <= getLastAppliedTermIndex().getIndex()) throw new IOException("Raft 日志重复或倒序应用");
            BatchResult result;
            byte[] key = namespace((byte) 1, command.getRequestId().getBytes(StandardCharsets.UTF_8));
            byte[] digest = digest(command.toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteArray());
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                byte[] currentEpochBytes = db.get(CONTROLLER_EPOCH);
                long currentEpoch = currentEpochBytes == null ? 0 : ByteBuffer.wrap(currentEpochBytes).getLong();
                if (command.getControllerEpoch() != 0 && command.getControllerEpoch() < currentEpoch) {
                    result = failure("STALE_CONTROLLER", "控制命令来自旧 Metadata Leader 任期");
                } else if (command.getRequestId().isBlank() || command.getRequestId().length() > 256
                        || (command.getMutationsCount() == 0 && command.getPurgeRequestIdsCount() == 0)
                        || command.getExpiresAtMs() <= 0
                        || (command.getPurgeRequestIdsCount() > 0 && command.getPurgeBeforeMs() <= 0)) {
                    result = failure("INVALID_ARGUMENT", "写入批次标识、内容或有效期无效");
                } else {
                    byte[] previous = db.get(key);
                    if (previous != null) {
                        var record = IdempotencyRecord.parseFrom(previous);
                        result = MessageDigest.isEqual(digest, record.getDigest().toByteArray()) ? record.getResult()
                                : failure("REQUEST_CONFLICT", "相同 requestId 对应不同写入内容；requestId=" + command.getRequestId());
                    } else {
                        boolean matches = true;
                        for (var condition : command.getConditionsList()) {
                            byte[] current = db.get(namespace((byte) 2, condition.getKey().toByteArray()));
                            if (condition.getExpectedPresent() != (current != null)
                                    || (current != null && !java.util.Arrays.equals(current, condition.getExpectedValue().toByteArray()))) {
                                matches = false;
                                break;
                            }
                        }
                        long affected = 0;
                        if (matches) {
                            var finalValues = new java.util.LinkedHashMap<ByteString, byte[]>();
                            for (var op : command.getMutationsList()) {
                                byte[] dataKey = namespace((byte) 2, op.getKey().toByteArray());
                                if (op.getDelete()) batch.delete(dataKey);
                                else batch.put(dataKey, op.getValue().toByteArray());
                                if (op.getKey().size() > 9 && (op.getKey().byteAt(0) == 16 || op.getKey().byteAt(0) == 17)) {
                                    finalValues.put(ByteString.copyFrom(dataKey), op.getDelete() ? null : op.getValue().toByteArray());
                                }
                                affected++;
                            }
                            EntityCountStore.update(db, batch, finalValues);
                            for (String expiredId : command.getPurgeRequestIdsList()) {
                                byte[] expiredKey = namespace((byte) 1, expiredId.getBytes(StandardCharsets.UTF_8));
                                byte[] encoded = db.get(expiredKey);
                                if (encoded != null && !expiredId.equals(command.getRequestId())
                                        && IdempotencyRecord.parseFrom(encoded).getExpiresAtMs() <= command.getPurgeBeforeMs()) {
                                    batch.delete(expiredKey);
                                    affected++;
                                }
                            }
                        }
                        result = matches ? BatchResult.newBuilder().setCode("OK").setAffected(affected).build()
                                : failure("WRITE_CONFLICT", "写入对象的预期状态已改变");
                        batch.put(key, IdempotencyRecord.newBuilder().setDigest(ByteString.copyFrom(digest))
                                .setResult(result).setExpiresAtMs(command.getExpiresAtMs()).build().toByteArray());
                    }
                }
                if (command.getControllerEpoch() > currentEpoch) batch.put(CONTROLLER_EPOCH, ByteBuffer.allocate(8).putLong(command.getControllerEpoch()).array());
                batch.put(APPLIED, position(term, index));
                db.write(writes, batch);
            }
            updateLastAppliedTermIndex(term, index);
            return CompletableFuture.completedFuture(message(result.toByteArray()));
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(new IOException("分片状态机应用失败", ex));
        }
    }

    @Override public synchronized void notifyTermIndexUpdated(long term, long index) {
        if (index <= getLastAppliedTermIndex().getIndex()) return;
        try {
            db.put(writes, APPLIED, position(term, index));
            updateLastAppliedTermIndex(term, index);
        } catch (RocksDBException ex) { throw new IllegalStateException("无法持久化 Raft 控制日志进度", ex); }
    }

    @Override public synchronized CompletableFuture<Message> query(Message request) {
        try {
            var key = ReadKey.parseFrom(request.getContent().toByteArray());
            if (key.getEntityCounts()) {
                return CompletableFuture.completedFuture(message(EntityCountStore.read(db, key.getCountsGeneration()).toByteArray()));
            }
            if (key.getKeysCount() > 0) {
                if (key.getKeysCount() > 256) throw new IOException("批量点读最多 256 个键");
                var values = BatchValues.newBuilder();
                long bytes = 0;
                for (ByteString requested : key.getKeysList()) {
                    byte[] value = db.get(namespace((byte) 2, requested.toByteArray()));
                    var found = StoredValue.newBuilder().setFound(value != null);
                    if (value != null) { bytes += value.length; found.setValue(ByteString.copyFrom(value)); }
                    if (bytes > 7 * 1024 * 1024) throw new IOException("批量点读结果超过字节上限");
                    values.addValues(found);
                }
                return CompletableFuture.completedFuture(message(values.build().toByteArray()));
            }
            long now = System.nanoTime();
            var expired = scans.entrySet().stream().filter(entry -> now - entry.getValue().expiresAt() >= 0).map(java.util.Map.Entry::getKey).toList();
            for (String id : expired) releaseScan(id);
            if (key.getCloseCursor()) {
                releaseScan(key.getCursorId());
                return CompletableFuture.completedFuture(message(ScanPage.getDefaultInstance().toByteArray()));
            }
            if (key.getScan()) {
                if (key.getLimit() < 1 || key.getLimit() > 10000 || key.getMaxBytes() < 1024 || key.getMaxBytes() > 7 * 1024 * 1024) {
                    throw new IOException("扫描批次行数或字节限制无效");
                }
                byte namespace = key.getScanIdempotency() ? (byte) 1 : (byte) 2;
                byte[] prefix = namespace(namespace, key.getKey().toByteArray());
                byte[] after = key.getAfterKey().isEmpty() ? prefix : namespace(namespace, key.getAfterKey().toByteArray());
                String cursorId = key.getCursorId();
                ByteString queryIdentity = key.toBuilder().clearAfterKey().clearCursorId().clearScanId().clearLimit().clearMaxBytes().build().toByteString();
                ScanCursor cursor;
                if (cursorId.isEmpty()) {
                    if (!key.getAfterKey().isEmpty()) throw new IOException("续扫必须携带有效快照游标");
                    if (scans.size() >= 128) throw new IOException("分片快照游标数量已达上限");
                    cursorId = key.getScanId().isBlank() ? UUID.randomUUID().toString() : key.getScanId();
                    if (scans.containsKey(cursorId)) throw new IOException("扫描请求标识已存在，禁止从头重放已开始扫描");
                    cursor = new ScanCursor(db.getSnapshot(), queryIdentity, now + java.util.concurrent.TimeUnit.SECONDS.toNanos(30), ByteString.EMPTY);
                    scans.put(cursorId, cursor);
                } else {
                    cursor = scans.get(cursorId);
                    if (cursor == null || !cursor.query().equals(queryIdentity) || !cursor.lastKey().equals(key.getAfterKey())) throw new IOException("分片扫描游标已失效、查询改变或顺序不匹配");
                }
                var page = ScanPage.newBuilder();
                int bytes = 0;
                try (ReadOptions options = new ReadOptions().setSnapshot(cursor.snapshot()); RocksIterator iterator = db.newIterator(options)) {
                    iterator.seek(after);
                    if (!key.getAfterKey().isEmpty() && iterator.isValid() && java.util.Arrays.equals(iterator.key(), after)) iterator.next();
                    while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                        byte[] dataKey = iterator.key(), value = iterator.value();
                        if (key.getTransactionSummaries()) {
                            TransactionRecord transaction = TransactionRecord.parseFrom(value);
                            value = transaction.toBuilder().setPlan(transaction.getPlan().toBuilder().clearParticipants()).build().toByteArray();
                        }
                        if (!key.getEntityType().isBlank()) {
                            VersionedCell cell = VersionedCell.parseFrom(value);
                            boolean possible = false;
                            // 不能仅按最新准备版本过滤：旧查询还可能需要旧已提交版本。
                            for (var version : cell.getVersionsList()) {
                                if (version.getDeleted()) continue;
                                Entity entity = Entity.parseFrom(version.getValue());
                                if (!entity.getType().equals(key.getEntityType())) continue;
                                boolean match = true;
                                for (var filter : key.getFiltersList()) {
                                    var actual = filter.getProperty().equals("id") ? entity.getId() : entity.getPropertiesMap().get(filter.getProperty());
                                    if (!filter.getExpected().equals(actual)) { match = false; break; }
                                }
                                if (match) { possible = true; break; }
                            }
                            if (!possible) { iterator.next(); continue; }
                        }
                        int cost = dataKey.length + value.length + 32;
                        if (cost > key.getMaxBytes()) throw new IOException("单条扫描结果超出批次字节限制");
                        if (page.getEntriesCount() >= key.getLimit() || bytes + cost > key.getMaxBytes()) {
                            page.setMore(true);
                            break;
                        }
                        ByteString logicalKey = ByteString.copyFrom(dataKey, 1, dataKey.length - 1);
                        page.addEntries(KeyValue.newBuilder().setKey(logicalKey).setValue(ByteString.copyFrom(value)));
                        page.setLastKey(logicalKey);
                        bytes += cost;
                        iterator.next();
                    }
                    iterator.status();
                } catch (Exception ex) {
                    releaseScan(cursorId);
                    throw ex;
                }
                if (page.getMore()) {
                    page.setCursorId(cursorId);
                    scans.put(cursorId, new ScanCursor(cursor.snapshot(), cursor.query(), now + java.util.concurrent.TimeUnit.SECONDS.toNanos(30), page.getLastKey()));
                } else releaseScan(cursorId);
                return CompletableFuture.completedFuture(message(page.build().toByteArray()));
            }
            byte[] value = db.get(namespace((byte) 2, key.getKey().toByteArray()));
            var result = StoredValue.newBuilder().setFound(value != null);
            if (value != null) result.setValue(ByteString.copyFrom(value));
            return CompletableFuture.completedFuture(message(result.build().toByteArray()));
        } catch (Exception ex) { return CompletableFuture.failedFuture(new IOException("分片读取失败", ex)); }
    }

    @Override public long takeSnapshot() throws IOException {
        if (rebuildOnly) throw new IOException("重建分片不生成快照");
        synchronized (snapshotLock) { return snapshotLocked(); }
    }

    private long snapshotLocked() throws IOException {
        Path checkpoint = stateDirectory.resolve("checkpoint-" + UUID.randomUUID());
        final TermIndex position;
        synchronized (this) {
            position = getLastAppliedTermIndex();
            if (position.getIndex() < 0) return -1;
            var current = storage.getLatestSnapshot();
            if (current != null && current.getIndex() == position.getIndex()) return position.getIndex();
            RocksDBException failure = null;
            for (int attempt = 0; attempt < 3; attempt++) {
                try (Checkpoint cp = Checkpoint.create(db)) {
                    cp.createCheckpoint(checkpoint.toString());
                    failure = null;
                    break;
                } catch (RocksDBException ex) {
                    failure = ex;
                    // Windows 文件扫描器可能短暂占用 Checkpoint 内部的临时目录。
                    // 只清理本次随机路径，失败快照从不发布，也不推进可清日志位置。
                    deleteOwnedTree(checkpoint);
                    deleteOwnedTree(checkpoint.resolveSibling(checkpoint.getFileName() + ".tmp"));
                    if (attempt < 2) {
                        java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50L * (attempt + 1)));
                        if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("Checkpoint 创建被取消");
                    }
                }
            }
            if (failure != null) throw new IOException("无法创建 RocksDB Checkpoint", failure);
        }
        Path target = storage.getSnapshotFile(position.getTerm(), position.getIndex()).toPath();
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp-" + UUID.randomUUID());
        // SST 打包和校验不持有状态机锁，源 Leader 可以继续应用新写入。
        try {
            java.util.Properties manifest = new java.util.Properties();
            manifest.setProperty("format", "1");
            manifest.setProperty("group_id", getGroupId().getUuid().toString());
            manifest.setProperty("term", Long.toString(position.getTerm()));
            manifest.setProperty("index", Long.toString(position.getIndex()));
            try (var output = Files.newOutputStream(checkpoint.resolve(MANIFEST))) { manifest.store(output, "Cengyan Checkpoint"); }
            try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(temporary)); var files = Files.list(checkpoint)) {
                zip.setLevel(0);
                long started = System.nanoTime(), copied = 0;
                byte[] buffer = new byte[256 * 1024];
                for (Path file : files.sorted().toList()) {
                    if (!Files.isRegularFile(file)) throw new IOException("Checkpoint 存在不支持的文件");
                    zip.putNextEntry(new ZipEntry(file.getFileName().toString()));
                    try (InputStream input = Files.newInputStream(file)) {
                        int count;
                        while ((count = input.read(buffer)) >= 0) {
                            zip.write(buffer, 0, count); copied += count;
                            long targetElapsed = (long) ((double) copied * 1_000_000_000L / snapshotBytesPerSecond);
                            long wait = targetElapsed - (System.nanoTime() - started);
                            if (wait > 0) java.util.concurrent.locks.LockSupport.parkNanos(wait);
                            if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("快照生成被取消");
                        }
                    }
                    zip.closeEntry();
                }
            }
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            var md5 = MD5FileUtil.computeMd5ForFile(target.toFile());
            MD5FileUtil.saveMD5File(target.toFile(), md5);
            storage.updateLatestSnapshot(new SingleFileSnapshotInfo(new FileInfo(target, md5), position));
            return position.getIndex();
        } finally {
            Files.deleteIfExists(temporary);
            deleteOwnedTree(checkpoint);
        }
    }

    private static void unpack(Path snapshot, Path directory) throws IOException {
        try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(snapshot))) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                String name = entry.getName();
                // Checkpoint 仅包含一层文件；禁止路径穿越和任意目录写入。
                if (name.isBlank() || name.contains("/") || name.contains("\\") || name.equals("..") || name.contains(":")) {
                    throw new IOException("快照文件路径无效");
                }
                try (OutputStream out = Files.newOutputStream(directory.resolve(name), StandardOpenOption.CREATE_NEW)) {
                    zip.transferTo(out);
                }
                zip.closeEntry();
            }
        }
    }

    private static void deleteOwnedTree(Path directory) throws IOException {
        if (!Files.exists(directory)) return;
        if (Files.isSymbolicLink(directory)) throw new IOException("状态机临时目录不能是符号链接");
        try (var files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
        }
    }

    private static byte[] namespace(byte prefix, byte[] key) {
        return ByteBuffer.allocate(1 + key.length).put(prefix).put(key).array();
    }
    private static boolean startsWith(byte[] key, byte[] prefix) {
        return key.length >= prefix.length && java.util.Arrays.mismatch(key, 0, prefix.length, prefix, 0, prefix.length) < 0;
    }
    private static byte[] position(long term, long index) { return ByteBuffer.allocate(16).putLong(term).putLong(index).array(); }
    private static TermIndex decodePosition(byte[] bytes) throws IOException {
        if (bytes.length != 16) throw new IOException("应用位置记录损坏");
        ByteBuffer in = ByteBuffer.wrap(bytes);
        return TermIndex.valueOf(in.getLong(), in.getLong());
    }
    private static byte[] digest(byte[] value) {
        try { return MessageDigest.getInstance("SHA-256").digest(value); }
        catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
    private static BatchResult failure(String code, String text) { return BatchResult.newBuilder().setCode(code).setMessage(text).build(); }
    public static Message message(byte[] bytes) {
        return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes));
    }
    private void closeDatabase() {
        if (db != null) for (String id : java.util.List.copyOf(scans.keySet())) releaseScan(id);
        if (db != null) { db.close(); db = null; }
        if (writes != null) { writes.close(); writes = null; }
        if (options != null) { options.close(); options = null; }
    }
    private void releaseScan(String id) {
        ScanCursor cursor = scans.remove(id);
        if (cursor != null && db != null) db.releaseSnapshot(cursor.snapshot());
    }
    @Override public synchronized void close() {
        if (rebuildOnly) { closeDatabase(); return; }
        closeDatabase();
        if (getLifeCycle().getCurrentState() != org.apache.ratis.util.LifeCycle.State.CLOSED) {
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSING);
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSED);
        }
    }
}
