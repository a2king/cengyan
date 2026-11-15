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
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.rocksdb.*;
import java.io.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * 分片状态机基础：数据、去重结果、应用位置在同一个 RocksDB 批次内提交。
 * 查询仅由 Ratis 的线性一致读 API 进入，不自行绕过读屏障。
 */
public final class RocksStateMachine extends BaseStateMachine {
    static { RocksDB.loadLibrary(); }
    private static final byte[] APPLIED = {0};
    private static final byte[] CONTROLLER_EPOCH = {0, 1};
    private static final Logger LOG = Logger.getLogger(RocksStateMachine.class.getName());
    private static final int IMPORT_L0_SLOWDOWN = 1024;
    private static final int IMPORT_L0_STOP = 4096;
    private static final int NORMAL_L0_SLOWDOWN = 20;
    private static final int NORMAL_L0_STOP = 36;
    private static final ExecutorService IMPORT_COMPACTOR = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "storage-import-compact");
        thread.setDaemon(true);
        return thread;
    });
    private static final CopyOnWriteArrayList<RocksStateMachine> LIVE = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService IMPORT_COMPACT_CLOCK = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "storage-import-compact-clock");
        thread.setDaemon(true);
        return thread;
    });
    static {
        IMPORT_COMPACT_CLOCK.scheduleWithFixedDelay(RocksStateMachine::compactDue, 60, 60, TimeUnit.SECONDS);
    }
    private final CheckpointStorage storage = new CheckpointStorage();
    private final Object snapshotLock = new Object();
    private final Set<String> importSessions = new HashSet<>();
    /** 读不挡住写，写不挡住其他分片。同一分片的 apply 仍然互斥。 */
    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private static final Cache BLOCK_CACHE = new LRUCache(256L * 1024 * 1024);
    private static final Filter BLOOM = new BloomFilter(10);
    private Options options;
    private WriteOptions writes;
    private RocksDB db;
    private Path stateDirectory;
    private Path liveDirectory;
    private Path importStaging;
    /** 导入期间关掉自动压实，并抬高 L0 停写阈值。进程重启后恢复为打开，不写入配置。 */
    private boolean compactionPaused;
    /** 本进程所有分片共用一把锁，保证压实和关闭数据库不会同时进行。 */
    private final ReentrantLock compactExclusive = new ReentrantLock();
    private final Object compactState = new Object();
    private final java.util.Map<String, AppendDecision> appendSessions = new java.util.HashMap<>();
    /** 测试可改小，用来触发一轮导入压实。 */
    long importCompactWaveBytes = 1L << 30;
    /** 未压实数据攒够这个时间后，即使不足 1GB 也压一次。测试可改成 0。 */
    long importCompactIdleMs = 3_600_000L;
    private long bytesSinceCompact;
    private long uncompactedSinceMs;
    private boolean compactQueued;
    private volatile boolean closing;
    private volatile int completedCompactions;
    private record AppendDecision(long generation, boolean appendOnly) { }
    private boolean rebuildOnly;
    private record ScanCursor(Snapshot snapshot, ByteString query, long expiresAt, ByteString lastKey) { }
    private final java.util.Map<String, ScanCursor> scans = new java.util.HashMap<>();
    private final long snapshotBytesPerSecond;
    /** 图空间已经删除。新的导入和查询直接拒绝；正在进行的调用结束于关闭数据库之前。 */
    private volatile boolean dropping;
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
            machine.liveDirectory = directory;
            machine.importStaging = directory.resolve("import-staging");
            Files.createDirectories(machine.importStaging);
            machine.options = databaseOptions(true);
            machine.writes = new WriteOptions().setDisableWAL(true).setSync(false);
            machine.db = RocksDB.open(machine.options, directory.toString());
            EntityCountStore.initialize(machine.db, machine.writes);
            machine.register();
            return machine;
        } catch (RocksDBException ex) { machine.closeDatabase(); throw new IOException(ex); }
    }

    /** 重新打开已经存在的分片目录。调用方需要先确认目录属于本集群。 */
    public static RocksStateMachine reopenSealedReplica(Path directory) throws IOException {
        if (!Files.isDirectory(directory) || !Files.exists(directory.resolve("CURRENT"))) throw new IOException("封存的分片目录不存在：" + directory);
        var machine = new RocksStateMachine();
        machine.rebuildOnly = true;
        machine.liveDirectory = directory;
        machine.importStaging = directory.resolve("import-staging");
        Files.createDirectories(machine.importStaging);
        try {
            machine.options = databaseOptions(false);
            machine.writes = new WriteOptions().setDisableWAL(true).setSync(false);
            machine.db = RocksDB.open(machine.options, directory.toString());
            EntityCountStore.initialize(machine.db, machine.writes);
            machine.register();
            return machine;
        } catch (RocksDBException ex) { machine.closeDatabase(); throw new IOException(ex); }
    }

    /** 把 SST 分块写到本副本。文件不进入 Raft 日志；摄入由随后的导入指令触发。 */
    public void stageImport(io.cengyan.protocol.proto.ClusterProtocol.ImportChunk chunk) throws IOException {
        stageImport(chunk, System.currentTimeMillis());
    }

    void stageImport(io.cengyan.protocol.proto.ClusterProtocol.ImportChunk chunk, long now) throws IOException {
        if (importStaging == null) throw new IOException("导入暂存目录尚未就绪");
        dbLock.writeLock().lock();
        try {
            if (dropping || db == null) throw new IOException("图空间已删除");
            boolean started = !importSessions.contains(chunk.getSessionId());
            ImportApplier.stage(importStaging, chunk, now);
            if (importSessions.add(chunk.getSessionId()) && started) pauseCompaction();
        }
        finally { dbLock.writeLock().unlock(); }
    }

    /** 刷新导入会话心跳。没有心跳满 30 分钟后，暂存文件会被删掉。 */
    public void heartbeatImport(String sessionId) throws IOException {
        heartbeatImport(sessionId, System.currentTimeMillis());
    }

    void heartbeatImport(String sessionId, long now) throws IOException {
        if (dropping) return;
        if (importStaging == null) throw new IOException("导入暂存目录尚未就绪");
        dbLock.writeLock().lock();
        try {
            if (dropping || db == null) return;
            ImportSessions.heartbeat(importStaging, sessionId, now);
        }
        finally { dbLock.writeLock().unlock(); }
    }

    /**
     * 导入会话结束。只清理会话和暂存文件，不等待压实。
     * L0 降到默认停写线以下、并且没有导入会话时，后台再恢复自动压实。
     * @return 本分片是否持有这个会话
     */
    public boolean finishImport(String sessionId) throws IOException {
        dbLock.writeLock().lock();
        try {
            if (importStaging == null || !importSessions.contains(sessionId)) return false;
            importSessions.remove(sessionId);
            appendSessions.remove(sessionId);
            ImportSessions.close(importStaging, sessionId);
            if (importSessions.isEmpty()) resumeCompactionIfSettled();
            return true;
        } finally { dbLock.writeLock().unlock(); }
    }

    /** 计划内重启前把内存表刷进 SST，使已应用位置和工作库一起落盘。 */
    public void flushForRestart() throws IOException {
        dbLock.writeLock().lock();
        try { flushHeld(); }
        finally { dbLock.writeLock().unlock(); }
    }

    /** @return 本次删掉的暂存文件数 */
    public int expireImports(long now) throws IOException {
        if (importStaging == null) return 0;
        dbLock.writeLock().lock();
        try { return ImportSessions.expire(importStaging, now); }
        finally { dbLock.writeLock().unlock(); }
    }

    /** 拒绝这张图上新的导入和查询。已进入的调用在关闭数据库时排空，避免文件句柄把空间留住。 */
    void beginDrop() { dropping = true; }

    /** 在已经持有本地 SST 的分片上直接摄入。Raft 路径走 apply，不重复进入这里。 */
    public io.cengyan.protocol.proto.ClusterProtocol.BatchResult applyImport(
            io.cengyan.protocol.proto.ClusterProtocol.ImportDirective directive) throws IOException {
        dbLock.writeLock().lock();
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            if (dropping || db == null) return failure("GRAPH_STATE", "图空间已删除");
            var result = ingestHeld(batch, directive);
            db.write(writes, batch);
            ImportApplier.release(importStaging, directive.getChecksum().toByteArray());
            return result;
        } catch (RocksDBException ex) {
            throw new IOException(ex);
        } finally {
            dbLock.writeLock().unlock();
            compactIfDue(System.currentTimeMillis());
        }
    }

    byte[] readPhysical(byte[] key) throws org.rocksdb.RocksDBException {
        dbLock.readLock().lock();
        try { return db.get(key); }
        finally { dbLock.readLock().unlock(); }
    }

    io.cengyan.protocol.proto.ClusterProtocol.EntityCounts readCounts(long generation) throws IOException {
        dbLock.readLock().lock();
        try { return EntityCountStore.read(db, generation); }
        catch (org.rocksdb.RocksDBException ex) { throw new IOException(ex); }
        finally { dbLock.readLock().unlock(); }
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
        importStaging = stateDirectory.resolve("import-staging");
        Files.createDirectories(importStaging);
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }

    @Override public synchronized void pause() {
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSING);
        closing = true;
        compactExclusive.lock();
        try {
            try { flushHeld(); }
            catch (IOException ex) { LOG.warning("暂停前刷盘失败：" + ex.getMessage()); }
            synchronized (compactState) {
                bytesSinceCompact = 0;
                uncompactedSinceMs = 0;
            }
            closeDatabase();
        } finally { compactExclusive.unlock(); }
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSED);
    }
    @Override public synchronized void reinitialize() throws IOException {
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }
    @Override public StateMachineStorage getStateMachineStorage() { return storage; }

    private void restore() throws IOException {
        closing = true;
        compactExclusive.lock();
        try { closeDatabase(); }
        finally { compactExclusive.unlock(); }
        importSessions.clear();
        appendSessions.clear();
        compactionPaused = false;
        synchronized (compactState) {
            bytesSinceCompact = 0;
            uncompactedSinceMs = 0;
        }
        Files.createDirectories(stateDirectory);
        importStaging = stateDirectory.resolve("import-staging");
        Files.createDirectories(importStaging);
        Path dbDir = CheckpointStorage.dbDirectory(stateDirectory);
        TermIndex adopted = null;
        if (!Files.exists(dbDir.resolve("CURRENT"))) {
            Path installed = CheckpointStorage.installedSnapshot(stateDirectory);
            if (installed != null) {
                adopted = CheckpointStorage.termIndex(installed);
                stripDigestFiles(installed);
                CheckpointStorage.deleteTree(dbDir);
                Files.move(installed, dbDir);
            }
        }
        try {
            boolean create = !Files.exists(dbDir.resolve("CURRENT"));
            if (create) Files.createDirectories(dbDir);
            options = databaseOptions(create);
            // Raft 日志已经 fsync。内存表没刷盘的部分由日志重放补上，不再为每次写入打开 RocksDB WAL。
            writes = new WriteOptions().setDisableWAL(true).setSync(false);
            db = RocksDB.open(options, dbDir.toString());
            liveDirectory = dbDir;
            closing = false;
            register();
            storage.retain(dbDir);
            byte[] applied = db.get(APPLIED);
            TermIndex position = applied == null ? TermIndex.INITIAL_VALUE : decodePosition(applied);
            if (adopted != null && !adopted.equals(position)) throw new IOException("安装的快照与库内应用位置不一致");
            EntityCountStore.initialize(db, writes);
            setLastAppliedTermIndex(position);
            if (adopted != null) storage.capture(db, adopted.getTerm(), adopted.getIndex(), snapshotBytesPerSecond);
            else storage.reload();
        } catch (RocksDBException | IOException | RuntimeException ex) {
            closeDatabase();
            throw new IOException("无法恢复分片状态机", ex);
        }
    }

    /** 安装快照时 Ratis 会在文件旁写下校验和。RocksDB 不使用这些旁路文件。 */
    private static void stripDigestFiles(Path directory) throws IOException {
        try (var files = Files.list(directory)) {
            for (Path file : files.toList()) {
                if (file.getFileName().toString().endsWith(".md5")) Files.deleteIfExists(file);
            }
        }
    }

    @Override public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        dbLock.writeLock().lock();
        try {
            var entry = trx.getLogEntry();
            try { return applyCommand(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch.parseFrom(
                    entry.getStateMachineLogEntry().getLogData().toByteArray()), entry.getIndex(), entry.getTerm()); }
            catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
        } finally {
            dbLock.writeLock().unlock();
            compactIfDue(System.currentTimeMillis());
        }
    }

    private CompletableFuture<Message> applyCommand(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command, long index, long term) {
        try {
            if (index <= getLastAppliedTermIndex().getIndex()) throw new IOException("Raft 日志重复或倒序应用");
            BatchResult result;
            boolean releaseImport = false;
            byte[] key = namespace((byte) 1, command.getRequestId().getBytes(StandardCharsets.UTF_8));
            byte[] digest = digest(command.toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteArray());
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                byte[] currentEpochBytes = db.get(CONTROLLER_EPOCH);
                long currentEpoch = currentEpochBytes == null ? 0 : ByteBuffer.wrap(currentEpochBytes).getLong();
                boolean importing = command.hasImportFile();
                if (command.getControllerEpoch() != 0 && command.getControllerEpoch() < currentEpoch) {
                    result = failure("STALE_CONTROLLER", "控制命令来自旧 Metadata Leader 任期");
                } else if (command.getRequestId().isBlank() || command.getRequestId().length() > 256
                        || (command.getMutationsCount() == 0 && command.getPurgeRequestIdsCount() == 0 && !importing)
                        || command.getExpiresAtMs() <= 0
                        || (command.getPurgeRequestIdsCount() > 0 && command.getPurgeBeforeMs() <= 0)) {
                    result = failure("INVALID_ARGUMENT", "写入批次标识、内容或有效期无效");
                } else {
                    byte[] previous = db.get(key);
                    if (previous != null) {
                        var record = IdempotencyRecord.parseFrom(previous);
                        result = MessageDigest.isEqual(digest, record.getDigest().toByteArray()) ? record.getResult()
                                : failure("REQUEST_CONFLICT", "相同 requestId 对应不同写入内容；requestId=" + command.getRequestId());
                    } else if (importing) {
                        if (command.getMutationsCount() > 0 || command.getConditionsCount() > 0 || command.getPurgeRequestIdsCount() > 0) {
                            result = failure("INVALID_ARGUMENT", "导入指令不能混入普通写入");
                        } else if (dropping) {
                            result = failure("GRAPH_STATE", "图空间已删除");
                        } else {
                            result = ingestHeld(batch, command.getImportFile());
                            releaseImport = true;
                        }
                        batch.put(key, IdempotencyRecord.newBuilder().setDigest(ByteString.copyFrom(digest))
                                .setResult(result).setExpiresAtMs(command.getExpiresAtMs()).build().toByteArray());
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
            if (releaseImport) ImportApplier.release(importStaging, command.getImportFile().getChecksum().toByteArray());
            updateLastAppliedTermIndex(term, index);
            return CompletableFuture.completedFuture(message(result.toByteArray()));
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(new IOException("分片状态机应用失败", ex));
        }
    }

    @Override public void notifyTermIndexUpdated(long term, long index) {
        dbLock.writeLock().lock();
        try {
            if (index <= getLastAppliedTermIndex().getIndex()) return;
            try {
                db.put(writes, APPLIED, position(term, index));
                updateLastAppliedTermIndex(term, index);
            } catch (RocksDBException ex) { throw new IllegalStateException("无法持久化 Raft 控制日志进度", ex); }
        } finally { dbLock.writeLock().unlock(); }
    }

    @Override public CompletableFuture<Message> query(Message request) {
        dbLock.readLock().lock();
        try { synchronized (scans) { return queryLocked(request); } }
        finally { dbLock.readLock().unlock(); }
    }
    private CompletableFuture<Message> queryLocked(Message request) {
        try {
            if (dropping || db == null) throw new IOException("图空间已删除");
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
        synchronized (snapshotLock) {
            final TermIndex position;
            dbLock.writeLock().lock();
            try {
                flushHeld();
                position = getLastAppliedTermIndex();
                if (position.getIndex() < 0) return -1;
                var current = storage.getLatestSnapshot();
                if (current != null && current.getIndex() == position.getIndex()) return position.getIndex();
                storage.link(db, position.getTerm(), position.getIndex());
            } catch (RocksDBException ex) {
                throw new IOException("无法创建 RocksDB Checkpoint", ex);
            } finally { dbLock.writeLock().unlock(); }
            storage.seal(position.getTerm(), position.getIndex(), snapshotBytesPerSecond);
            return position.getIndex();
        }
    }

    private void flushHeld() throws IOException {
        if (db == null || dropping) return;
        try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) { db.flush(flush); }
        catch (RocksDBException ex) { throw new IOException("无法把内存表刷入本地数据库", ex); }
    }

    private void pauseCompaction() throws IOException {
        if (compactionPaused || db == null) return;
        applyCompactionOptions(true);
        compactionPaused = true;
    }

    private void resumeCompaction() throws IOException {
        if (!compactionPaused || db == null) return;
        applyCompactionOptions(false);
        compactionPaused = false;
    }

    private void applyCompactionOptions(boolean importing) throws IOException {
        MutableColumnFamilyOptions changed = MutableColumnFamilyOptions.builder()
                .setDisableAutoCompactions(importing)
                .setLevel0SlowdownWritesTrigger(importing ? IMPORT_L0_SLOWDOWN : NORMAL_L0_SLOWDOWN)
                .setLevel0StopWritesTrigger(importing ? IMPORT_L0_STOP : NORMAL_L0_STOP)
                .build();
        try { db.setOptions(db.getDefaultColumnFamily(), changed); }
        catch (RocksDBException ex) { throw new IOException(importing ? "无法暂停自动压实" : "无法恢复自动压实", ex); }
    }

    /** 必须已持有分片写锁。压实在后台进行，这里只记账。 */
    private BatchResult ingestHeld(org.rocksdb.WriteBatch batch, io.cengyan.protocol.proto.ClusterProtocol.ImportDirective directive) throws IOException, RocksDBException {
        byte[] checksum = directive.getChecksum().toByteArray();
        long size = ImportApplier.stagedBytes(importStaging, checksum);
        boolean[] created = {false};
        BatchResult result = ImportApplier.ingest(db, batch, importStaging, directive, appendOnly(directive), created);
        if (created[0]) noteIngested(size);
        return result;
    }

    private boolean appendOnly(io.cengyan.protocol.proto.ClusterProtocol.ImportDirective directive) throws IOException, RocksDBException {
        if (directive.getUndo() || directive.getConflict() == io.cengyan.protocol.proto.ClusterProtocol.ImportConflict.IMPORT_REPLACE) return false;
        String session = ImportApplier.sessionOwner(importStaging, directive.getChecksum().toByteArray());
        if (session == null || !importSessions.contains(session)) return false;
        AppendDecision decision = appendSessions.get(session);
        if (decision != null) return decision.generation == directive.getGeneration() && decision.appendOnly;
        boolean empty = !generationHasData(directive.getGeneration());
        appendSessions.put(session, new AppendDecision(directive.getGeneration(), empty));
        return empty;
    }

    private boolean generationHasData(long generation) throws RocksDBException {
        for (byte kind : new byte[] {16, 17}) {
            byte[] prefix = ByteBuffer.allocate(10).put((byte) 2).put(kind).putLong(generation).array();
            try (RocksIterator iterator = db.newIterator()) {
                iterator.seek(prefix);
                if (iterator.isValid() && startsWith(iterator.key(), prefix)) return true;
            }
        }
        return false;
    }

    private void noteIngested(long bytes) {
        if (bytes <= 0) return;
        synchronized (compactState) {
            if (bytesSinceCompact == 0) uncompactedSinceMs = System.currentTimeMillis();
            bytesSinceCompact += bytes;
        }
    }

    /** 未压实字节达到一轮，或已经积满设定时间。调用方持有 {@link #compactState}。 */
    private boolean due(long now) {
        if (bytesSinceCompact <= 0) return false;
        if (bytesSinceCompact >= importCompactWaveBytes) return true;
        return uncompactedSinceMs > 0 && now - uncompactedSinceMs >= importCompactIdleMs;
    }

    void compactIfDue(long now) {
        synchronized (compactState) {
            if (due(now)) enqueue();
        }
    }

    /** 调用方持有 {@link #compactState}。 */
    private void enqueue() {
        if (closing || dropping || db == null || compactQueued) return;
        compactQueued = true;
        IMPORT_COMPACTOR.execute(this::runRound);
    }

    private void runRound() {
        boolean again = false;
        try { again = compactTop(); }
        finally {
            synchronized (compactState) {
                compactQueued = false;
                if (again && !closing && !dropping && db != null && due(System.currentTimeMillis())) enqueue();
            }
        }
    }

    /** @return 本轮有进展，调用方可以接着看是否还要再压 */
    private boolean compactTop() {
        compactExclusive.lock();
        try {
            if (closing || dropping || db == null || liveDirectory == null) return false;
            List<String> level0 = new ArrayList<>();
            long level0Bytes = 0;
            ColumnFamilyMetaData metadata = db.getColumnFamilyMetaData();
            for (LevelMetaData level : metadata.levels()) {
                if (level.level() != 0) continue;
                for (SstFileMetaData file : level.files()) {
                    if (file.beingCompacted()) continue;
                    level0.add(file.fileName());
                    level0Bytes += file.size();
                }
            }
            if (level0.isEmpty()) {
                synchronized (compactState) {
                    bytesSinceCompact = 0;
                    uncompactedSinceMs = 0;
                }
                LOG.info("新摄入的数据已经不在 L0，清零未压实计数");
                resumeWhenIdle();
                return false;
            }
            long free = Files.getFileStore(liveDirectory).getUsableSpace();
            if (free <= level0Bytes) {
                LOG.warning("空闲空间不足本轮 L0 压实。L0 " + level0Bytes + " 字节，空闲 " + free + " 字节");
                return false;
            }
            long accounted;
            synchronized (compactState) { accounted = bytesSinceCompact; }
            try (CompactionOptions options = new CompactionOptions(); CompactionJobInfo info = new CompactionJobInfo()) {
                db.compactFiles(options, level0, 1, -1, info);
            }
            synchronized (compactState) {
                bytesSinceCompact -= accounted;
                if (bytesSinceCompact < 0) bytesSinceCompact = 0;
                if (bytesSinceCompact == 0) uncompactedSinceMs = 0;
            }
            completedCompactions++;
            resumeWhenIdle();
            return true;
        } catch (Exception ex) {
            LOG.warning("导入压实没有完成：" + ex.getMessage());
            return false;
        } finally { compactExclusive.unlock(); }
    }

    private void resumeWhenIdle() {
        dbLock.readLock().lock();
        try { resumeCompactionIfSettled(); }
        finally { dbLock.readLock().unlock(); }
    }

    /** 调用方持有分片锁。没有导入会话且 L0 已经低于默认减速线时，恢复自动压实。 */
    private void resumeCompactionIfSettled() {
        try {
            if (!compactionPaused || db == null || !importSessions.isEmpty()) return;
            if (level0Count() > NORMAL_L0_SLOWDOWN) return;
            resumeCompaction();
        } catch (Exception ex) {
            LOG.warning("恢复自动压实没有完成：" + ex.getMessage());
        }
    }

    private int level0Count() throws RocksDBException {
        ColumnFamilyMetaData metadata = db.getColumnFamilyMetaData();
        for (LevelMetaData level : metadata.levels()) {
            if (level.level() == 0) return level.files().size();
        }
        return 0;
    }

    private void register() {
        LIVE.addIfAbsent(this);
        if (db == null) return;
        long level0Bytes = 0;
        ColumnFamilyMetaData metadata = db.getColumnFamilyMetaData();
        for (LevelMetaData level : metadata.levels()) {
            if (level.level() != 0) continue;
            level0Bytes = level.size();
        }
        if (level0Bytes <= 0) return;
        synchronized (compactState) {
            if (bytesSinceCompact < level0Bytes) bytesSinceCompact = level0Bytes;
            if (uncompactedSinceMs == 0) uncompactedSinceMs = System.currentTimeMillis();
        }
        compactIfDue(System.currentTimeMillis());
    }

    private static void compactDue() {
        long now = System.currentTimeMillis();
        for (RocksStateMachine machine : LIVE) {
            try { machine.compactIfDue(now); }
            catch (RuntimeException ex) { LOG.warning("导入压实检查失败：" + ex.getMessage()); }
        }
    }

    int completedCompactions() { return completedCompactions; }

    long uncompactedBytes() {
        synchronized (compactState) { return bytesSinceCompact; }
    }

    private static Options databaseOptions(boolean create) {
        var table = new BlockBasedTableConfig().setFilterPolicy(BLOOM).setBlockCache(BLOCK_CACHE).setCacheIndexAndFilterBlocks(true);
        return new Options().setCreateIfMissing(create).setWriteBufferSize(32L * 1024 * 1024).setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION).setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(table);
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
        LIVE.remove(this);
        if (db != null) for (String id : java.util.List.copyOf(scans.keySet())) releaseScan(id);
        if (db != null) { db.close(); db = null; }
        if (writes != null) { writes.close(); writes = null; }
        if (options != null) { options.close(); options = null; }
    }
    private void releaseScan(String id) {
        ScanCursor cursor = scans.remove(id);
        if (cursor != null && db != null) db.releaseSnapshot(cursor.snapshot());
    }
    @Override public void close() {
        dbLock.writeLock().lock();
        try { closeHeld(); }
        finally { dbLock.writeLock().unlock(); }
    }
    private void closeHeld() {
        closing = true;
        compactExclusive.lock();
        try {
            if (!rebuildOnly) {
                try { flushHeld(); }
                catch (IOException ex) { LOG.warning("关闭前刷盘失败：" + ex.getMessage()); }
            }
            synchronized (compactState) {
                bytesSinceCompact = 0;
                uncompactedSinceMs = 0;
            }
            closeDatabase();
        } finally { compactExclusive.unlock(); }
        if (rebuildOnly) return;
        if (getLifeCycle().getCurrentState() != org.apache.ratis.util.LifeCycle.State.CLOSED) {
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSING);
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSED);
        }
    }
}
