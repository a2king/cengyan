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
import io.cengyan.protocol.IndexFamilyName;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;
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
    /** 本副本上一个导入会话的暂存情况。 */
    public record ImportStatus(int runs, int mergedRuns, int coveredRuns, int partials, long bytes, long partialBytes,
                               List<String> prepared) { }
    static { RocksDB.loadLibrary(); }
    private static final byte[] APPLIED = {0};
    private static final byte[] CONTROLLER_EPOCH = {0, 1};
    private static final Logger LOG = Logger.getLogger(RocksStateMachine.class.getName());
    private final CheckpointStorage storage = new CheckpointStorage();
    private final Object snapshotLock = new Object();
    /** 读不挡住写，写不挡住其他分片。同一分片的 apply 仍然互斥。 */
    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private static final Filter BLOOM = new BloomFilter(10);
    /**
     * 压实输出在键类（物理键前两个字节：命名空间和点/边/邻接/点目录）变化处切开文件。新类型的编号总比已有类型大，
     * 它的键区间落在本键类末尾；没有文件跨过键类边界，新类型导入的文件就不和任何已有文件重叠，能直接放进最底层。
     */
    private static final SstPartitionerFactory KIND_PARTITIONER = new SstPartitionerFixedPrefixFactory(2);
    private Options options;
    private DBOptions dbOptions;
    /** 每个已打开 CF 的选项。RocksJava 要求选项对象活到数据库关闭之后。 */
    private final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
    /** 已打开的全部 Column Family（含 default）。只在持有 dbLock 写锁时修改。 */
    private final Map<String, ColumnFamilyHandle> families = new HashMap<>();
    /** 已通过 Raft 创建、可以读写的索引 Column Family。 */
    private final Map<String, ColumnFamilyHandle> activeIndexes = new HashMap<>();
    /** 已通过 Raft 删除的索引。读视为空，迟到的写入直接丢弃；物理 CF 可能还在等刷盘后删除。 */
    private final Set<String> droppedIndexes = new HashSet<>();
    private WriteOptions writes;
    private RocksDB db;
    private Path stateDirectory;
    private Path liveDirectory;
    /** 导入暂存区。上传、合并、归并都只碰这里的文件，不拿分片库的锁。 */
    private volatile ImportStaging imports;
    private boolean rebuildOnly;
    private record ScanCursor(Snapshot snapshot, ByteString query, long expiresAt, ByteString lastKey) { }
    /** 扫描游标表。只在登记、取用、归还游标时短暂加锁；翻页读取本身不持有这把锁。 */
    private final java.util.Map<String, ScanCursor> scans = new java.util.HashMap<>();
    /** 正在翻页的游标。过期清理不会释放它们的快照。 */
    private final Set<String> busyScans = new HashSet<>();
    private volatile Map<String, Long> lastUsage = Map.of();
    private final long snapshotBytesPerSecond;
    /** 图空间已经删除。新的导入和查询直接拒绝；正在进行的调用结束于关闭数据库之前。 */
    private volatile boolean dropping;
    private static final byte PHYSICAL_NAMESPACE = 2;
    /** 索引 CF 登记：[6][CF 名称] -> ACTIVE/DROPPED，与数据和 APPLIED 在同一个 WriteBatch 里提交。 */
    private static final byte INDEX_REGISTRY = 6;
    private static final byte[] REGISTRY_ACTIVE = {1};
    private static final byte[] REGISTRY_DROPPED = {2};
    /** indexId -> CF 名称。避免每次读写索引键都算一次 SHA-256。 */
    private static final Map<String, String> FAMILY_NAMES = new java.util.concurrent.ConcurrentHashMap<>();
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
            machine.imports = new ImportStaging(Files.createDirectories(directory.resolve("import-staging")));
            machine.openDatabase(directory, true);
            machine.writes = new WriteOptions().setDisableWAL(true).setSync(false);
            EntityCountStore.initialize(machine.db, machine.writes);
            return machine;
        } catch (RocksDBException | IOException ex) { machine.closeDatabase(); throw ex instanceof IOException io ? io : new IOException(ex); }
    }

    /** 重新打开已经存在的分片目录。调用方需要先确认目录属于本集群。 */
    public static RocksStateMachine reopenSealedReplica(Path directory) throws IOException {
        if (!Files.isDirectory(directory) || !Files.exists(directory.resolve("CURRENT"))) throw new IOException("封存的分片目录不存在：" + directory);
        var machine = new RocksStateMachine();
        machine.rebuildOnly = true;
        machine.liveDirectory = directory;
        machine.imports = new ImportStaging(Files.createDirectories(directory.resolve("import-staging")));
        try {
            machine.openDatabase(directory, false);
            machine.writes = new WriteOptions().setDisableWAL(true).setSync(false);
            EntityCountStore.initialize(machine.db, machine.writes);
            return machine;
        } catch (RocksDBException | IOException ex) { machine.closeDatabase(); throw ex instanceof IOException io ? io : new IOException(ex); }
    }

    /** 把上传段的一个分块写到本副本的暂存区。不拿分片库的锁，不进 Raft 日志。 */
    public void stageImport(io.cengyan.protocol.proto.ClusterProtocol.ImportChunk chunk) throws IOException {
        stageImport(chunk, System.currentTimeMillis());
    }

    void stageImport(io.cengyan.protocol.proto.ClusterProtocol.ImportChunk chunk, long now) throws IOException {
        ImportStaging staging = imports;
        if (dropping || staging == null) throw new IOException("图空间已删除");
        if (staging.stage(chunk, now)) staging.scheduleCompaction(chunk.getSessionId());
    }

    /**
     * 把这个会话的上传段归并成可以直接摄入的文件，返回行数等统计。发布前在每个副本上调用一次；
     * 没调用过的副本在 apply 发布时自己归并。
     */
    public io.cengyan.protocol.proto.ClusterProtocol.ImportPrepared prepareImport(io.cengyan.protocol.proto.ClusterProtocol.ImportPublish publish) throws IOException {
        ImportStaging staging = imports;
        if (dropping || staging == null) throw new IOException("图空间已删除");
        ImportStaging.Prepared prepared = staging.prepare(publish);
        if (!prepared.ok()) return prepared.proto();
        // 读锁下比对：apply 正在写的时候等一下，不影响查询。
        dbLock.readLock().lock();
        try {
            if (db == null) throw new IOException("分片库已关闭");
            return ImportPublisher.precheck(db, publish, prepared).proto();
        } catch (RocksDBException ex) {
            throw new IOException("导入预检失败", ex);
        } finally { dbLock.readLock().unlock(); }
    }

    /** 刷新导入会话心跳。没有心跳满 30 分钟后，会话的暂存文件会被删掉。 */
    public void heartbeatImport(String sessionId) throws IOException {
        ImportStaging staging = imports;
        if (dropping || staging == null) return;
        staging.heartbeat(sessionId, System.currentTimeMillis());
    }

    public ImportStatus importStatus(String sessionId) throws IOException {
        ImportStaging staging = imports;
        if (staging == null) throw new IOException("导入暂存目录尚未就绪");
        var status = staging.status(sessionId);
        return new ImportStatus(status.runs(), status.mergedRuns(), status.coveredRuns(), status.partials(), status.bytes(),
                status.partialBytes(), status.prepared());
    }

    /**
     * 导入会话结束，删掉它在本副本上的暂存文件。
     * @param abort 客户端在发布之前就放弃了：没有发布指令会用到归并结果，一起删掉
     * @return 会话的文件已经全部删掉
     */
    public boolean finishImport(String sessionId, boolean abort) throws IOException {
        ImportStaging staging = imports;
        return staging != null && staging.close(sessionId, abort);
    }

    /** 计划内重启前把内存表刷进 SST，使已应用位置和工作库一起落盘。 */
    public void flushForRestart() throws IOException {
        dbLock.writeLock().lock();
        try { flushHeld(); }
        finally { dbLock.writeLock().unlock(); }
    }

    /** @return 本次删掉的会话数 */
    public int expireImports(long now) throws IOException {
        ImportStaging staging = imports;
        return staging == null ? 0 : staging.expire(now);
    }

    /** 拒绝这张图上新的导入和查询。已进入的调用在关闭数据库时排空，避免文件句柄把空间留住。 */
    void beginDrop() { dropping = true; }

    /** 测试用：不经过 Raft，在本分片上走 apply 的同一条发布路径。 */
    BatchResult publishLocal(io.cengyan.protocol.proto.ClusterProtocol.ImportPublish publish) throws IOException {
        var command = io.cengyan.protocol.proto.ClusterProtocol.WriteBatch.newBuilder().setRequestId("local-import/" + UUID.randomUUID())
                .setExpiresAtMs(System.currentTimeMillis() + 3_600_000L).setImportPublish(publish).build();
        try {
            return BatchResult.parseFrom(applyPublish(command, getLastAppliedTermIndex().getIndex() + 1, 1).join().getContent().toByteArray());
        } catch (java.util.concurrent.CompletionException ex) {
            throw new IOException("导入发布失败", ex.getCause());
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
        flushAll();
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
        imports = new ImportStaging(Files.createDirectories(stateDirectory.resolve("import-staging")));
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }

    @Override public synchronized void pause() {
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSING);
        dbLock.writeLock().lock();
        try {
            try { flushHeld(); }
            catch (IOException ex) { LOG.warning("暂停前刷盘失败：" + ex.getMessage()); }
            closeDatabase();
        } finally { dbLock.writeLock().unlock(); }
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.PAUSED);
    }
    @Override public synchronized void reinitialize() throws IOException {
        restore();
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);
        getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);
    }
    @Override public StateMachineStorage getStateMachineStorage() { return storage; }

    private void restore() throws IOException {
        dbLock.writeLock().lock();
        try { closeDatabase(); }
        finally { dbLock.writeLock().unlock(); }
        Files.createDirectories(stateDirectory);
        imports = new ImportStaging(Files.createDirectories(stateDirectory.resolve("import-staging")));
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
            openDatabase(dbDir, create);
            // Raft 日志已经 fsync。内存表没刷盘的部分由日志重放补上，不再为每次写入打开 RocksDB WAL。
            writes = new WriteOptions().setDisableWAL(true).setSync(false);
            liveDirectory = dbDir;
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
        var entry = trx.getLogEntry();
        io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command;
        try { command = io.cengyan.protocol.proto.ClusterProtocol.WriteBatch.parseFrom(entry.getStateMachineLogEntry().getLogData().toByteArray()); }
        catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
        // 导入发布自己管锁：比对和重写在读锁下做，查询不用等它。
        if (command.hasImportPublish()) return applyPublish(command, entry.getIndex(), entry.getTerm());
        dbLock.writeLock().lock();
        try { return applyCommand(command, entry.getIndex(), entry.getTerm()); }
        finally { dbLock.writeLock().unlock(); }
    }

    private CompletableFuture<Message> applyCommand(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command, long index, long term) {
        try {
            if (index <= getLastAppliedTermIndex().getIndex()) throw new IOException("Raft 日志重复或倒序应用");
            BatchResult result;
            byte[] key = namespace((byte) 1, command.getRequestId().getBytes(StandardCharsets.UTF_8));
            byte[] digest = digest(command.toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteArray());
            IndexChanges indexChanges = new IndexChanges();
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                byte[] currentEpochBytes = db.get(CONTROLLER_EPOCH);
                long currentEpoch = currentEpochBytes == null ? 0 : ByteBuffer.wrap(currentEpochBytes).getLong();
                boolean indexing = command.getCreateIndexPrefixesCount() > 0 || command.getDropIndexPrefixesCount() > 0;
                if (command.getControllerEpoch() != 0 && command.getControllerEpoch() < currentEpoch) {
                    result = failure("STALE_CONTROLLER", "控制命令来自旧 Metadata Leader 任期");
                } else if (command.getRequestId().isBlank() || command.getRequestId().length() > 256
                        || (command.getMutationsCount() == 0 && command.getPurgeRequestIdsCount() == 0 && !indexing)
                        || command.getExpiresAtMs() <= 0
                        || (command.getPurgeRequestIdsCount() > 0 && command.getPurgeBeforeMs() <= 0)) {
                    result = failure("INVALID_ARGUMENT", "写入批次标识、内容或有效期无效");
                } else {
                    byte[] previous = db.get(key);
                    if (previous != null) {
                        var record = IdempotencyRecord.parseFrom(previous);
                        result = MessageDigest.isEqual(digest, record.getDigest().toByteArray()) ? record.getResult()
                                : failure("REQUEST_CONFLICT", "相同 requestId 对应不同写入内容；requestId=" + command.getRequestId());
                    } else if (indexing) {
                        if (command.getMutationsCount() > 0 || command.getConditionsCount() > 0 || command.getPurgeRequestIdsCount() > 0) {
                            result = failure("INVALID_ARGUMENT", "索引 Column Family 指令不能混入普通写入");
                        } else {
                            result = planIndexChanges(batch, command, indexChanges);
                        }
                        batch.put(key, IdempotencyRecord.newBuilder().setDigest(ByteString.copyFrom(digest))
                                .setResult(result).setExpiresAtMs(command.getExpiresAtMs()).build().toByteArray());
                    } else {
                        boolean matches = true;
                        for (var condition : command.getConditionsList()) {
                            byte[] current = getLogical(condition.getKey().toByteArray());
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
                                Target target = target(op.getKey().toByteArray());
                                // 索引已删除：旧 Schema 下迟到的写入直接丢弃。各副本在同一日志位置看到同一份登记，结果一致。
                                if (target.dropped()) continue;
                                if (op.getDelete()) {
                                    if (target.family() == null) batch.delete(target.key()); else batch.delete(target.family(), target.key());
                                } else if (target.family() == null) {
                                    batch.put(target.key(), op.getValue().toByteArray());
                                } else {
                                    batch.put(target.family(), target.key(), op.getValue().toByteArray());
                                }
                                if (op.getKey().size() > 9 && (op.getKey().byteAt(0) == 16 || op.getKey().byteAt(0) == 17)) {
                                    finalValues.put(ByteString.copyFrom(target.key()), op.getDelete() ? null : op.getValue().toByteArray());
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
            publishIndexChanges(indexChanges);
            updateLastAppliedTermIndex(term, index);
            return CompletableFuture.completedFuture(message(result.toByteArray()));
        } catch (Exception ex) {
            LOG.log(java.util.logging.Level.SEVERE, "分片状态机应用失败，requestId=" + command.getRequestId(), ex);
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
        try { return queryLocked(request); }
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
                    byte[] value = getLogical(requested.toByteArray());
                    var found = StoredValue.newBuilder().setFound(value != null);
                    if (value != null) { bytes += value.length; found.setValue(ByteString.copyFrom(value)); }
                    if (bytes > 7 * 1024 * 1024) throw new IOException("批量点读结果超过字节上限");
                    values.addValues(found);
                }
                return CompletableFuture.completedFuture(message(values.build().toByteArray()));
            }
            long now = System.nanoTime();
            if (key.getCloseCursor()) {
                sweepScans(now);
                synchronized (scans) { if (!busyScans.contains(key.getCursorId())) releaseScan(key.getCursorId()); }
                return CompletableFuture.completedFuture(message(ScanPage.getDefaultInstance().toByteArray()));
            }
            if (key.getScan()) {
                sweepScans(now);
                if (key.getLimit() < 1 || key.getLimit() > 10000 || key.getMaxBytes() < 1024 || key.getMaxBytes() > 7 * 1024 * 1024) {
                    throw new IOException("扫描批次行数或字节限制无效");
                }
                byte namespace = key.getScanIdempotency() ? (byte) 1 : PHYSICAL_NAMESPACE;
                Target target = key.getScanIdempotency() ? new Target(null, namespace((byte) 1, key.getKey().toByteArray()), false)
                        : target(key.getKey().toByteArray());
                if (target.dropped()) {
                    // 索引已删除：按空结果返回，续扫的游标一并放掉。
                    if (!key.getCursorId().isEmpty()) synchronized (scans) { if (!busyScans.contains(key.getCursorId())) releaseScan(key.getCursorId()); }
                    return CompletableFuture.completedFuture(message(ScanPage.getDefaultInstance().toByteArray()));
                }
                ColumnFamilyHandle family = target.family();
                byte[] prefix = target.key();
                byte[] after = key.getAfterKey().isEmpty() ? prefix : family == null ? namespace(namespace, key.getAfterKey().toByteArray()) : key.getAfterKey().toByteArray();
                String cursorId = key.getCursorId();
                ByteString queryIdentity = key.toBuilder().clearAfterKey().clearCursorId().clearScanId().clearLimit().clearMaxBytes().build().toByteString();
                ScanCursor cursor;
                if (cursorId.isEmpty()) {
                    if (!key.getAfterKey().isEmpty()) throw new IOException("续扫必须携带有效快照游标");
                    cursorId = key.getScanId().isBlank() ? UUID.randomUUID().toString() : key.getScanId();
                    cursor = new ScanCursor(db.getSnapshot(), queryIdentity, now + java.util.concurrent.TimeUnit.SECONDS.toNanos(30), ByteString.EMPTY);
                    openScan(cursorId, cursor);
                } else {
                    cursor = acquireScan(cursorId);
                    if (cursor == null) throw new IOException("分片扫描游标已失效、查询改变或顺序不匹配");
                    if (!cursor.query().equals(queryIdentity) || !cursor.lastKey().equals(key.getAfterKey())) {
                        finishScan(cursorId, cursor);
                        throw new IOException("分片扫描游标已失效、查询改变或顺序不匹配");
                    }
                }
                var page = ScanPage.newBuilder();
                int bytes = 0;
                ScanCursor next = null;
                try (ReadOptions options = new ReadOptions().setSnapshot(cursor.snapshot()); RocksIterator iterator = family == null ? db.newIterator(options) : db.newIterator(family, options)) {
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
                        ByteString logicalKey = family == null ? ByteString.copyFrom(dataKey, 1, dataKey.length - 1) : ByteString.copyFrom(dataKey);
                        page.addEntries(KeyValue.newBuilder().setKey(logicalKey).setValue(ByteString.copyFrom(value)));
                        page.setLastKey(logicalKey);
                        bytes += cost;
                        iterator.next();
                    }
                    iterator.status();
                    if (page.getMore()) {
                        page.setCursorId(cursorId);
                        next = new ScanCursor(cursor.snapshot(), cursor.query(), now + java.util.concurrent.TimeUnit.SECONDS.toNanos(30), page.getLastKey());
                    }
                } finally {
                    // next 为空：扫描结束或中途失败，快照随游标一起释放。
                    finishScan(cursorId, next);
                }
                return CompletableFuture.completedFuture(message(page.build().toByteArray()));
            }
            byte[] value = getLogical(key.getKey().toByteArray());
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
                // Data Group 被删除/隔离后，Ratis 可能仍触发一次已排队的快照回调。
                // 此时数据库句柄已关闭，不能再调用 RocksDB Checkpoint。
                if (db == null || dropping) return -1;
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
        flushAll();
    }

    /** 关掉 WAL 后必须把所有 CF 一起刷盘；DBOptions 打开了原子刷盘，APPLIED 与索引数据在同一点落盘。 */
    private void flushAll() throws IOException {
        try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) { db.flush(flush, new ArrayList<>(families.values())); }
        catch (RocksDBException ex) { throw new IOException("无法把内存表刷入本地数据库", ex); }
    }

    /**
     * 导入发布。比对已有数据、必要时重写文件都在读锁下做，查询照常进行；只有摄入和写批次持写锁。
     * apply 线程是分片库唯一的逻辑写入者，读锁期间看到的状态在拿到写锁之前不会变。
     *
     * <p>本副本缺少上传段时这里会失败，而别的副本已经摄入了数据：这个副本从此与其他副本不一致，
     * 只能删掉分片目录后从 Leader 重新同步。Compute 在发布前要求每个副本都收齐、归并成功，正常不会走到这里。
     */
    private CompletableFuture<Message> applyPublish(io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command, long index, long term) {
        var publish = command.getImportPublish();
        byte[] key = namespace((byte) 1, command.getRequestId().getBytes(StandardCharsets.UTF_8));
        byte[] digest = digest(command.toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteArray());
        ImportPublisher.Plan plan = null;
        BatchResult result;
        boolean record = false;
        long currentEpoch = 0;
        try {
            dbLock.readLock().lock();
            try {
                if (db == null) throw new IOException("分片库已关闭");
                if (index <= getLastAppliedTermIndex().getIndex()) throw new IOException("Raft 日志重复或倒序应用");
                byte[] epochBytes = db.get(CONTROLLER_EPOCH);
                currentEpoch = epochBytes == null ? 0 : ByteBuffer.wrap(epochBytes).getLong();
                if (command.getControllerEpoch() != 0 && command.getControllerEpoch() < currentEpoch) {
                    result = failure("STALE_CONTROLLER", "控制命令来自旧 Metadata Leader 任期");
                } else if (command.getRequestId().isBlank() || command.getRequestId().length() > 256 || command.getExpiresAtMs() <= 0
                        || command.getMutationsCount() > 0 || command.getConditionsCount() > 0 || command.getPurgeRequestIdsCount() > 0
                        || command.getCreateIndexPrefixesCount() > 0 || command.getDropIndexPrefixesCount() > 0) {
                    result = failure("INVALID_ARGUMENT", "导入发布不能混入普通写入");
                } else {
                    byte[] previous = db.get(key);
                    if (previous != null) {
                        var saved = IdempotencyRecord.parseFrom(previous);
                        result = MessageDigest.isEqual(digest, saved.getDigest().toByteArray()) ? saved.getResult()
                                : failure("REQUEST_CONFLICT", "相同 requestId 对应不同写入内容；requestId=" + command.getRequestId());
                    } else {
                        record = true;
                        if (dropping) result = failure("GRAPH_STATE", "图空间已删除");
                        else {
                            plan = ImportPublisher.plan(db, imports, publish);
                            result = plan.result();
                        }
                    }
                }
            } finally { dbLock.readLock().unlock(); }
            dbLock.writeLock().lock();
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                if (db == null) throw new IOException("分片库已关闭");
                if (plan != null && !plan.ingest().isEmpty()) {
                    // WAL 关闭：内存表里还没落盘的写入，崩溃后由 Raft 重放补回，那时它们的序号会排到摄入的文件之后，
                    // 改变同一个键的新旧顺序。先刷盘，摄入之前的状态就全部在 SST 里了。
                    flushAll();
                    try (IngestExternalFileOptions options = new IngestExternalFileOptions().setMoveFiles(true)) {
                        db.ingestExternalFile(plan.ingest().stream().map(Path::toString).toList(), options);
                    }
                }
                if (plan != null && plan.ok() && !plan.done()) {
                    EntityCountStore.add(db, batch, publish.getNode(), publish.getGeneration(), publish.getTypeName(), plan.inserted());
                    batch.put(plan.markerKey(), plan.doneMarker());
                }
                if (record) batch.put(key, IdempotencyRecord.newBuilder().setDigest(ByteString.copyFrom(digest))
                        .setResult(result).setExpiresAtMs(command.getExpiresAtMs()).build().toByteArray());
                if (command.getControllerEpoch() > currentEpoch) batch.put(CONTROLLER_EPOCH, ByteBuffer.allocate(8).putLong(command.getControllerEpoch()).array());
                batch.put(APPLIED, position(term, index));
                db.write(writes, batch);
                updateLastAppliedTermIndex(term, index);
            } finally { dbLock.writeLock().unlock(); }
        } catch (Exception ex) {
            LOG.log(java.util.logging.Level.SEVERE, "导入发布应用失败；如果其他副本已经成功，本副本缺少这批数据，需要删除分片目录后从 Leader 重新同步。requestId="
                    + command.getRequestId(), ex);
            return CompletableFuture.failedFuture(new IOException("导入发布应用失败", ex));
        }
        if (plan != null && plan.ok()) {
            try { imports.released(publish); }
            catch (IOException ex) { LOG.warning("导入发布后清理暂存文件失败，会话结束时再删：" + ex.getMessage()); }
        }
        return CompletableFuture.completedFuture(message(result.toByteArray()));
    }

    /** 当前分片各 Column Family 已落盘 SST 的大小，不含内存表。 */
    public Map<String, Long> physicalUsage() {
        // 监控轮询不排在长时间的 apply 后面：拿不到锁就返回上一次的结果。
        if (!dbLock.readLock().tryLock()) return lastUsage;
        try {
            if (db == null) return lastUsage;
            Map<String, Long> result = new java.util.TreeMap<>();
            for (var entry : families.entrySet()) {
                long bytes = 0;
                try { bytes = db.getColumnFamilyMetaData(entry.getValue()).size(); }
                catch (RuntimeException ignored) { }
                result.put(entry.getKey(), bytes);
            }
            lastUsage = Map.copyOf(result);
            return lastUsage;
        } finally { dbLock.readLock().unlock(); }
    }

    private void openDatabase(Path directory, boolean create) throws RocksDBException, IOException {
        options = databaseOptions(create);
        // 关掉 WAL 后，各 CF 各自刷盘会让 APPLIED（默认 CF）先于索引 CF 落盘：崩溃后从更晚的位置重放，
        // 索引 CF 会丢掉中间的写入。原子刷盘让所有 CF 在同一个点一起落盘。
        dbOptions = new DBOptions(options).setCreateIfMissing(create).setAtomicFlush(true);
        List<byte[]> names = Files.exists(directory.resolve("CURRENT"))
                ? RocksDB.listColumnFamilies(options, directory.toString()) : List.of(RocksDB.DEFAULT_COLUMN_FAMILY);
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        for (byte[] name : names) {
            String text = new String(name, StandardCharsets.UTF_8);
            boolean index = isIndexFamily(text);
            if (!index && !Arrays.equals(name, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                throw new IOException("分片库含有本版本不认识的 Column Family：" + text + "。请删除这个分片目录，让副本从 Leader 重新同步");
            }
            ColumnFamilyOptions family = index ? indexOptions() : new ColumnFamilyOptions(options);
            familyOptions.add(family);
            descriptors.add(new ColumnFamilyDescriptor(name, family));
        }
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        db = RocksDB.open(dbOptions, directory.toString(), descriptors, handles);
        for (int i = 0; i < handles.size(); i++) families.put(new String(names.get(i), StandardCharsets.UTF_8), handles.get(i));
        loadIndexRegistry();
    }

    /**
     * 打开时库里没有内存表（WAL 关闭），读到的登记都已落盘。DROPPED 但物理 CF 还在，说明上次删除停在刷盘之后，
     * 这里补做 Drop。登记为 ACTIVE 的 CF 必须存在；没有登记的 CF 是创建命令写入前崩溃留下的，等日志重放时再登记。
     */
    private void loadIndexRegistry() throws RocksDBException, IOException {
        activeIndexes.clear();
        droppedIndexes.clear();
        Map<String, Boolean> registry = new HashMap<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(new byte[] {INDEX_REGISTRY}); iterator.isValid() && iterator.key()[0] == INDEX_REGISTRY; iterator.next()) {
                String name = new String(iterator.key(), 1, iterator.key().length - 1, StandardCharsets.UTF_8);
                registry.put(name, Arrays.equals(iterator.value(), REGISTRY_DROPPED));
            }
            iterator.status();
        }
        for (var entry : registry.entrySet()) {
            String name = entry.getKey();
            ColumnFamilyHandle handle = families.get(name);
            if (entry.getValue()) {
                droppedIndexes.add(name);
                if (handle != null) dropPhysically(name);
            } else if (handle == null) {
                throw new IOException("索引 Column Family 已登记但不存在：" + name);
            } else activeIndexes.put(name, handle);
        }
    }

    private ColumnFamilyOptions indexOptions() {
        return new ColumnFamilyOptions(options).setWriteBufferSize(16L * 1024 * 1024).setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION).setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableConfig());
    }

    /** 逻辑键的物理位置。family 为 null 表示默认 CF 的 namespace 2；dropped 表示索引已删除，读为空、写丢弃。 */
    private record Target(ColumnFamilyHandle family, byte[] key, boolean dropped) { }

    /**
     * 索引键：已登记 ACTIVE 的写到它自己的 CF；已登记 DROPPED 的视为不存在；没有登记的是升级前的旧索引，
     * 仍在默认 CF。其他键全部在默认 CF。调用方持有 dbLock。
     */
    private Target target(byte[] logicalKey) {
        String family = indexFamily(logicalKey);
        if (family != null) {
            if (droppedIndexes.contains(family)) return new Target(null, null, true);
            ColumnFamilyHandle handle = activeIndexes.get(family);
            if (handle != null) return new Target(handle, logicalKey, false);
        }
        return new Target(null, namespace(PHYSICAL_NAMESPACE, logicalKey), false);
    }

    private static String indexFamily(byte[] logicalKey) {
        String identity = IndexFamilyName.identity(logicalKey);
        if (identity == null) return null;
        if (FAMILY_NAMES.size() > 100_000) FAMILY_NAMES.clear();
        return FAMILY_NAMES.computeIfAbsent(identity, IndexFamilyName::of);
    }

    private byte[] getLogical(byte[] logicalKey) throws RocksDBException {
        Target target = target(logicalKey);
        if (target.dropped()) return null;
        return target.family() == null ? db.get(target.key()) : db.get(target.family(), target.key());
    }

    private static boolean isIndexFamily(String name) { return name.matches("idx-[0-9a-f]{64}"); }

    private static byte[] registryKey(String family) { return namespace(INDEX_REGISTRY, family.getBytes(StandardCharsets.UTF_8)); }

    /** 本条日志要创建/删除的索引 CF。batch 写入成功后才改内存状态。 */
    private static final class IndexChanges {
        final Map<String, ColumnFamilyHandle> created = new java.util.LinkedHashMap<>();
        final Set<String> dropped = new java.util.LinkedHashSet<>();
    }

    /**
     * 创建：先建物理 CF，再把 ACTIVE 登记放进本条 batch。batch 没落盘就崩溃时，留下一个没有登记的空 CF，
     * 日志重放会再次登记它。已经 DROPPED 的索引不会重新创建。
     * 删除：只写 DROPPED 登记，外加一条覆盖升级前旧索引键的 DeleteRange；Drop CF 在 batch 写入后做。
     */
    private BatchResult planIndexChanges(org.rocksdb.WriteBatch batch, io.cengyan.protocol.proto.ClusterProtocol.WriteBatch command,
                                         IndexChanges changes) throws RocksDBException {
        List<ByteString> all = new ArrayList<>(command.getCreateIndexPrefixesList());
        all.addAll(command.getDropIndexPrefixesList());
        for (ByteString prefix : all) {
            if (!IndexFamilyName.isIndexPrefix(prefix.toByteArray())) return failure("INVALID_ARGUMENT", "索引前缀无效");
        }
        long affected = 0;
        for (ByteString encoded : command.getCreateIndexPrefixesList()) {
            String name = indexFamily(encoded.toByteArray());
            if (droppedIndexes.contains(name) || activeIndexes.containsKey(name) || changes.created.containsKey(name)) continue;
            ColumnFamilyHandle handle = families.get(name);
            if (handle == null) {
                ColumnFamilyOptions family = indexOptions();
                familyOptions.add(family);
                handle = db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), family));
                families.put(name, handle);
            }
            batch.put(registryKey(name), REGISTRY_ACTIVE);
            changes.created.put(name, handle);
            affected++;
        }
        for (ByteString encoded : command.getDropIndexPrefixesList()) {
            byte[] prefix = encoded.toByteArray();
            String name = indexFamily(prefix);
            if (droppedIndexes.contains(name) || !changes.dropped.add(name)) continue;
            batch.put(registryKey(name), REGISTRY_DROPPED);
            // 升级前建的索引没有自己的 CF，键在默认 CF：一条范围删除代替逐键删除，空间由后台压实回收。
            batch.deleteRange(namespace(PHYSICAL_NAMESPACE, prefix), namespace(PHYSICAL_NAMESPACE, successor(prefix)));
            affected++;
        }
        return BatchResult.newBuilder().setCode("OK").setAffected(affected).build();
    }

    /** batch 已经写入：更新内存登记，再物理删除 CF。调用方持有 dbLock 写锁。 */
    private void publishIndexChanges(IndexChanges changes) {
        activeIndexes.putAll(changes.created);
        List<String> physical = new ArrayList<>();
        for (String name : changes.dropped) {
            activeIndexes.remove(name);
            droppedIndexes.add(name);
            if (families.containsKey(name)) physical.add(name);
        }
        if (physical.isEmpty() || dropping) return;
        try {
            // 先让 DROPPED 登记和 APPLIED 一起落盘，再 Drop CF。否则崩溃后会从更早的位置重放，
            // 重放中的条件读读不到这个 CF，得出与第一次不同的结果，副本就分叉了。
            flushAll();
            for (String name : physical) dropPhysically(name);
        } catch (IOException ex) {
            LOG.warning("索引 Column Family 的物理删除推迟到下次打开分片：" + ex.getMessage());
        }
    }

    /** Drop CF 只改 MANIFEST，SST 在句柄释放后由 RocksDB 直接 unlink，不逐键读写。 */
    private void dropPhysically(String name) {
        ColumnFamilyHandle handle = families.get(name);
        if (handle == null) return;
        try {
            db.dropColumnFamily(handle);
            families.remove(name);
            handle.close();
        } catch (RocksDBException ex) {
            LOG.warning("删除索引 Column Family 失败，下次打开分片时重试：" + name + "，" + ex.getMessage());
        }
    }

    /** 字典序上紧跟在所有以 prefix 开头的键之后的第一个键。 */
    private static byte[] successor(byte[] prefix) {
        byte[] end = prefix.clone();
        for (int i = end.length - 1; i >= 0; i--) {
            if (end[i] != (byte) 0xFF) {
                end[i]++;
                return Arrays.copyOf(end, i + 1);
            }
        }
        throw new IllegalArgumentException("前缀没有后继键");
    }

    /**
     * 索引块和过滤器按 4KB 分区存放，只有顶层常驻块缓存。SST 再大，一次点查也只读命中的那个分区，
     * 不会把整份过滤器或整份索引装进缓存再挤出去。
     */
    static BlockBasedTableConfig tableConfig() {
        return new BlockBasedTableConfig().setFilterPolicy(BLOOM).setBlockCache(RocksTuning.shared().cache())
                .setCacheIndexAndFilterBlocks(true).setCacheIndexAndFilterBlocksWithHighPriority(true)
                .setIndexType(IndexType.kTwoLevelIndexSearch).setPartitionFilters(true)
                .setPinTopLevelIndexAndFilter(true).setPinL0FilterAndIndexBlocksInCache(true)
                .setMetadataBlockSize(4096).setOptimizeFiltersForMemory(true);
    }

    /** 块缓存、内存表总额、后台线程和写入限速由本进程全部分片库共用，见 {@link RocksTuning}。 */
    private static Options databaseOptions(boolean create) {
        RocksTuning.Shared shared = RocksTuning.shared();
        RocksTuning tuning = RocksTuning.current();
        Options options = new Options().setCreateIfMissing(create).setWriteBufferSize(32L * 1024 * 1024).setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION).setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableConfig())
                .setMaxBackgroundJobs(tuning.backgroundJobs()).setMaxSubcompactions(tuning.subcompactions())
                // 压实和刷盘每写 1MiB 就让内核开始回写，避免攒出一次几百 MiB 的回写把同盘的 Raft 日志 fsync 堵住。
                .setBytesPerSync(1024 * 1024).setWriteBufferManager(shared.buffers())
                .setSstPartitionerFactory(KIND_PARTITIONER);
        if (shared.limiter() != null) options.setRateLimiter(shared.limiter());
        return options;
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
        if (db != null) synchronized (scans) { for (String id : java.util.List.copyOf(scans.keySet())) releaseScan(id); }
        if (db != null) {
            for (ColumnFamilyHandle handle : new ArrayList<>(families.values())) handle.close();
            families.clear();
            activeIndexes.clear();
            droppedIndexes.clear();
            db.close(); db = null;
        }
        if (writes != null) { writes.close(); writes = null; }
        if (dbOptions != null) { dbOptions.close(); dbOptions = null; }
        for (ColumnFamilyOptions family : familyOptions) family.close();
        familyOptions.clear();
        if (options != null) { options.close(); options = null; }
    }
    /** 调用方持有 {@code scans} 的锁，或者已经独占数据库（关闭时）。 */
    private void releaseScan(String id) {
        ScanCursor cursor = scans.remove(id);
        busyScans.remove(id);
        if (cursor != null && db != null) db.releaseSnapshot(cursor.snapshot());
    }

    private void openScan(String id, ScanCursor cursor) throws IOException {
        synchronized (scans) {
            String problem = scans.size() >= 128 ? "分片快照游标数量已达上限"
                    : scans.containsKey(id) ? "扫描请求标识已存在，禁止从头重放已开始扫描" : null;
            if (problem != null) {
                db.releaseSnapshot(cursor.snapshot());
                throw new IOException(problem);
            }
            scans.put(id, cursor);
            busyScans.add(id);
        }
    }

    /** @return 游标不存在时为 null。同一个游标同一时刻只能有一个请求在翻页。 */
    private ScanCursor acquireScan(String id) throws IOException {
        synchronized (scans) {
            ScanCursor cursor = scans.get(id);
            if (cursor != null && !busyScans.add(id)) throw new IOException("分片扫描游标正被另一个请求使用");
            return cursor;
        }
    }

    /** 归还游标。{@code next} 为 null 时扫描已经结束或失败，释放快照。 */
    private void finishScan(String id, ScanCursor next) {
        synchronized (scans) {
            busyScans.remove(id);
            if (next == null) releaseScan(id);
            else scans.put(id, next);
        }
    }

    private void sweepScans(long now) {
        synchronized (scans) {
            var expired = scans.entrySet().stream()
                    .filter(entry -> now - entry.getValue().expiresAt() >= 0 && !busyScans.contains(entry.getKey()))
                    .map(java.util.Map.Entry::getKey).toList();
            for (String id : expired) releaseScan(id);
        }
    }
    @Override public void close() {
        dbLock.writeLock().lock();
        try { closeHeld(); }
        finally { dbLock.writeLock().unlock(); }
    }
    private void closeHeld() {
        if (!rebuildOnly) {
            try { flushHeld(); }
            catch (IOException ex) { LOG.warning("关闭前刷盘失败：" + ex.getMessage()); }
        }
        closeDatabase();
        if (rebuildOnly) return;
        if (getLifeCycle().getCurrentState() != org.apache.ratis.util.LifeCycle.State.CLOSED) {
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSING);
            getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.CLOSED);
        }
    }
}
