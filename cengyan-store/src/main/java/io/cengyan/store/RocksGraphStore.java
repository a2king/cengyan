package io.cengyan.store;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * 嵌入式 RocksDB。整库打开时七个列族一次建好；按图拆开时只建这个目录要用的列族。
 * WAL 保持打开，写在这个库自己的目录里。边列族使用 17 字节前缀。
 */
public final class RocksGraphStore implements GraphStore {
    private static final Logger log = LoggerFactory.getLogger(RocksGraphStore.class);

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyOptions> cfOptions;
    private final List<ColumnFamilyHandle> handles;
    private final Map<Cf, ColumnFamilyHandle> byFamily;
    private final Cache blockCache;
    private final boolean ownsCache;
    private final BloomFilter edgeBloom;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private final AtomicLong propReads = new AtomicLong();
    private final AtomicLong edgesExamined = new AtomicLong();
    private final AtomicLong indexReads = new AtomicLong();
    private final AtomicLong verticesExamined = new AtomicLong();
    private volatile Runnable beforeWrite;
    private volatile WriteGate writeGate;
    private volatile BiFunction<Integer, Long, Integer> partResolver = (graphId, vid) -> 0;
    private static final byte[] EMPTY = new byte[0];

    private RocksGraphStore(
            Path directory,
            RocksDB db,
            DBOptions dbOptions,
            List<ColumnFamilyOptions> cfOptions,
            List<ColumnFamilyHandle> handles,
            Map<Cf, ColumnFamilyHandle> byFamily,
            Cache blockCache,
            boolean ownsCache,
            BloomFilter edgeBloom) {
        this.directory = directory;
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.handles = handles;
        this.byFamily = byFamily;
        this.blockCache = blockCache;
        this.ownsCache = ownsCache;
        this.edgeBloom = edgeBloom;
    }

    public static RocksGraphStore open(Path directory, StoreOptions options) {
        return open(directory, options, Set.of(Cf.values()), null);
    }

    /**
     * @param sharedCache 多个库共用的块缓存。为空时这个库自己创建并在关闭时释放。
     */
    public static RocksGraphStore open(Path directory, StoreOptions options, Set<Cf> families, Cache sharedCache) {
        if (directory == null) {
            throw new IllegalArgumentException("数据目录不能为空");
        }
        if (options == null) {
            throw new IllegalArgumentException("打开参数不能为空");
        }
        if (families == null || families.isEmpty()) {
            throw new IllegalArgumentException("列族不能为空");
        }
        Path path = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(path);
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法创建数据目录：" + ex.getMessage(), ex);
        }

        boolean ownsCache = sharedCache == null;
        Cache cache = null;
        BloomFilter bloom = null;
        DBOptions dbOptions = null;
        List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = null;
        try {
            cache = ownsCache ? new LRUCache(options.blockCacheBytes()) : sharedCache;
            List<Cf> ordered = orderedFamilies(families);
            if (ordered.contains(Cf.EDGE)) {
                bloom = new BloomFilter(10);
            }
            dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true);
            dbOptions.setCreateMissingColumnFamilies(true);
            dbOptions.setMaxBackgroundJobs(options.maxBackgroundJobs());
            // 预写日志、运行日志、刷盘和压缩产生的文件都留在这个库自己的目录。
            String location = path.toString();
            dbOptions.setWalDir(location);
            dbOptions.setDbLogDir(location);

            boolean slimDefault = ordered.size() < Cf.values().length;
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (Cf cf : ordered) {
                ColumnFamilyOptions one = columnFamilyOptions(cf, options, cache, bloom, slimDefault);
                cfOptions.add(one);
                descriptors.add(new ColumnFamilyDescriptor(cf.nameBytes(), one));
            }
            db = RocksDB.open(dbOptions, path.toString(), descriptors, handles);
            Map<Cf, ColumnFamilyHandle> byFamily = new EnumMap<>(Cf.class);
            if (handles.size() != ordered.size()) {
                throw new CengyanException(ErrorCode.INTERNAL, "列族数量与预期不符");
            }
            for (int i = 0; i < ordered.size(); i++) {
                byFamily.put(ordered.get(i), handles.get(i));
            }
            log.info("已打开数据库，目录 {}", path);
            return new RocksGraphStore(path, db, dbOptions, cfOptions, handles, byFamily, cache, ownsCache, bloom);
        } catch (RuntimeException | RocksDBException ex) {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
            if (db != null) {
                db.close();
            }
            for (ColumnFamilyOptions opt : cfOptions) {
                opt.close();
            }
            if (dbOptions != null) {
                dbOptions.close();
            }
            if (bloom != null) {
                bloom.close();
            }
            if (ownsCache && cache != null) {
                cache.close();
            }
            if (ex instanceof CengyanException cengyan) {
                throw cengyan;
            }
            throw new CengyanException(ErrorCode.INTERNAL, "无法打开数据库：" + ex.getMessage(), ex);
        }
    }

    private static List<Cf> orderedFamilies(Set<Cf> families) {
        LinkedHashSet<Cf> ordered = new LinkedHashSet<>();
        ordered.add(Cf.DEFAULT);
        for (Cf cf : Cf.values()) {
            if (cf != Cf.DEFAULT && families.contains(cf)) {
                ordered.add(cf);
            }
        }
        return List.copyOf(ordered);
    }

    private static ColumnFamilyOptions columnFamilyOptions(
            Cf cf, StoreOptions options, Cache cache, BloomFilter bloom, boolean slimDefault) {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        cfOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        long buffer = cf == Cf.DEFAULT && slimDefault
                ? Math.min(1L << 20, options.writeBufferSize())
                : options.writeBufferSize();
        cfOptions.setWriteBufferSize(buffer);
        BlockBasedTableConfig table = new BlockBasedTableConfig();
        table.setBlockCache(cache);
        table.setCacheIndexAndFilterBlocks(true);
        if (cf == Cf.EDGE) {
            // 出边前缀是前 17 字节。前缀布隆只挂在边列族上。
            cfOptions.useFixedLengthPrefixExtractor(KeyCodec.EDGE_PREFIX_LENGTH);
            cfOptions.setMemtablePrefixBloomSizeRatio(0.125);
            if (bloom != null) {
                table.setFilterPolicy(bloom);
                table.setWholeKeyFiltering(false);
            }
        }
        cfOptions.setTableFormatConfig(table);
        return cfOptions;
    }

    ColumnFamilyHandle handle(Cf cf) {
        ColumnFamilyHandle handle = byFamily.get(cf);
        if (handle == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "列族未打开：" + cf.familyName());
        }
        return handle;
    }

    RocksDB db() {
        return db;
    }

    @Override
    public void mutate(StoreBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("批次不能为空");
        }
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        if (batch.isEmpty()) {
            return;
        }
        writeLock.lock();
        try {
            WriteGate gate = writeGate;
            if (gate != null) {
                gate.replicate(this, batch);
                return;
            }
            writeUnlocked(batch);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void applyLocal(StoreBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("批次不能为空");
        }
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        if (batch.isEmpty()) {
            return;
        }
        writeLock.lock();
        try {
            writeUnlocked(batch);
        } finally {
            writeLock.unlock();
        }
    }

    private void writeUnlocked(StoreBatch batch) {
        try (WriteBatch writeBatch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            options.setDisableWAL(false);
            for (StoreBatch.Op op : batch.operations()) {
                ColumnFamilyHandle family = handle(op.family());
                if (op.delete()) {
                    writeBatch.delete(family, op.key());
                } else {
                    writeBatch.put(family, op.key(), op.value());
                }
            }
            Runnable hook = beforeWrite;
            if (hook != null) {
                beforeWrite = null;
                hook.run();
            }
            db.write(options, writeBatch);
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "写入失败：" + ex.getMessage(), ex);
        }
    }

    @Override
    public void commitEdges(StoreBatch batch) {
        requireEdgePairs(batch);
        mutate(batch);
    }

    @Override
    public void insertEdge(int graphId, int edgeTypeId, long rank, long outSrc, long outDst) {
        int outPart = partOf(outSrc);
        int inPart = partOf(outDst);
        if (outPart != inPart) {
            throw new CengyanException(ErrorCode.CROSS_PART_UNSUPPORTED, "边的两端落在不同分片");
        }
        StoreBatch batch = new StoreBatch();
        batch.put(Cf.EDGE, KeyCodec.edgeKey(
                graphId, outPart, outSrc, edgeTypeId, Direction.OUT.code(), rank, outDst), EMPTY);
        batch.put(Cf.EDGE, KeyCodec.edgeKey(
                graphId, inPart, outDst, edgeTypeId, Direction.IN.code(), rank, outSrc), EMPTY);
        commitEdges(batch);
    }

    @Override
    public ReadSession read() {
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        return new RocksReadSession(this, db.getSnapshot());
    }

    @Override
    public <T> T withWriteLock(Supplier<T> action) {
        if (action == null) {
            throw new IllegalArgumentException("写入动作不能为空");
        }
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void injectBeforeWrite(Runnable hook) {
        this.beforeWrite = hook;
    }

    @Override
    public long propReads() {
        return propReads.get();
    }

    @Override
    public long edgesExamined() {
        return edgesExamined.get();
    }

    @Override
    public long indexReads() {
        return indexReads.get();
    }

    @Override
    public long verticesExamined() {
        return verticesExamined.get();
    }

    @Override
    public long storageSizeBytes() {
        try (var files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException ignored) {
                            return 0L;
                        }
                    })
                    .sum();
        } catch (IOException ignored) {
            return -1L;
        }
    }

    void addPropReads(int count) {
        if (count > 0) {
            propReads.addAndGet(count);
        }
    }

    void addEdgeExamined() {
        edgesExamined.incrementAndGet();
    }

    void addIndexReads(int count) {
        if (count > 0) {
            indexReads.addAndGet(count);
        }
    }

    void addVerticesExamined() {
        verticesExamined.incrementAndGet();
    }

    @Override
    public void flush() {
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        try (FlushOptions options = new FlushOptions()) {
            options.setWaitForFlush(true);
            db.flush(options, handles);
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "刷盘失败：" + ex.getMessage(), ex);
        }
        log.info("已刷盘");
    }

    @Override
    public void compact() {
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        try {
            for (ColumnFamilyHandle handle : handles) {
                db.compactRange(handle);
            }
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "压缩失败：" + ex.getMessage(), ex);
        }
        log.info("已压缩");
    }

    @Override
    public Path directory() {
        return directory;
    }

    @Override
    public void checkpoint(Path target) {
        if (target == null) {
            throw new IllegalArgumentException("检查点目录不能为空");
        }
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据库已关闭");
        }
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(target.toAbsolutePath().toString());
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "检查点失败：" + ex.getMessage(), ex);
        }
        log.info("已创建检查点 {}", target);
    }

    @Override
    public void setWriteGate(WriteGate gate) {
        this.writeGate = gate;
    }

    @Override
    public void setPartResolver(BiFunction<Integer, Long, Integer> resolver) {
        this.partResolver = resolver == null ? (graphId, vid) -> 0 : resolver;
    }

    int partOf(int graphId, long vid) {
        return partResolver.apply(graphId, vid);
    }

    /** 不带图时按 1 个分片。扫描要走 {@link #partOf(int, long)}。 */
    public static int partOf(long vid) {
        return KeyCodec.partOf(vid);
    }

    private static void requireEdgePairs(StoreBatch batch) {
        Map<Boolean, Map<LogicalEdge, boolean[]>> groups = new HashMap<>();
        int edgeOps = 0;
        for (StoreBatch.Op op : batch.operations()) {
            if (op.family() != Cf.EDGE) {
                continue;
            }
            edgeOps++;
            KeyCodec.DecodedEdge decoded = KeyCodec.decodeEdge(op.key());
            long outSrc;
            long outDst;
            if (decoded.direction() == Direction.OUT.code()) {
                outSrc = decoded.src();
                outDst = decoded.dst();
            } else if (decoded.direction() == Direction.IN.code()) {
                outSrc = decoded.dst();
                outDst = decoded.src();
            } else {
                throw new CengyanException(ErrorCode.INTERNAL, "边方向无效");
            }
            LogicalEdge id = new LogicalEdge(
                    decoded.graphId(), decoded.edgeTypeId(), decoded.rank(), outSrc, outDst);
            groups.computeIfAbsent(op.delete(), ignored -> new HashMap<>())
                    .computeIfAbsent(id, ignored -> new boolean[2])[decoded.direction()] = true;
        }
        if (edgeOps == 0) {
            throw new CengyanException(ErrorCode.INTERNAL, "插入边必须同时写出边和入边");
        }
        for (Map<LogicalEdge, boolean[]> group : groups.values()) {
            for (boolean[] directions : group.values()) {
                if (!directions[0] || !directions[1]) {
                    throw new CengyanException(ErrorCode.INTERNAL, "插入边必须同时写出边和入边");
                }
            }
        }
    }

    private record LogicalEdge(int graphId, int edgeTypeId, long rank, long outSrc, long outDst) {
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        db.close();
        for (ColumnFamilyOptions opt : cfOptions) {
            opt.close();
        }
        dbOptions.close();
        if (edgeBloom != null) {
            edgeBloom.close();
        }
        if (ownsCache) {
            blockCache.close();
        }
        log.info("已关闭数据库，目录 {}", directory);
    }
}
