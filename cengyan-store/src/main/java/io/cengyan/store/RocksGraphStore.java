package io.cengyan.store;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 嵌入式 RocksDB。七个列族在第一次打开时建好，WAL 保持打开，边列族使用 17 字节前缀。
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
    private final BloomFilter edgeBloom;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RocksGraphStore(
            Path directory,
            RocksDB db,
            DBOptions dbOptions,
            List<ColumnFamilyOptions> cfOptions,
            List<ColumnFamilyHandle> handles,
            Map<Cf, ColumnFamilyHandle> byFamily,
            Cache blockCache,
            BloomFilter edgeBloom) {
        this.directory = directory;
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.handles = handles;
        this.byFamily = byFamily;
        this.blockCache = blockCache;
        this.edgeBloom = edgeBloom;
    }

    public static RocksGraphStore open(Path directory, StoreOptions options) {
        if (directory == null) {
            throw new IllegalArgumentException("数据目录不能为空");
        }
        if (options == null) {
            throw new IllegalArgumentException("打开参数不能为空");
        }
        Path path = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(path);
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法创建数据目录：" + ex.getMessage(), ex);
        }

        Cache cache = null;
        BloomFilter bloom = null;
        DBOptions dbOptions = null;
        List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = null;
        try {
            cache = new LRUCache(options.blockCacheBytes());
            bloom = new BloomFilter(10);
            dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true);
            dbOptions.setCreateMissingColumnFamilies(true);
            dbOptions.setMaxBackgroundJobs(options.maxBackgroundJobs());

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (Cf cf : Cf.values()) {
                ColumnFamilyOptions one = columnFamilyOptions(cf, options, cache, bloom);
                cfOptions.add(one);
                descriptors.add(new ColumnFamilyDescriptor(cf.nameBytes(), one));
            }
            db = RocksDB.open(dbOptions, path.toString(), descriptors, handles);
            Map<Cf, ColumnFamilyHandle> byFamily = new EnumMap<>(Cf.class);
            Cf[] families = Cf.values();
            if (handles.size() != families.length) {
                throw new CengyanException(ErrorCode.INTERNAL, "列族数量与预期不符");
            }
            for (int i = 0; i < families.length; i++) {
                byFamily.put(families[i], handles.get(i));
            }
            log.info("已打开数据库，目录 {}", path);
            return new RocksGraphStore(path, db, dbOptions, cfOptions, handles, byFamily, cache, bloom);
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
            if (cache != null) {
                cache.close();
            }
            if (ex instanceof CengyanException cengyan) {
                throw cengyan;
            }
            throw new CengyanException(ErrorCode.INTERNAL, "无法打开数据库：" + ex.getMessage(), ex);
        }
    }

    private static ColumnFamilyOptions columnFamilyOptions(
            Cf cf, StoreOptions options, Cache cache, BloomFilter bloom) {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        cfOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        cfOptions.setWriteBufferSize(options.writeBufferSize());
        BlockBasedTableConfig table = new BlockBasedTableConfig();
        table.setBlockCache(cache);
        table.setCacheIndexAndFilterBlocks(true);
        if (cf == Cf.EDGE) {
            // 出边前缀是前 17 字节。前缀布隆只挂在边列族上。
            cfOptions.useFixedLengthPrefixExtractor(17);
            cfOptions.setMemtablePrefixBloomSizeRatio(0.125);
            table.setFilterPolicy(bloom);
            table.setWholeKeyFiltering(false);
        }
        cfOptions.setTableFormatConfig(table);
        return cfOptions;
    }

    Path directory() {
        return directory;
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
        edgeBloom.close();
        blockCache.close();
        log.info("已关闭数据库，目录 {}", directory);
    }
}
