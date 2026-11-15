package io.cengyan.client;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** 跨文件去重用的主键集合。数据在磁盘上，避免把全部主键留在堆里。 */
final class ImportIds implements AutoCloseable {
    private final Options options;
    private final Cache cache;
    private final RocksDB db;
    private final WriteOptions write;

    ImportIds(Path directory) throws Exception {
        Files.createDirectories(directory);
        Cache cache = null;
        Options options = null;
        RocksDB db = null;
        try {
            cache = new LRUCache(16L * 1024 * 1024);
            options = new Options().setCreateIfMissing(true).setWriteBufferSize(16L * 1024 * 1024).setMaxWriteBufferNumber(2);
            options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache).setCacheIndexAndFilterBlocks(true));
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            db = RocksDB.open(options, directory.toString());
        } catch (Exception ex) {
            if (db != null) db.close();
            if (options != null) options.close();
            if (cache != null) cache.close();
            throw ex;
        }
        this.cache = cache;
        this.options = options;
        this.db = db;
        this.write = new WriteOptions().setDisableWAL(true);
    }

    boolean add(String id) throws Exception {
        byte[] key = id.getBytes(StandardCharsets.UTF_8);
        if (db.get(key) != null) return false;
        db.put(write, key, new byte[] {1});
        return true;
    }

    @Override public void close() {
        write.close();
        db.close();
        options.close();
        cache.close();
    }
}
