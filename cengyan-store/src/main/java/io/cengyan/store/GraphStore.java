package io.cengyan.store;

import java.nio.file.Path;

/**
 * 进程内图存储。查询模块只依赖这个接口，不引用 RocksDB 的类。
 */
public interface GraphStore extends AutoCloseable {
    /**
     * 用开发默认项打开。服务进程应使用 {@link #open(Path, StoreOptions)} 传入配置里的块缓存。
     */
    static GraphStore open(Path path) {
        return RocksGraphStore.open(path, StoreOptions.development());
    }

    static GraphStore open(Path path, StoreOptions options) {
        return RocksGraphStore.open(path, options);
    }

    @Override
    void close();
}
