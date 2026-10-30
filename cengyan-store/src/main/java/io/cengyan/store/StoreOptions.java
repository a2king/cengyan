package io.cengyan.store;

/**
 * RocksDB 打开参数。服务进程按配置传入块缓存；测试用 {@link #development()}，避免占满开发机内存。
 */
public record StoreOptions(long blockCacheBytes, int maxBackgroundJobs, long writeBufferSize) {
    /** 计划书里的写缓冲和后台任务数。块缓存由调用方按机器内存给出。 */
    public static final long SERVER_WRITE_BUFFER_BYTES = 128L * 1024 * 1024;
    public static final int SERVER_BACKGROUND_JOBS = 6;

    public StoreOptions {
        if (blockCacheBytes <= 0) {
            throw new IllegalArgumentException("blockCacheBytes 必须为正");
        }
        if (maxBackgroundJobs <= 0) {
            throw new IllegalArgumentException("maxBackgroundJobs 必须为正");
        }
        if (writeBufferSize <= 0) {
            throw new IllegalArgumentException("writeBufferSize 必须为正");
        }
    }

    /** 服务进程：写缓冲 64MB，后台任务 4。 */
    public static StoreOptions server(long blockCacheBytes) {
        return new StoreOptions(blockCacheBytes, SERVER_BACKGROUND_JOBS, SERVER_WRITE_BUFFER_BYTES);
    }

    public static StoreOptions server(
            long blockCacheBytes, int maxBackgroundJobs, long writeBufferSize) {
        return new StoreOptions(blockCacheBytes, maxBackgroundJobs, writeBufferSize);
    }

    /** 开发与测试：块缓存 64MB，写缓冲缩小，少占内存。 */
    public static StoreOptions development() {
        return new StoreOptions(64L * 1024 * 1024, 2, 8L * 1024 * 1024);
    }
}
