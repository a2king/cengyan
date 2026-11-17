package io.cengyan.raft;

import org.rocksdb.Cache;
import org.rocksdb.HyperClockCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.RateLimiterMode;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;

import java.util.logging.Logger;

/**
 * 一个进程里所有分片库共用的 RocksDB 资源：块缓存、内存表总额、后台线程和后台写入限速。
 * 这些对象在打开第一个分片库时创建，之后不再改变；{@link #install} 必须在此之前调用。
 *
 * @param blockCacheBytes 块缓存容量，数据块、索引分区和过滤器分区都记在里面
 * @param writeBufferBytes 全部分片库内存表的总额，超过后由 RocksDB 挑最大的内存表刷盘
 * @param backgroundJobs 压实和刷盘线程总数，全部分片库共用同一个线程池
 * @param subcompactions 单次压实最多拆成几段并行
 * @param rateLimitBytesPerSecond 压实和刷盘的写入限速，0 表示不限速
 */
public record RocksTuning(long blockCacheBytes, long writeBufferBytes, int backgroundJobs, int subcompactions,
                          long rateLimitBytesPerSecond) {
    private static final Logger LOG = Logger.getLogger(RocksTuning.class.getName());
    private static final long MIB = 1024L * 1024;
    /** 没有显式配置时的取值，够单元测试和小内存进程使用。 */
    private static final RocksTuning SMALL = new RocksTuning(256 * MIB, 256 * MIB, 2, 1, 0);
    private static RocksTuning current = SMALL;
    private static Shared shared;

    public RocksTuning {
        if (blockCacheBytes < 8 * MIB || writeBufferBytes < 16 * MIB || backgroundJobs < 2 || subcompactions < 1
                || rateLimitBytesPerSecond < 0 || (rateLimitBytesPerSecond > 0 && rateLimitBytesPerSecond < MIB)) {
            throw new IllegalArgumentException("RocksDB 资源配置无效");
        }
    }

    /**
     * 按机器大小取默认值：块缓存占物理内存五分之一，内存表总额占十六分之一，后台线程占四分之三的核。
     * 16 核 32G 的机器得到约 6.4G 块缓存、2G 内存表、12 个后台线程、4 段并行压实。
     * 五分之一而不是更多：Compute 通常和 Storage 在同一台机器上，两个 JVM 的堆也要放得下。
     */
    public static RocksTuning forMachine(long memoryBytes, int processors, long rateLimitBytesPerSecond) {
        long cache = clamp(memoryBytes / 5, 256 * MIB, 16 * 1024 * MIB);
        long buffers = clamp(memoryBytes / 16, 256 * MIB, 4 * 1024 * MIB);
        int jobs = (int) clamp(processors * 3L / 4, 2, 16);
        int sub = (int) clamp(processors / 4, 1, 4);
        return new RocksTuning(cache, buffers, jobs, sub, rateLimitBytesPerSecond);
    }

    /** Storage 进程启动时调用一次。分片库已经打开后再调用不会生效，只记一条日志。 */
    public static synchronized void install(RocksTuning tuning) {
        if (shared != null) {
            if (!tuning.equals(current)) LOG.warning("分片库已经打开，新的 RocksDB 资源配置不会生效");
            return;
        }
        current = tuning;
    }

    public static synchronized RocksTuning current() { return current; }

    static synchronized Shared shared() {
        if (shared == null) {
            RocksDB.loadLibrary();
            // HyperClockCache 读取不加锁；第二个参数为 0 时由 RocksDB 按实际条目大小自动调整哈希表。
            Cache cache = new HyperClockCache(current.blockCacheBytes, 0, -1, false);
            WriteBufferManager buffers = new WriteBufferManager(current.writeBufferBytes, cache);
            RateLimiter limiter = current.rateLimitBytesPerSecond == 0 ? null
                    : new RateLimiter(current.rateLimitBytesPerSecond, 100_000, 10, RateLimiterMode.WRITES_ONLY, false);
            shared = new Shared(cache, buffers, limiter);
            LOG.info("RocksDB 共享资源：块缓存 " + current.blockCacheBytes / MIB + " MiB，内存表总额 "
                    + current.writeBufferBytes / MIB + " MiB，后台线程 " + current.backgroundJobs + "，并行压实 "
                    + current.subcompactions + "，写入限速 "
                    + (limiter == null ? "关闭" : current.rateLimitBytesPerSecond / MIB + " MiB/s"));
        }
        return shared;
    }

    private static long clamp(long value, long low, long high) { return Math.max(low, Math.min(high, value)); }

    /** 进程存活期间不关闭：RocksJava 要求这些对象活得比所有引用它们的数据库更久。 */
    record Shared(Cache cache, WriteBufferManager buffers, RateLimiter limiter) { }
}
