package io.cengyan.store;

import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.Supplier;

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

    /** 原始批次。不检查出边和入边是否成对，目录写入走这里。 */
    void mutate(StoreBatch batch);

    /**
     * 提交边批次。每条逻辑边都必须同时有出边和入边，只写一个方向直接失败。
     * 批次里可以同时带上点和属性键。
     */
    void commitEdges(StoreBatch batch);

    /** 插入一条逻辑边，写出边和入边两条键，值是空的。 */
    void insertEdge(int graphId, int edgeTypeId, long rank, long outSrc, long outDst);

    /** 打开一个读快照。用完关闭。 */
    ReadSession read();

    /**
     * 占住进程写锁做「先读再写」。{@link #mutate} 可以在里面重入。
     * 创建名字必须走这里，避免两个会话抢同一个名字都成功。
     */
    <T> T withWriteLock(Supplier<T> action);

    /** 下一次提交前运行，然后清除。测试用来模拟提交前进程退出。 */
    void injectBeforeWrite(Runnable hook);

    /** {@code prop} 列族被 get 或 multiGet 命中的键次数。扫拓扑时应当保持为 0。 */
    long propReads();

    /** 扫边时实际检查过的键数，包含没通过条件下推的边。 */
    long edgesExamined();

    /** 索引范围扫描返回的键数。建完前的普通扫描应当保持不变。 */
    long indexReads();

    /** 按类型扫描起点时检查过的点键数。走索引时应当保持不变。 */
    long verticesExamined();

    /** 当前存储目录中的文件总字节数。无法读取时返回 -1。 */
    default long storageSizeBytes() {
        return -1L;
    }

    /** 把内存表刷到磁盘。进行中的快照读可以继续。 */
    void flush();

    /** 压缩全部列族。进行中的快照读可以继续。 */
    void compact();

    /** 数据目录。副本的提交日志写在这个目录下。 */
    Path directory();

    /**
     * 做检查点。调用前应先 {@link #flush()}。目标目录必须还不存在。
     * 查询模块不引用 RocksDB 的检查点类。
     */
    void checkpoint(Path directory);

    /** 只写这一份库，不再转发给副本。 */
    void applyLocal(StoreBatch batch);

    /**
     * 有 3 个投票成员以后，写入先记提交日志，多数派确认后再写各副本。
     * 不足 3 个时仍只保证本机这一份。
     */
    void setWriteGate(WriteGate gate);

    /** 按图计算边和点的分片号。没设置时分片数是 1，分片号是 0。 */
    void setPartResolver(BiFunction<Integer, Long, Integer> resolver);

    @Override
    void close();
}
