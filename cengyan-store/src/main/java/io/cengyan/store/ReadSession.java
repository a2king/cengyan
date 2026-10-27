package io.cengyan.store;

import io.cengyan.CancelToken;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** 一次查询的快照。写在快照打开之后提交的话，这次读取看不见。 */
public interface ReadSession extends AutoCloseable {
    EdgeCursor scanEdges(
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel);

    /**
     * 和上面一样，但 {@code predicate} 为假的边不计入语句上限。
     * 检查过的边仍然计入每点上限。
     */
    EdgeCursor scanEdges(
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            Predicate<EdgeRef> predicate,
            CancelToken cancel);

    /**
     * 按节点类型扫描点。{@code queryLimit} 为 0 时不扫描。
     * 检查到的点数超过 {@code maxExamined} 时失败，而不是把剩下的丢掉。
     */
    List<VertexId> scanVertices(
            int graphId,
            int tagId,
            int queryLimit,
            int maxExamined,
            CancelToken cancel);

    byte[] get(Cf family, byte[] key);

    List<byte[]> multiGet(Cf family, List<byte[]> keys);

    /** 按前缀收集键。短前缀用全序扫描，不走边列族的 17 字节前缀布隆。 */
    List<byte[]> keysWithPrefix(Cf family, byte[] prefix);

    /**
     * 按前缀逐个访问键，每 64 个检查一次取消。
     * 重建和统计用它，避免把整段键先装进内存再没法停下。
     */
    void visitPrefix(Cf family, byte[] prefix, CancelToken cancel, Consumer<byte[]> consumer);

    /**
     * 索引列族的范围扫描，含起点，不含终点。
     * {@code limit} 小于 0 表示不限条数。终点为空表示扫到这个 indexId 结束。
     */
    List<byte[]> scanIndex(byte[] startInclusive, byte[] endExclusive, int limit, CancelToken cancel);

    @Override
    void close();
}
