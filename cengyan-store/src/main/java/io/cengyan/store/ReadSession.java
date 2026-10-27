package io.cengyan.store;

import io.cengyan.CancelToken;

import java.util.List;
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

    @Override
    void close();
}
