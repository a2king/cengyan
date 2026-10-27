package io.cengyan.store;

import io.cengyan.CancelToken;

import java.util.List;

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

    byte[] get(Cf family, byte[] key);

    List<byte[]> multiGet(Cf family, List<byte[]> keys);

    /** 按前缀收集键。短前缀用全序扫描，不走边列族的 17 字节前缀布隆。 */
    List<byte[]> keysWithPrefix(Cf family, byte[] prefix);

    @Override
    void close();
}
