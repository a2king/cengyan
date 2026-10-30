package io.cengyan.store;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * 把一条键放到哪个目录。实现在目录模块，存储模块只按返回的路径打开库。
 */
public interface DataPlacement {
    Path place(Cf family, byte[] key);

    default List<Path> placePrefix(Cf family, byte[] prefix) {
        throw new UnsupportedOperationException();
    }

    /** 建图的记录还没提交时，先记住图名，避免目录找不到。 */
    default void rememberGraph(int graphId, String name) {
    }

    default String graphName(int graphId) {
        throw new UnsupportedOperationException();
    }

    default int partCount(int graphId) {
        throw new UnsupportedOperationException();
    }

    default Set<Integer> nodeTypeIds(int graphId) {
        throw new UnsupportedOperationException();
    }

    default Set<Integer> edgeTypeIds(int graphId) {
        throw new UnsupportedOperationException();
    }

    default List<Integer> indexIds(int graphId) {
        throw new UnsupportedOperationException();
    }

    default Path nodeShard(int graphId, int typeId, int part) {
        throw new UnsupportedOperationException();
    }

    default Path edgeShard(int graphId, int typeId, int part) {
        throw new UnsupportedOperationException();
    }

    default Path indexShard(int indexId, int part) {
        throw new UnsupportedOperationException();
    }

    /** 按新的分片数计算这条索引键该去哪个分片。 */
    default int indexPart(byte[] key, int partCount) {
        throw new UnsupportedOperationException();
    }
}
