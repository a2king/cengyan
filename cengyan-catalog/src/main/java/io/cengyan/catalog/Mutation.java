package io.cengyan.catalog;

import java.util.LinkedHashMap;
import java.util.Map;

/** 一次语句里的点或边修改。目录只负责在同一批次里落盘。 */
public sealed interface Mutation {
    record SetVertex(String typeName, long vid, Map<String, Object> updates) implements Mutation {
        public SetVertex {
            if (typeName == null || updates == null) {
                throw new IllegalArgumentException("点的修改不能为空");
            }
            updates = new LinkedHashMap<>(updates);
        }
    }

    record SetEdge(String typeName, long rank, long outSrc, long outDst, Map<String, Object> updates) implements Mutation {
        public SetEdge {
            if (typeName == null || updates == null) {
                throw new IllegalArgumentException("边的修改不能为空");
            }
            updates = new LinkedHashMap<>(updates);
        }
    }

    record DeleteVertex(long vid) implements Mutation {
    }

    record DeleteEdge(String typeName, long rank, long outSrc, long outDst) implements Mutation {
        public DeleteEdge {
            if (typeName == null) {
                throw new IllegalArgumentException("边类型不能为空");
            }
        }
    }
}
