package io.cengyan.catalog;

import java.util.List;

/** 节点类型。一个点的每种节点类型对应一条点键。 */
public record NodeType(
        int id,
        String name,
        int schemaVersion,
        List<PropDef> properties,
        IdType idType,
        int idLength,
        String comment) {

    public NodeType {
        idType = idType == null ? IdType.INT : idType;
        if (idLength <= 0) {
            throw new IllegalArgumentException("ID 长度必须为正数");
        }
        if (idLength > 4096) {
            throw new IllegalArgumentException("ID 长度不能超过 4096");
        }
        if (idType == IdType.INT && idLength > 19) {
            throw new IllegalArgumentException("INT ID 长度不能超过 19 位");
        }
        properties = properties == null ? List.of() : List.copyOf(properties);
    }

    /** 兼容内部构造调用，默认使用整数 ID。 */
    public NodeType(int id, String name, int schemaVersion, List<PropDef> properties) {
        this(id, name, schemaVersion, properties, IdType.INT, 19, null);
    }

    public NodeType(int id, String name, int schemaVersion, List<PropDef> properties,
                    IdType idType, int idLength) {
        this(id, name, schemaVersion, properties, idType, idLength, null);
    }
}
