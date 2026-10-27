package io.cengyan.catalog;

import java.util.Map;

/**
 * 插入边时的一端。类型名为空表示只使用 vid，不写点记录。
 */
public record VertexWrite(String typeName, long vid, Map<String, Object> properties) {
    public VertexWrite {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    public static VertexWrite onlyId(long vid) {
        return new VertexWrite(null, vid, Map.of());
    }

    public static VertexWrite of(String typeName, long vid, Map<String, Object> properties) {
        return new VertexWrite(typeName, vid, properties);
    }
}
