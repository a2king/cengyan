package com.cengyan.core;

import com.cengyan.core.exception.ResultException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** 查询结果里的一个点：服务端形如 {@code {"id":..., "type":"Person", "properties":{...}}}。不可变。 */
public final class GraphNode {
    private final Object id;
    private final String type;
    private final Map<String, Object> properties;

    public GraphNode(Object id, String type, Map<String, ?> properties) {
        this.id = id;
        this.type = type;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(properties));
    }

    /** 判断一个结果值是不是点（有 id、type、properties，且没有 src/dst）。 */
    public static boolean isNode(Object value) {
        return Entities.isEntity(value) && !((Map<?, ?>) value).containsKey("src");
    }

    /** 把结果值解析为点；形状不对时抛 {@link ResultException}。 */
    public static GraphNode from(Object value) {
        if (!isNode(value)) throw new ResultException("值不是点：" + Entities.describe(value), "RESULT_TYPE", null);
        Map<?, ?> map = (Map<?, ?>) value;
        return new GraphNode(map.get("id"), (String) map.get("type"), Entities.properties(map));
    }

    /** 主键：INT64 图为 {@code Long}，STRING 图为 {@code String}。 */
    public Object id() {
        return id;
    }

    public String type() {
        return type;
    }

    public Map<String, Object> properties() {
        return properties;
    }

    /** 单个属性，不存在时为 null。 */
    public Object property(String name) {
        return properties.get(name);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GraphNode)) return false;
        GraphNode that = (GraphNode) other;
        return Entities.same(id, that.id) && Entities.same(type, that.type) && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return (id == null ? 0 : id.hashCode()) * 31 + (type == null ? 0 : type.hashCode());
    }

    @Override
    public String toString() {
        return "(" + type + " " + id + " " + properties + ")";
    }
}
