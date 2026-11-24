package com.cengyan.core;

import com.cengyan.core.exception.ResultException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** 查询结果里的一条边：服务端形如 {@code {"id":..., "type":"knows", "properties":{...}, "src":..., "dst":...}}。不可变。 */
public final class GraphEdge {
    private final Object id;
    private final String type;
    private final Object src;
    private final Object dst;
    private final Map<String, Object> properties;

    public GraphEdge(Object id, String type, Object src, Object dst, Map<String, ?> properties) {
        this.id = id;
        this.type = type;
        this.src = src;
        this.dst = dst;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(properties));
    }

    /** 判断一个结果值是不是边（有 id、type、properties、src、dst）。 */
    public static boolean isEdge(Object value) {
        return Entities.isEntity(value) && ((Map<?, ?>) value).containsKey("src") && ((Map<?, ?>) value).containsKey("dst");
    }

    /** 把结果值解析为边；形状不对时抛 {@link ResultException}。 */
    public static GraphEdge from(Object value) {
        if (!isEdge(value)) throw new ResultException("值不是边：" + Entities.describe(value), "RESULT_TYPE", null);
        Map<?, ?> map = (Map<?, ?>) value;
        return new GraphEdge(map.get("id"), (String) map.get("type"), map.get("src"), map.get("dst"), Entities.properties(map));
    }

    public Object id() {
        return id;
    }

    public String type() {
        return type;
    }

    /** 起点主键。 */
    public Object src() {
        return src;
    }

    /** 终点主键。 */
    public Object dst() {
        return dst;
    }

    public Map<String, Object> properties() {
        return properties;
    }

    public Object property(String name) {
        return properties.get(name);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GraphEdge)) return false;
        GraphEdge that = (GraphEdge) other;
        return Entities.same(id, that.id) && Entities.same(type, that.type) && Entities.same(src, that.src)
                && Entities.same(dst, that.dst) && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return (id == null ? 0 : id.hashCode()) * 31 + (type == null ? 0 : type.hashCode());
    }

    @Override
    public String toString() {
        return "[" + type + " " + id + " " + src + "->" + dst + " " + properties + "]";
    }
}
