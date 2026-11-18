package com.cengyan.core;

import com.cengyan.core.exception.ResultException;
import com.cengyan.core.internal.PojoMapper;
import com.cengyan.core.internal.Values;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 结果中的一行。按列名或列序号取值；带类型的取值方法在值为 null 时返回 null，
 * 类型不符或越界时抛 {@link ResultException}（code {@code RESULT_TYPE}）。不可变。
 */
public final class Record {
    private final List<String> columns;
    private final Map<String, Integer> indexes;
    private final List<Object> values;

    Record(List<String> columns, Map<String, Integer> indexes, List<Object> values) {
        this.columns = columns;
        this.indexes = indexes;
        this.values = Collections.unmodifiableList(values);
    }

    /** 列名，按结果列顺序。 */
    public List<String> keys() {
        return columns;
    }

    /** 本行的原始值，按列顺序（行比列短时缺的列不在其中）。 */
    public List<Object> values() {
        return values;
    }

    public int size() {
        return columns.size();
    }

    public boolean containsKey(String column) {
        return indexes.containsKey(column);
    }

    /** 第 index 列的原始值；行比列短时缺的列为 null。 */
    public Object get(int index) {
        if (index < 0 || index >= columns.size()) {
            throw new ResultException("列序号越界：" + index + "（共 " + columns.size() + " 列）", "RESULT_COLUMN", null);
        }
        return index < values.size() ? values.get(index) : null;
    }

    /** 列的原始值（Long / Double / String / Boolean / Map / List / null）。列不存在时抛 {@link ResultException}。 */
    public Object get(String column) {
        Integer index = indexes.get(column);
        if (index == null) throw new ResultException("结果中没有列：" + column + "，可用列：" + columns, "RESULT_COLUMN", null);
        return get(index);
    }

    public String getString(String column) {
        try {
            return Values.toText(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    public Long getLong(String column) {
        try {
            return Values.toLong(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    /** 整数列取为 int；超出 int 范围时报错而不是截断。 */
    public Integer getInt(String column) {
        try {
            return Values.toInt(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    public Double getDouble(String column) {
        try {
            return Values.toDouble(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    public Boolean getBoolean(String column) {
        try {
            return Values.toBoolean(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    /** DATETIME 列（结果里是 ISO 字符串）解析为 UTC 时刻。 */
    public Instant getInstant(String column) {
        try {
            return Values.toInstant(get(column));
        } catch (IllegalArgumentException ex) {
            throw typeError(column, ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String column) {
        Object value = get(column);
        if (value == null || value instanceof Map) return (Map<String, Object>) value;
        throw typeError(column, new IllegalArgumentException("期望对象，实际是 " + value.getClass().getSimpleName()));
    }

    @SuppressWarnings("unchecked")
    public List<Object> getList(String column) {
        Object value = get(column);
        if (value == null || value instanceof List) return (List<Object>) value;
        throw typeError(column, new IllegalArgumentException("期望列表，实际是 " + value.getClass().getSimpleName()));
    }

    /** 整个点（如 {@code RETURN n}）。 */
    public GraphNode getNode(String column) {
        Object value = get(column);
        return value == null ? null : GraphNode.from(value);
    }

    /** 整条边（如 {@code RETURN e}）。 */
    public GraphEdge getEdge(String column) {
        Object value = get(column);
        return value == null ? null : GraphEdge.from(value);
    }

    /** 整条路径（如 {@code RETURN p}）。 */
    public GraphPath getPath(String column) {
        Object value = get(column);
        return value == null ? null : GraphPath.from(value);
    }

    /** 列名到值的有序映射（缺的列为 null）。 */
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < columns.size(); i++) map.put(columns.get(i), i < values.size() ? values.get(i) : null);
        return map;
    }

    /**
     * 映射为 POJO。按列名填同名字段（{@code @CengyanProperty} 可改名）；整行只有一列且是点或边时，
     * 展开为 id（{@code @CengyanId} 或名为 id 的字段）+ properties（边还有 src/dst）。
     */
    public <T> T as(Class<T> type) {
        return PojoMapper.fromRecord(asMap(), type);
    }

    private ResultException typeError(String column, IllegalArgumentException cause) {
        return new ResultException("列 " + column + " 的值类型不符：" + cause.getMessage(), "RESULT_TYPE", null, cause);
    }

    @Override
    public String toString() {
        return asMap().toString();
    }
}
