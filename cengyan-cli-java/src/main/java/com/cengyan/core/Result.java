package com.cengyan.core;

import com.cengyan.core.exception.ResultException;
import com.cengyan.core.internal.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 一条查询的完整结果。结果集已经全部在内存里，{@link #size()} 和迭代不会触发额外请求。不可变。
 *
 * <p>值的 Java 类型：整数 {@code Long}，小数 {@code Double}，字符串 {@code String}，布尔 {@code Boolean}，
 * 点/边/路径和其他对象为 {@code Map}，列表为 {@code List}；DATETIME 为 ISO 字符串（用 {@link Record#getInstant} 解析）。
 */
public final class Result implements Iterable<Record> {
    private final Map<String, Object> raw;
    private final List<String> columns;
    private final Map<String, Integer> indexes;
    private final List<List<Object>> rows;
    private final long rowsAffected;

    @SuppressWarnings("unchecked")
    public Result(Map<String, Object> response) {
        this.raw = Collections.unmodifiableMap(response);
        Object rawColumns = response.get("columns");
        Object rawRows = response.get("rows");
        List<String> names = new ArrayList<String>();
        if (rawColumns != null) {
            if (!(rawColumns instanceof List)) throw shape("columns 不是列表");
            for (Object name : (List<Object>) rawColumns) {
                if (!(name instanceof String)) throw shape("列名不是字符串：" + name);
                names.add((String) name);
            }
        }
        List<List<Object>> values = new ArrayList<List<Object>>();
        if (rawRows != null) {
            if (!(rawRows instanceof List)) throw shape("rows 不是列表");
            for (Object row : (List<Object>) rawRows) {
                if (!(row instanceof List)) throw shape("结果行不是列表");
                values.add(Collections.unmodifiableList((List<Object>) row));
            }
        }
        this.columns = Collections.unmodifiableList(names);
        Map<String, Integer> positions = new HashMap<String, Integer>();
        // 重名列取第一个，与按列名生成 dict 时"后者覆盖"不同：保证 get(name) 稳定指向最左列。
        for (int i = names.size() - 1; i >= 0; i--) positions.put(names.get(i), i);
        this.indexes = Collections.unmodifiableMap(positions);
        this.rows = Collections.unmodifiableList(values);
        Object affected = response.get("rowsAffected");
        this.rowsAffected = affected instanceof Number ? ((Number) affected).longValue() : 0L;
    }

    private static ResultException shape(String message) {
        return new ResultException(message, "RESULT_SHAPE", null);
    }

    /** 服务端原始响应（只读）。 */
    public Map<String, Object> raw() {
        return raw;
    }

    public List<String> columns() {
        return columns;
    }

    /** 原始行，按列顺序。 */
    public List<List<Object>> rows() {
        return rows;
    }

    public int size() {
        return rows.size();
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** 写语句影响的行数。 */
    public long rowsAffected() {
        return rowsAffected;
    }

    public String queryId() {
        Object value = raw.get("queryId");
        return value == null ? null : String.valueOf(value);
    }

    /** 第 index 行。 */
    public Record get(int index) {
        return new Record(columns, indexes, rows.get(index));
    }

    /** 全部行（按需构造的只读视图）。 */
    public List<Record> records() {
        return new AbstractList<Record>() {
            @Override
            public Record get(int index) {
                return Result.this.get(index);
            }

            @Override
            public int size() {
                return rows.size();
            }
        };
    }

    @Override
    public Iterator<Record> iterator() {
        return records().iterator();
    }

    public Stream<Record> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /** 第一行，没有行时为 null。 */
    public Record first() {
        return rows.isEmpty() ? null : get(0);
    }

    /** 唯一的一行；行数不为 1 时抛 {@link ResultException}（code {@code RESULT_SHAPE}）。 */
    public Record one() {
        if (rows.size() != 1) {
            throw new ResultException("result does not contain exactly one row (got " + rows.size() + ")", "RESULT_SHAPE", null);
        }
        return get(0);
    }

    /** 第一行第一列，没有时为 null。 */
    public Object scalar() {
        return rows.isEmpty() || rows.get(0).isEmpty() ? null : rows.get(0).get(0);
    }

    /** 每行映射为 POJO（规则见 {@link Record#as}）。 */
    public <T> List<T> list(Class<T> type) {
        List<T> list = new ArrayList<T>(rows.size());
        for (Record record : this) list.add(record.as(type));
        return list;
    }

    /** 第一行映射为 POJO，没有行时为 null。 */
    public <T> T first(Class<T> type) {
        Record record = first();
        return record == null ? null : record.as(type);
    }

    /** 唯一一行映射为 POJO；行数不为 1 时抛 {@link ResultException}。 */
    public <T> T one(Class<T> type) {
        return one().as(type);
    }

    /** 行列表的 JSON（每行一个按列名的对象）。 */
    public String toJson() {
        List<Object> list = new ArrayList<Object>(rows.size());
        for (Record record : this) list.add(record.asMap());
        return new String(Json.encodeForExport(list), StandardCharsets.UTF_8);
    }

    public void writeJson(Writer writer) {
        try {
            writer.write(toJson());
            writer.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /** 每行一个 JSON 对象。 */
    public void writeJsonLines(Writer writer) {
        try {
            for (Record record : this) {
                writer.write(new String(Json.encodeForExport(record.asMap()), StandardCharsets.UTF_8));
                writer.write('\n');
            }
            writer.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /** RFC 4180 CSV：首行列名，CRLF 换行；列表/对象值（点、路径等）写成 JSON，null 为空。 */
    public void writeCsv(Writer writer) {
        try {
            writeCsvRow(writer, new ArrayList<Object>(columns));
            for (Record record : this) writeCsvRow(writer, new ArrayList<Object>(record.asMap().values()));
            writer.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void toJson(Path path) {
        write(path, false, Format.JSON);
    }

    public void toJsonLines(Path path) {
        write(path, false, Format.JSONL);
    }

    /** 写 CSV 文件；带 UTF-8 BOM，Excel 能直接识别中文。 */
    public void toCsv(Path path) {
        write(path, true, Format.CSV);
    }

    private enum Format { JSON, JSONL, CSV }

    private void write(Path path, boolean bom, Format format) {
        try (OutputStream out = Files.newOutputStream(path); Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            if (bom) writer.write('\uFEFF');
            if (format == Format.JSON) writeJson(writer);
            else if (format == Format.JSONL) writeJsonLines(writer);
            else writeCsv(writer);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void writeCsvRow(Writer writer, List<Object> cells) throws IOException {
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) writer.write(',');
            Object value = cells.get(i);
            String text;
            if (value == null) text = "";
            else if (value instanceof Map || value instanceof List) text = new String(Json.encodeForExport(value), StandardCharsets.UTF_8);
            else text = String.valueOf(value);
            if (text.indexOf(',') >= 0 || text.indexOf('"') >= 0 || text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
                text = "\"" + text.replace("\"", "\"\"") + "\"";
            }
            writer.write(text);
        }
        writer.write("\r\n");
    }

    @Override
    public String toString() {
        return "Result{columns=" + columns.size() + ", rows=" + rows.size() + ", affected=" + rowsAffected + "}";
    }
}
