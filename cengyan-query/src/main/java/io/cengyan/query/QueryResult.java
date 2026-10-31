package io.cengyan.query;

import java.util.List;

/** 一条语句的结果。失败不走这个对象，而是抛出带错误码的异常。 */
public record QueryResult(List<String> columns, List<List<Object>> rows, int rowsAffected, Long collectedAt) {
    public QueryResult(List<String> columns, List<List<Object>> rows, int rowsAffected) {
        this(columns, rows, rowsAffected, null);
    }

    public QueryResult {
        columns = columns == null ? List.of() : List.copyOf(columns);
        rows = rows == null ? List.of() : List.copyOf(rows);
    }

    public static QueryResult changed(int rowsAffected) {
        return new QueryResult(List.of(), List.of(), rowsAffected);
    }
}
