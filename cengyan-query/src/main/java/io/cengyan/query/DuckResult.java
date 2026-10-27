package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.parser.Stmt;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * 把已经收住的行交给 DuckDB 做去重、排序、偏移和聚合。
 * 不在这里扩大扫描范围。内存放不下时写到临时目录。
 */
final class DuckResult {
    private DuckResult() {
    }

    static List<List<Object>> arrange(
            List<List<Object>> rows,
            List<List<Object>> sortKeys,
            List<Stmt.Sort> sorts,
            boolean distinct,
            Long skip,
            Long limit,
            DuckSettings settings) {
        if (rows.isEmpty()) {
            return List.of();
        }
        long offset = skip == null ? 0L : skip;
        if (offset < 0 || (limit != null && limit < 0)) {
            throw new CengyanException(ErrorCode.SYNTAX, "LIMIT 或 SKIP 超出范围");
        }
        if (!sorts.isEmpty() && sortKeys.size() != rows.size()) {
            throw new CengyanException(ErrorCode.INTERNAL, "排序键和行数不一致");
        }
        int width = rows.get(0).size();
        Kind[] keys = new Kind[sorts.size()];
        for (int i = 0; i < sorts.size(); i++) {
            keys[i] = kind(sortKeys, i, true);
        }
        try (Held held = open(settings)) {
            try {
                create(held.connection, distinct, width, keys);
                insert(held.connection, rows, sortKeys, distinct, keys);
                return read(held.connection, query(distinct, width, sorts, offset, limit), rows);
            } finally {
                if (settings.beforeClose() != null) {
                    settings.beforeClose().accept(settings.tempDirectory());
                }
            }
        } catch (SQLException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "排序失败：" + ex.getMessage(), ex);
        }
    }

    static List<List<Object>> aggregate(List<List<Object>> rows, String[] kinds, DuckSettings settings) {
        if (kinds == null || kinds.length == 0) {
            throw new CengyanException(ErrorCode.INTERNAL, "聚合列不能为空");
        }
        if (rows.isEmpty()) {
            List<Object> row = new ArrayList<>();
            for (String kind : kinds) {
                row.add("sum".equals(kind) ? null : 0L);
            }
            return List.of(row);
        }
        boolean[] integral = new boolean[kinds.length];
        Kind[] columns = new Kind[kinds.length];
        for (int i = 0; i < kinds.length; i++) {
            integral[i] = !"sum".equals(kinds[i]) || integralSum(rows, i);
            columns[i] = "star".equals(kinds[i]) ? Kind.LONG : kind(rows, i, false);
        }
        try (Held held = open(settings)) {
            try {
                createAggregate(held.connection, columns);
                insertAggregate(held.connection, rows, columns);
                return readAggregate(held.connection, aggregateSql(kinds), kinds, integral);
            } finally {
                if (settings.beforeClose() != null) {
                    settings.beforeClose().accept(settings.tempDirectory());
                }
            }
        } catch (SQLException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "聚合失败：" + ex.getMessage(), ex);
        }
    }

    private static Held open(DuckSettings settings) throws SQLException {
        if (!settings.memoryLimit().matches("[0-9]+([KMGT]B)?")) {
            throw new CengyanException(ErrorCode.INTERNAL, "DuckDB 内存上限格式不正确");
        }
        Path work = settings.tempDirectory().resolve("duck-" + UUID.randomUUID());
        Path spill = work.resolve("spill");
        try {
            Files.createDirectories(spill);
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "找不到 DuckDB 驱动", ex);
        } catch (java.io.IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法创建 DuckDB 临时目录：" + ex.getMessage(), ex);
        }
        Properties properties = new Properties();
        properties.setProperty("memory_limit", settings.memoryLimit());
        properties.setProperty("temp_directory", spill.toAbsolutePath().toString());
        properties.setProperty("max_temp_directory_size", "100GB");
        properties.setProperty("threads", "1");
        Connection connection = DriverManager.getConnection("jdbc:duckdb:" + sqlPath(work.resolve("query.duckdb")), properties);
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET memory_limit='" + settings.memoryLimit() + "'");
            statement.execute("SET temp_directory='" + sqlPath(spill) + "'");
            statement.execute("SET max_temp_directory_size='100GB'");
            statement.execute("SET threads=1");
            statement.execute("SET preserve_insertion_order=false");
        } catch (SQLException ex) {
            connection.close();
            deleteQuietly(work);
            throw ex;
        }
        return new Held(connection, work);
    }

    private static void create(Connection connection, boolean distinct, int width, Kind[] keys) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE t (i BIGINT");
        if (distinct) {
            for (int i = 0; i < width; i++) {
                sql.append(", c").append(i).append(" VARCHAR");
            }
        }
        for (int i = 0; i < keys.length; i++) {
            sql.append(", k").append(i).append(' ').append(keys[i].sql);
        }
        sql.append(')');
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }
    }

    private static void createAggregate(Connection connection, Kind[] columns) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE t (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append('c').append(i).append(' ').append(columns[i].sql);
        }
        sql.append(')');
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }
    }

    private static void insert(
            Connection connection,
            List<List<Object>> rows,
            List<List<Object>> sortKeys,
            boolean distinct,
            Kind[] keys) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO t VALUES (?");
        int extras = (distinct ? rows.get(0).size() : 0) + keys.length;
        for (int i = 0; i < extras; i++) {
            sql.append(", ?");
        }
        sql.append(')');
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int row = 0; row < rows.size(); row++) {
                int index = 1;
                statement.setLong(index++, row);
                if (distinct) {
                    for (Object value : rows.get(row)) {
                        set(statement, index++, value, Kind.TEXT, true);
                    }
                }
                if (keys.length > 0) {
                    List<Object> key = sortKeys.get(row);
                    for (int i = 0; i < keys.length; i++) {
                        set(statement, index++, key.get(i), keys[i], false);
                    }
                }
                statement.addBatch();
                if ((row + 1) % 32 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        }
    }

    private static void insertAggregate(Connection connection, List<List<Object>> rows, Kind[] columns) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO t VALUES (?");
        for (int i = 1; i < columns.length; i++) {
            sql.append(", ?");
        }
        sql.append(')');
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (List<Object> row : rows) {
                for (int i = 0; i < columns.length; i++) {
                    set(statement, i + 1, row.get(i), columns[i], false);
                }
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static String query(boolean distinct, int width, List<Stmt.Sort> sorts, long offset, Long limit) {
        StringBuilder sql = new StringBuilder();
        if (distinct) {
            sql.append("SELECT d.i FROM (SELECT min(i) AS i FROM t GROUP BY ");
            for (int i = 0; i < width; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append('c').append(i);
            }
            sql.append(") d JOIN t ON t.i = d.i ORDER BY ");
        } else {
            sql.append("SELECT i FROM t ORDER BY ");
        }
        if (sorts.isEmpty()) {
            sql.append(distinct ? "d.i" : "i");
        } else {
            for (int i = 0; i < sorts.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(distinct ? "t.k" : "k").append(i)
                        .append(sorts.get(i).descending() ? " DESC" : " ASC")
                        .append(" NULLS LAST");
            }
            sql.append(", ").append(distinct ? "d.i" : "i");
        }
        if (limit != null) {
            sql.append(" LIMIT ").append(limit);
        }
        sql.append(" OFFSET ").append(offset);
        return sql.toString();
    }

    private static String aggregateSql(String[] kinds) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < kinds.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            switch (kinds[i]) {
                case "star" -> sql.append("count(*)");
                case "count" -> sql.append("count(c").append(i).append(')');
                case "sum" -> sql.append("sum(c").append(i).append(')');
                default -> throw new CengyanException(ErrorCode.INTERNAL, "无法识别的聚合 " + kinds[i]);
            }
        }
        sql.append(" FROM t");
        return sql.toString();
    }

    private static List<List<Object>> read(Connection connection, String sql, List<List<Object>> rows) throws SQLException {
        List<List<Object>> ordered = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                int index = result.getInt(1);
                if (index < 0 || index >= rows.size()) {
                    throw new CengyanException(ErrorCode.INTERNAL, "排序结果指向了不存在的行");
                }
                ordered.add(rows.get(index));
            }
        }
        return ordered;
    }

    private static List<List<Object>> readAggregate(
            Connection connection, String sql, String[] kinds, boolean[] integral) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new CengyanException(ErrorCode.INTERNAL, "聚合没有结果");
            }
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < kinds.length; i++) {
                Object value = result.getObject(i + 1);
                if (value == null || result.wasNull()) {
                    row.add(null);
                } else if ("sum".equals(kinds[i]) && !integral[i]) {
                    row.add(toDouble(value));
                } else {
                    row.add(toLong(value));
                }
            }
            return List.of(row);
        }
    }

    private static void set(PreparedStatement statement, int index, Object value, Kind kind, boolean canonical) throws SQLException {
        if (value == null) {
            statement.setNull(index, kind.type);
            return;
        }
        switch (kind) {
            case LONG -> statement.setLong(index, ((Number) value).longValue());
            case DOUBLE -> statement.setDouble(index, ((Number) value).doubleValue());
            case BOOLEAN -> statement.setBoolean(index, (Boolean) value);
            case TEXT -> statement.setString(index, canonical ? canonical(value) : String.valueOf(value));
            default -> throw new CengyanException(ErrorCode.INTERNAL, "无法写入排序列");
        }
    }

    private static Kind kind(List<List<Object>> rows, int column, boolean sort) {
        Kind kind = null;
        for (List<Object> row : rows) {
            Object value = row.get(column);
            if (value == null) {
                continue;
            }
            Kind next = kindOf(value, sort);
            if (kind == null) {
                kind = next;
            } else if (kind != next) {
                if ((kind == Kind.LONG || kind == Kind.DOUBLE) && (next == Kind.LONG || next == Kind.DOUBLE)) {
                    kind = Kind.DOUBLE;
                } else {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这两边的类型不能排序");
                }
            }
        }
        return kind == null ? Kind.TEXT : kind;
    }

    private static Kind kindOf(Object value, boolean sort) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short) {
            return Kind.LONG;
        }
        if (value instanceof Double || value instanceof Float) {
            return Kind.DOUBLE;
        }
        if (value instanceof Boolean) {
            return Kind.BOOLEAN;
        }
        if (value instanceof String) {
            return Kind.TEXT;
        }
        if (!sort && (value instanceof Map<?, ?> || value instanceof List<?>)) {
            return Kind.TEXT;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这两边的类型不能排序");
    }

    private static boolean integralSum(List<List<Object>> rows, int column) {
        for (List<Object> row : rows) {
            Object value = row.get(column);
            if (value instanceof Double || value instanceof Float) {
                return false;
            }
        }
        return true;
    }

    private static long toLong(Object value) {
        if (value instanceof BigInteger big) {
            if (big.bitLength() > 63) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围");
            }
            return big.longValue();
        }
        if (value instanceof BigDecimal decimal) {
            try {
                return decimal.longValueExact();
            } catch (ArithmeticException ex) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围", ex);
            }
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new CengyanException(ErrorCode.INTERNAL, "聚合结果不是数字");
    }

    private static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new CengyanException(ErrorCode.INTERNAL, "聚合结果不是数字");
    }

    private static String canonical(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short) {
            return "l:" + ((Number) value).longValue();
        }
        if (value instanceof Double || value instanceof Float) {
            return "d:" + ((Number) value).doubleValue();
        }
        if (value instanceof Boolean bool) {
            return "b:" + bool;
        }
        if (value instanceof String text) {
            return "s:" + text;
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder text = new StringBuilder("m:");
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                text.append(entry.getKey()).append('=').append(canonical(entry.getValue())).append(';');
            }
            return text.toString();
        }
        if (value instanceof List<?> list) {
            StringBuilder text = new StringBuilder("a:");
            for (Object item : list) {
                text.append(canonical(item)).append(';');
            }
            return text.toString();
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这两边的类型不能去重");
    }

    private static String sqlPath(Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
    }

    private static void deleteQuietly(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (java.io.IOException ignored) {
            // 临时文件可能仍被占用，留给进程退出后清理。
        }
    }

    private static final class Held implements AutoCloseable {
        private final Connection connection;
        private final Path work;

        private Held(Connection connection, Path work) {
            this.connection = connection;
            this.work = work;
        }

        @Override
        public void close() throws SQLException {
            connection.close();
            deleteQuietly(work);
        }
    }

    private enum Kind {
        LONG("BIGINT", Types.BIGINT),
        DOUBLE("DOUBLE", Types.DOUBLE),
        BOOLEAN("BOOLEAN", Types.BOOLEAN),
        TEXT("VARCHAR", Types.VARCHAR);

        private final String sql;
        private final int type;

        Kind(String sql, int type) {
            this.sql = sql;
            this.type = type;
        }
    }
}
