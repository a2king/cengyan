package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/** 查询期间的临时 spill 工作区；默认不打开 DuckDB，只有检测到超级节点才 lazy 创建。 */
final class QuerySpillContext implements AutoCloseable {
    private static final ThreadLocal<QuerySpillContext> CURRENT = new ThreadLocal<>();

    private final QueryLimits limits;
    private final DuckSettings settings;
    private final AtomicInteger tableIds = new AtomicInteger();
    private Connection connection;
    private Path work;
    private boolean active;

    private QuerySpillContext(QueryLimits limits, DuckSettings settings) {
        this.limits = limits;
        this.settings = settings;
    }

    static Scope open(QueryLimits limits, DuckSettings settings) {
        QuerySpillContext previous = CURRENT.get();
        QuerySpillContext current = new QuerySpillContext(limits, settings);
        CURRENT.set(current);
        return new Scope(previous, current);
    }

    static QuerySpillContext current() {
        return CURRENT.get();
    }

    boolean enabled() {
        return limits.supernodeSpillEnabled() && limits.supernodeFrontierSpillEnabled()
                && limits.supernodeEdgeThreshold() > 0;
    }

    boolean active() {
        return active;
    }

    int edgeThreshold() {
        return limits.supernodeEdgeThreshold();
    }

    int batchSize() {
        return limits.supernodeSpillBatchEdges();
    }

    void activate(SpillSet... sets) {
        if (!enabled() || active) {
            return;
        }
        open();
        active = true;
        for (SpillSet set : sets) {
            if (set != null) {
                set.activate();
            }
        }
    }

    void activateSets(SpillSet... sets) {
        if (!active) {
            activate(sets);
            return;
        }
        for (SpillSet set : sets) {
            if (set != null) set.activate();
        }
    }

    SpillSet newSet() {
        SpillSet set = new SpillSet(this);
        if (active) {
            set.activate();
        }
        return set;
    }

    private void open() {
        if (connection != null) {
            return;
        }
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            work = settings.tempDirectory().resolve("spill-" + UUID.randomUUID());
            Path spill = work.resolve("spill");
            Files.createDirectories(spill);
            String dbPath = work.resolve("query.duckdb").toAbsolutePath().toString()
                    .replace('\\', '/').replace("'", "''");
            connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET memory_limit='" + settings.forCurrentQuery().memoryLimit() + "'");
                statement.execute("SET temp_directory='" + spill.toAbsolutePath().toString().replace('\\', '/') + "'");
                statement.execute("SET max_temp_directory_size='" + maxTempSize() + "'");
                statement.execute("SET threads=" + settings.threads());
            }
        } catch (ClassNotFoundException | SQLException | IOException ex) {
            closeQuietly();
            throw new CengyanException(ErrorCode.INTERNAL, "无法打开超级节点 spill 工作区：" + ex.getMessage(), ex);
        }
    }

    private String maxTempSize() {
        if (limits.supernodeSpillMaxBytes() <= 0) {
            return settings.maxTempDirectorySize();
        }
        long bytes = limits.supernodeSpillMaxBytes();
        if (bytes % (1024L * 1024 * 1024) == 0) return (bytes / (1024L * 1024 * 1024)) + "GB";
        if (bytes % (1024L * 1024) == 0) return (bytes / (1024L * 1024)) + "MB";
        if (bytes % 1024L == 0) return (bytes / 1024L) + "KB";
        return bytes + "B";
    }

    Connection connection() {
        if (connection == null) {
            throw new IllegalStateException("spill 工作区尚未打开");
        }
        return connection;
    }

    String tableName() {
        return "spill_set_" + tableIds.incrementAndGet();
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
            connection = null;
        }
        if (work != null && Files.exists(work)) {
            try (var walk = Files.walk(work)) {
                for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            } catch (IOException ignored) {
            }
        }
        work = null;
    }

    static final class Scope implements AutoCloseable {
        private final QuerySpillContext previous;
        private final QuerySpillContext current;

        private Scope(QuerySpillContext previous, QuerySpillContext current) {
            this.previous = previous;
            this.current = current;
        }

        @Override
        public void close() {
            current.close();
            if (previous == null) CURRENT.remove();
            else CURRENT.set(previous);
        }
    }

    static final class SpillSet {
        private final QuerySpillContext owner;
        private java.util.LinkedHashSet<Long> memory = new java.util.LinkedHashSet<>();
        private String table;
        private long sequence;

        private SpillSet(QuerySpillContext owner) {
            this.owner = owner;
        }

        boolean add(long value) {
            if (table == null) {
                return memory.add(value);
            }
            try (PreparedStatement statement = owner.connection().prepareStatement(
                    "INSERT OR IGNORE INTO " + table + " (vid, seq) VALUES (?, ?) RETURNING vid")) {
                statement.setLong(1, value);
                statement.setLong(2, sequence++);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next();
                }
            } catch (SQLException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "写入超级节点 spill 状态失败：" + ex.getMessage(), ex);
            }
        }

        long size() {
            if (table == null) return memory.size();
            try (Statement statement = owner.connection().createStatement();
                 ResultSet result = statement.executeQuery("SELECT count(*) FROM " + table)) {
                result.next();
                return result.getLong(1);
            } catch (SQLException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "读取超级节点 spill 状态失败：" + ex.getMessage(), ex);
            }
        }

        void forEach(LongConsumer consumer) {
            if (table == null) {
                memory.forEach(consumer::accept);
                return;
            }
            try (Statement statement = owner.connection().createStatement();
                 ResultSet result = statement.executeQuery("SELECT vid FROM " + table + " ORDER BY seq")) {
                while (result.next()) consumer.accept(result.getLong(1));
            } catch (SQLException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "读取超级节点 spill 状态失败：" + ex.getMessage(), ex);
            }
        }

        void activate() {
            if (table != null) return;
            table = owner.tableName();
            try (Statement statement = owner.connection().createStatement()) {
                statement.execute("CREATE TEMP TABLE " + table + " (vid BIGINT PRIMARY KEY, seq BIGINT)");
                try (PreparedStatement insert = owner.connection().prepareStatement(
                        "INSERT INTO " + table + " (vid, seq) VALUES (?, ?)")) {
                    int pending = 0;
                    for (Long value : memory) {
                        insert.setLong(1, value);
                        insert.setLong(2, sequence++);
                        insert.addBatch();
                        if (++pending >= owner.batchSize()) {
                            insert.executeBatch();
                            pending = 0;
                        }
                    }
                    insert.executeBatch();
                }
                memory.clear();
                memory = null;
            } catch (SQLException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "初始化超级节点 spill 状态失败：" + ex.getMessage(), ex);
            }
        }
    }
}
