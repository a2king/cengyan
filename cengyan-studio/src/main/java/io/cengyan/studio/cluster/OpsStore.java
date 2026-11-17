package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.studio.cluster.ClusterState.NodeSpec;
import io.cengyan.studio.cluster.ClusterState.Role;
import io.cengyan.studio.cluster.ClusterState.Server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Studio 本机的监控库 {@code data/ops.db}。
 * 页面读的当前状态是每轮写完后留在内存里的快照，不碰数据库；趋势曲线走单独的读连接，不等采集写入。
 * 服务器指标按服务器存数值：15 秒原始点保留 1 天，5 分钟汇总（平均值和最大值）保留 7 天。事件保留 7 天。
 */
final class OpsStore implements AutoCloseable {
    static final long RETENTION_MS = TimeUnit.DAYS.toMillis(7);
    static final long POINT_RETENTION_MS = TimeUnit.DAYS.toMillis(1);
    static final long ROLLUP_MS = TimeUnit.MINUTES.toMillis(5);
    static final long PURGE_EVERY_MS = TimeUnit.HOURS.toMillis(1);
    static final long CONCERN_MS = TimeUnit.SECONDS.toMillis(75);
    static final long BAD_MS = TimeUnit.SECONDS.toMillis(150);
    /** 服务器级指标。比例是 0..1，速率是字节/秒。 */
    static final List<String> METRICS = List.of("cpu", "memory", "diskUsed", "diskBusy", "diskRead", "diskWrite", "netIn", "netOut");
    private static final Set<String> RATES = Set.of("diskRead", "diskWrite", "netIn", "netOut");
    private static final int EVENTS_SHOWN = 50;
    private static final double DISK_BAD = 0.10;
    private static final double DISK_CONCERN = 0.20;
    private static final double BUSY_CONCERN = 0.90;

    private Connection connection;
    /** 只给趋势曲线用。WAL 下读不挡写，写也不挡读。 */
    private Connection reader;
    private final Object readerLock = new Object();
    private volatile Snapshot snapshot = Snapshot.EMPTY;

    OpsStore(Path dataDir) throws IOException {
        Path db = dataDir.resolve("ops.db");
        Files.createDirectories(dataDir);
        String url = "jdbc:sqlite:" + db.toAbsolutePath().toString().replace('\\', '/');
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(url);
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                boolean fresh = true;
                try (ResultSet tables = statement.executeQuery("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='node_latest'")) {
                    fresh = !tables.next() || tables.getInt(1) == 0;
                }
                if (fresh) query(statement, "PRAGMA auto_vacuum=INCREMENTAL");
                query(statement, "PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA synchronous=NORMAL");
                statement.execute("PRAGMA busy_timeout=3000");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS node_latest (
                          node_id TEXT PRIMARY KEY,
                          role TEXT NOT NULL,
                          state TEXT NOT NULL,
                          health TEXT NOT NULL,
                          collected_at INTEGER NOT NULL,
                          success_at INTEGER NOT NULL,
                          message TEXT NOT NULL,
                          payload TEXT NOT NULL
                        )""");
                // 旧版本按节点存整份 JSON 样本，页面从没读过，换成下面的数值表。
                statement.execute("DROP TABLE IF EXISTS sample");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS host_point (
                          host TEXT NOT NULL,
                          metric TEXT NOT NULL,
                          ts_ms INTEGER NOT NULL,
                          value REAL NOT NULL,
                          PRIMARY KEY (host, metric, ts_ms)
                        ) WITHOUT ROWID""");
                statement.execute("CREATE INDEX IF NOT EXISTS host_point_ts ON host_point(ts_ms)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS host_rollup (
                          host TEXT NOT NULL,
                          metric TEXT NOT NULL,
                          bucket_ms INTEGER NOT NULL,
                          n INTEGER NOT NULL,
                          total REAL NOT NULL,
                          peak REAL NOT NULL,
                          PRIMARY KEY (host, metric, bucket_ms)
                        ) WITHOUT ROWID""");
                statement.execute("CREATE INDEX IF NOT EXISTS host_rollup_bucket ON host_rollup(bucket_ms)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS event_log (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          ts_ms INTEGER NOT NULL,
                          node_id TEXT NOT NULL,
                          level TEXT NOT NULL,
                          event TEXT NOT NULL,
                          message TEXT NOT NULL
                        )""");
                statement.execute("CREATE INDEX IF NOT EXISTS event_log_ts ON event_log(ts_ms)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS shard_latest (
                          shard_key TEXT PRIMARY KEY,
                          graph_id INTEGER NOT NULL,
                          graph_name TEXT NOT NULL,
                          part_id INTEGER NOT NULL,
                          leader_id TEXT NOT NULL,
                          term INTEGER NOT NULL,
                          applied_index INTEGER NOT NULL,
                          members TEXT NOT NULL,
                          state TEXT NOT NULL,
                          updated_at INTEGER NOT NULL
                        )""");
                statement.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
            }
            reader = DriverManager.getConnection(url);
            try (Statement statement = reader.createStatement()) {
                statement.execute("PRAGMA busy_timeout=3000");
                statement.execute("PRAGMA query_only=1");
            }
        } catch (ClassNotFoundException ex) {
            close();
            throw new IOException("找不到 SQLite 驱动", ex);
        } catch (SQLException ex) {
            close();
            throw new IOException("监控库打开失败：" + ex.getMessage(), ex);
        }
        ClusterStore.restrict(db, false);
        purge(System.currentTimeMillis());
    }

    synchronized void record(ClusterState state, Map<String, OpsMonitor.Attempt> attempts, long now) {
        try {
            connection.setAutoCommit(false);
            Map<String, NodeRow> previousRows = snapshot.nodes;
            Map<String, String> states = new LinkedHashMap<>();
            Map<String, ObjectNode> fresh = new LinkedHashMap<>();
            List<NodeView> views = new ArrayList<>();
            for (NodeSpec node : state.nodes()) {
                if (!node.joined()) continue;
                OpsMonitor.Attempt attempt = attempts.get(node.id());
                if (attempt == null) attempt = OpsMonitor.Attempt.fail("未采集");
                NodeRow previous = previousRows.get(node.id());
                boolean reachable = attempt.reachable();
                long successAt = reachable ? now : previous == null ? 0 : previous.successAt;
                String nodeState = reachable ? "REACHABLE" : "UNAVAILABLE";
                ObjectNode payload = reachable || previous == null ? storedPayload(attempt.metrics()) : previous.payload;
                String health = health(nodeState, now, successAt, payload);
                String message = trim(attempt.message());
                upsertNode(node, nodeState, health, now, successAt, message, payload);
                if (reachable) fresh.put(node.id(), payload);
                if (previous == null) {
                    if (!"REACHABLE".equals(nodeState) || !"GOOD".equals(health)) {
                        event(now, node.id(), level(health), "NODE", nodeText(node.id(), nodeState, health, message, payload));
                    }
                } else if (!previous.state.equals(nodeState) || !previous.health.equals(health)) {
                    event(now, node.id(), level(health), "NODE", nodeText(node.id(), nodeState, health, message, payload));
                }
                states.put(node.id(), nodeState);
                views.add(new NodeView(node.role().name(), nodeState, health));
            }
            for (String id : previousRows.keySet()) {
                if (states.containsKey(id)) continue;
                try (PreparedStatement delete = connection.prepareStatement("DELETE FROM node_latest WHERE node_id=?")) {
                    delete.setString(1, id);
                    delete.executeUpdate();
                }
                event(now, id, "INFO", "NODE_REMOVED", id + " 已移出集群，停止采集");
            }
            recordHosts(state, fresh, now);
            List<ShardView> shards = mergeShards(attempts, states, now);
            String cluster = clusterHealth(views, shards);
            String previousCluster = meta("cluster_health");
            if (!cluster.equals(previousCluster) && (!previousCluster.isEmpty() || "CONCERNING".equals(cluster) || "BAD".equals(cluster))) {
                event(now, "", level(cluster), "CLUSTER", "集群状态变为" + label(cluster));
            }
            meta("cluster_health", cluster);
            meta("last_round", Long.toString(now));
            long purgedAt = longMeta("purge_at");
            if (purgedAt == 0 || now - purgedAt >= PURGE_EVERY_MS) purgeWithin(now);
            connection.commit();
        } catch (SQLException ex) {
            rollback();
            throw new IllegalStateException("监控记录写入失败：" + ex.getMessage(), ex);
        } finally {
            enableAutoCommit();
        }
        try { snapshot = load(); }
        catch (SQLException ex) { throw new IllegalStateException("监控记录读取失败：" + ex.getMessage(), ex); }
    }

    /** 当前状态。只读内存快照；健康按读取时刻重算，采集卡住时页面也会看到节点变旧。 */
    ObjectNode overview(ClusterState state, long now) {
        Snapshot current = snapshot;
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK");
        if (current.collectedAt > 0) result.put("collectedAt", current.collectedAt);
        ArrayNode compute = result.putArray("computeNodes");
        ArrayNode storage = result.putArray("storageNodes");
        List<NodeView> views = new ArrayList<>();
        Map<String, Integer> leaders = new TreeMap<>();
        for (NodeSpec node : state.nodes()) {
            if (!node.joined()) continue;
            NodeRow row = current.nodes.get(node.id());
            String nodeState = row == null ? "UNKNOWN" : row.state;
            ObjectNode payload = row == null ? ClusterStore.JSON.createObjectNode() : row.payload;
            long successAt = row == null ? 0 : row.successAt;
            String health = row == null ? "UNKNOWN" : health(nodeState, now, successAt, payload);
            ObjectNode item = ClusterStore.JSON.createObjectNode();
            item.setAll(payload.deepCopy());
            item.remove("shards");
            item.put("nodeId", node.id());
            item.put("role", node.role().name());
            item.put("serverId", node.serverId());
            item.put("host", host(state, node));
            String hostname = item.path("hostname").asText("");
            if (hostname.isBlank()) hostname = hostName(state, node.serverId(), "");
            item.put("hostname", hostname);
            item.put("state", nodeState);
            item.put("health", health);
            if (successAt > 0) item.put("successAt", successAt);
            if (row != null && !row.message.isBlank()) item.put("message", row.message);
            if (node.role() == Role.COMPUTE) compute.add(item);
            else {
                storage.add(item);
                String leader = payload.path("metadataLeaderId").asText("");
                if ("REACHABLE".equals(nodeState) && !leader.isBlank()) leaders.merge(leader, 1, Integer::sum);
            }
            views.add(new NodeView(node.role().name(), nodeState, health));
        }
        leaders.entrySet().stream().max(Map.Entry.comparingByValue()).ifPresent(entry -> result.put("metadataLeaderId", entry.getKey()));
        List<ShardView> shardViews = new ArrayList<>();
        ObjectNode summary = result.putObject("shardSummary");
        int healthy = 0, degraded = 0, unavailable = 0;
        for (ShardRow shard : current.shards) {
            shardViews.add(new ShardView(shard.state));
            switch (shard.state) {
                case "HEALTHY" -> healthy++;
                case "DEGRADED" -> degraded++;
                default -> unavailable++;
            }
        }
        summary.put("total", current.shards.size()).put("healthy", healthy).put("degraded", degraded).put("unavailable", unavailable);
        result.put("clusterHealth", clusterHealth(views, shardViews));
        ArrayNode events = result.putArray("events");
        for (EventRow event : current.events) {
            events.addObject().put("ts", event.ts).put("nodeId", event.nodeId).put("level", event.level)
                    .put("event", event.event).put("message", event.message);
        }
        return result;
    }

    ObjectNode shards() {
        Snapshot current = snapshot;
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK");
        if (current.collectedAt > 0) result.put("collectedAt", current.collectedAt);
        ArrayNode shards = result.putArray("shards");
        for (ShardRow row : current.shards) {
            ObjectNode shard = shards.addObject();
            shard.put("graphId", row.graphId);
            shard.put("graph", row.graph);
            shard.put("partId", row.partId);
            shard.put("leaderId", row.leaderId);
            shard.put("term", row.term);
            shard.put("appliedIndex", row.applied);
            ArrayNode members = shard.putArray("members");
            row.members.forEach(members::add);
            shard.put("state", row.state);
        }
        return result;
    }

    /**
     * 服务器趋势。1h 读 15 秒原始点；1d 读 5 分钟汇总；7d 把 5 分钟汇总再并成 30 分钟。
     * 每台服务器一组按时间对齐的数组，缺的点是 null。汇总档同时给出每段的最大值。
     */
    ObjectNode series(ClusterState state, String range, long now) {
        Range window = Range.of(range);
        long from = now - window.spanMs;
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK")
                .put("range", window.key).put("stepMs", window.stepMs).put("from", from).put("to", now);
        Map<String, HostSeries> hosts = new LinkedHashMap<>();
        for (NodeSpec node : state.nodes()) {
            if (!node.joined() || hosts.containsKey(node.serverId())) continue;
            hosts.put(node.serverId(), new HostSeries(node.serverId(), hostName(state, node.serverId(), snapshotHostname(node.serverId(), state)),
                    state.server(node.serverId()).map(Server::host).orElse("")));
        }
        synchronized (readerLock) {
            try {
                if (window.rollup) {
                    try (PreparedStatement statement = reader.prepareStatement("""
                            SELECT host, metric, (bucket_ms / ?) * ? AS slot, SUM(total), SUM(n), MAX(peak)
                            FROM host_rollup WHERE bucket_ms >= ? GROUP BY host, metric, slot""")) {
                        statement.setLong(1, window.stepMs);
                        statement.setLong(2, window.stepMs);
                        statement.setLong(3, from - from % window.stepMs);
                        try (ResultSet rows = statement.executeQuery()) {
                            while (rows.next()) {
                                HostSeries host = hosts.get(rows.getString(1));
                                long n = rows.getLong(5);
                                if (host == null || n <= 0) continue;
                                host.put(rows.getString(2), rows.getLong(3), rows.getDouble(4) / n, rows.getDouble(6));
                            }
                        }
                    }
                } else {
                    try (PreparedStatement statement = reader.prepareStatement("SELECT host, metric, ts_ms, value FROM host_point WHERE ts_ms >= ?")) {
                        statement.setLong(1, from);
                        try (ResultSet rows = statement.executeQuery()) {
                            while (rows.next()) {
                                HostSeries host = hosts.get(rows.getString(1));
                                if (host != null) host.put(rows.getString(2), rows.getLong(3), rows.getDouble(4), Double.NaN);
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new IllegalStateException("监控曲线读取失败：" + ex.getMessage(), ex);
            }
        }
        ArrayNode out = result.putArray("hosts");
        for (HostSeries host : hosts.values()) host.write(out.addObject(), window.rollup);
        return result;
    }

    synchronized void purge(long now) {
        try {
            connection.setAutoCommit(false);
            purgeWithin(now);
            connection.commit();
        } catch (SQLException ex) {
            rollback();
            throw new IllegalStateException("监控记录清理失败：" + ex.getMessage(), ex);
        } finally {
            enableAutoCommit();
        }
        try { snapshot = load(); }
        catch (SQLException ex) { throw new IllegalStateException("监控记录读取失败：" + ex.getMessage(), ex); }
    }

    int rows(String table) {
        if (!Set.of("host_point", "host_rollup", "event_log", "node_latest").contains(table)) throw new IllegalArgumentException(table);
        synchronized (this) {
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
                return rows.next() ? rows.getInt(1) : 0;
            } catch (SQLException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    @Override public void close() {
        synchronized (readerLock) {
            if (reader != null) {
                try { reader.close(); }
                catch (SQLException ignored) { }
                reader = null;
            }
        }
        synchronized (this) {
            if (connection == null) return;
            try (Statement statement = connection.createStatement()) {
                query(statement, "PRAGMA wal_checkpoint(TRUNCATE)");
            } catch (SQLException ignored) {
                /* 关闭时尽量收掉 WAL，失败也要放开文件锁。 */
            }
            try { connection.close(); }
            catch (SQLException ignored) { }
            connection = null;
        }
    }

    private void purgeWithin(long now) throws SQLException {
        try (PreparedStatement points = connection.prepareStatement("DELETE FROM host_point WHERE ts_ms < ?");
             PreparedStatement rollups = connection.prepareStatement("DELETE FROM host_rollup WHERE bucket_ms < ?");
             PreparedStatement events = connection.prepareStatement("DELETE FROM event_log WHERE ts_ms < ?")) {
            points.setLong(1, now - POINT_RETENTION_MS);
            rollups.setLong(1, now - RETENTION_MS);
            events.setLong(1, now - RETENTION_MS);
            points.executeUpdate();
            rollups.executeUpdate();
            events.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            query(statement, "PRAGMA incremental_vacuum");
        }
        meta("purge_at", Long.toString(now));
    }

    /** 同一台服务器上的 Compute 和 Storage 报的是同一份机器指标：每项先取 Storage，没有再取 Compute。 */
    private void recordHosts(ClusterState state, Map<String, ObjectNode> fresh, long now) throws SQLException {
        Map<String, Map<String, Double>> hosts = new LinkedHashMap<>();
        for (Role role : List.of(Role.STORAGE, Role.COMPUTE)) {
            for (NodeSpec node : state.nodes(role)) {
                ObjectNode payload = fresh.get(node.id());
                if (payload == null) continue;
                Map<String, Double> host = hosts.computeIfAbsent(node.serverId(), ignored -> new LinkedHashMap<>());
                hostMetrics(payload).forEach(host::putIfAbsent);
            }
        }
        if (hosts.isEmpty()) return;
        long bucket = now - now % ROLLUP_MS;
        try (PreparedStatement point = connection.prepareStatement("INSERT OR REPLACE INTO host_point (host, metric, ts_ms, value) VALUES (?, ?, ?, ?)");
             PreparedStatement rollup = connection.prepareStatement("""
                     INSERT INTO host_rollup (host, metric, bucket_ms, n, total, peak) VALUES (?, ?, ?, 1, ?, ?)
                     ON CONFLICT(host, metric, bucket_ms) DO UPDATE SET
                       n=n+1, total=total+excluded.total, peak=max(peak, excluded.peak)""")) {
            for (var host : hosts.entrySet()) {
                for (var metric : host.getValue().entrySet()) {
                    point.setString(1, host.getKey());
                    point.setString(2, metric.getKey());
                    point.setLong(3, now);
                    point.setDouble(4, metric.getValue());
                    point.addBatch();
                    rollup.setString(1, host.getKey());
                    rollup.setString(2, metric.getKey());
                    rollup.setLong(3, bucket);
                    rollup.setDouble(4, metric.getValue());
                    rollup.setDouble(5, metric.getValue());
                    rollup.addBatch();
                }
            }
            point.executeBatch();
            rollup.executeBatch();
        }
    }

    /** 从一个节点的采集结果里取机器指标。拿不到的项不写，不用 0 冒充。 */
    static Map<String, Double> hostMetrics(ObjectNode payload) {
        Map<String, Double> out = new LinkedHashMap<>();
        ratio(out, "cpu", number(payload, "systemCpuLoad"));
        double memory = number(payload, "systemMemoryBytes");
        double available = number(payload, "memAvailableBytes");
        if (Double.isNaN(available)) available = number(payload, "systemFreeMemoryBytes");
        if (memory > 0 && !Double.isNaN(available)) ratio(out, "memory", 1 - available / memory);
        double disk = number(payload, "diskTotalBytes");
        double usable = number(payload, "diskUsableBytes");
        if (disk > 0 && !Double.isNaN(usable)) ratio(out, "diskUsed", 1 - usable / disk);
        ratio(out, "diskBusy", number(payload, "diskBusy"));
        rate(out, "diskRead", number(payload, "diskReadBytesPerSec"));
        rate(out, "diskWrite", number(payload, "diskWriteBytesPerSec"));
        rate(out, "netIn", number(payload, "netInBytesPerSec"));
        rate(out, "netOut", number(payload, "netOutBytesPerSec"));
        return out;
    }

    private static double number(ObjectNode payload, String field) {
        JsonNode value = payload.get(field);
        if (value == null || !value.isNumber()) return Double.NaN;
        double number = value.asDouble();
        return Double.isFinite(number) && number >= 0 ? number : Double.NaN;
    }

    private static void ratio(Map<String, Double> out, String key, double value) {
        if (!Double.isNaN(value)) out.put(key, Math.max(0, Math.min(1, value)));
    }

    private static void rate(Map<String, Double> out, String key, double value) {
        if (!Double.isNaN(value)) out.put(key, value);
    }

    /**
     * 分片状态以 Storage 上报为准。新版 Storage 不是每轮都报分片：没报的轮次里，分片保持上次的状态，
     * 只在副本节点不可达时往坏处调；节点状态一变，采集端会马上补一轮分片。
     */
    private List<ShardView> mergeShards(Map<String, OpsMonitor.Attempt> attempts, Map<String, String> states, long now) throws SQLException {
        Map<String, ShardAcc> fresh = new LinkedHashMap<>();
        Set<String> reported = new LinkedHashSet<>();
        for (var entry : attempts.entrySet()) {
            if (!"REACHABLE".equals(states.get(entry.getKey()))) continue;
            ArrayNode list = entry.getValue().shardList();
            if (list == null) continue;
            reported.add(entry.getKey());
            for (JsonNode shard : list) {
                if (!shard.isObject() || shard.path("graphId").asLong() <= 0) continue;
                String key = shard.path("graphId").asLong() + "/" + shard.path("partId").asInt();
                ShardAcc acc = fresh.computeIfAbsent(key, ignored -> new ShardAcc(shard.path("graphId").asLong(), shard.path("partId").asInt()));
                String graph = shard.path("graph").asText("");
                if (acc.graph.isBlank() && !graph.isBlank()) acc.graph = graph;
                long term = shard.path("term").asLong(-1);
                if (term > acc.term) {
                    acc.term = term;
                    acc.applied = shard.path("appliedIndex").asLong(acc.applied);
                    if (!shard.path("leaderId").asText("").isBlank()) acc.leaderId = shard.path("leaderId").asText();
                }
                if (shard.path("leader").asBoolean(false) && !shard.path("leaderId").asText("").isBlank()) acc.leaderId = shard.path("leaderId").asText();
                if (shard.path("appliedIndex").asLong(-1) > acc.applied) acc.applied = shard.path("appliedIndex").asLong();
                for (JsonNode member : shard.path("members")) if (!member.asText("").isBlank()) acc.members.add(member.asText());
            }
        }
        Map<String, ShardRow> previous = new LinkedHashMap<>();
        for (ShardRow row : snapshot.shards) previous.put(row.key(), row);
        List<ShardView> views = new ArrayList<>();
        for (var entry : fresh.entrySet()) {
            ShardAcc acc = entry.getValue();
            String state = shardState(acc, states);
            ShardRow before = previous.remove(entry.getKey());
            if (before == null) {
                if (!"HEALTHY".equals(state)) event(now, entry.getKey(), level(state), "SHARD", shardText(acc, state, states));
            } else if (!before.state.equals(state)) {
                event(now, entry.getKey(), level(state), "SHARD", shardText(acc, state, states));
            }
            upsertShard(entry.getKey(), acc, state, now);
            views.add(new ShardView(state));
        }
        for (var entry : previous.entrySet()) {
            ShardRow old = entry.getValue();
            String label = shardLabel(old.graph, old.graphId, old.partId);
            boolean covered = !old.members.isEmpty() && reported.containsAll(old.members);
            boolean silent = old.members.stream().anyMatch(member -> "REACHABLE".equals(states.get(member)) && !reported.contains(member));
            if (covered) {
                event(now, entry.getKey(), "INFO", "SHARD_GONE", label + " 已不再由存活节点上报");
                try (PreparedStatement delete = connection.prepareStatement("DELETE FROM shard_latest WHERE shard_key=?")) {
                    delete.setString(1, entry.getKey());
                    delete.executeUpdate();
                }
                continue;
            }
            String state;
            String text;
            if (silent) {
                // 有副本节点可达只是这轮没报分片，不能据此判定分片丢了。
                boolean missing = old.members.stream().anyMatch(member -> !"REACHABLE".equals(states.get(member)));
                state = missing && "HEALTHY".equals(old.state) ? "DEGRADED" : old.state;
                text = label + " 缺副本：" + String.join(", ", old.members.stream().filter(member -> !"REACHABLE".equals(states.get(member))).toList());
            } else {
                state = "UNAVAILABLE";
                text = label + " 没有存活副本上报";
            }
            if (!state.equals(old.state)) {
                event(now, entry.getKey(), level(state), "SHARD", text);
                try (PreparedStatement update = connection.prepareStatement("UPDATE shard_latest SET state=?, updated_at=? WHERE shard_key=?")) {
                    update.setString(1, state);
                    update.setLong(2, now);
                    update.setString(3, entry.getKey());
                    update.executeUpdate();
                }
            }
            views.add(new ShardView(state));
        }
        return views;
    }

    private static String shardState(ShardAcc acc, Map<String, String> states) {
        if (acc.leaderId.isBlank()) return "UNAVAILABLE";
        boolean missing = acc.members.stream().anyMatch(member -> !"REACHABLE".equals(states.get(member)));
        return missing ? "DEGRADED" : "HEALTHY";
    }

    private void upsertShard(String key, ShardAcc acc, String state, long now) throws SQLException {
        ArrayNode members = ClusterStore.JSON.createArrayNode();
        acc.members.stream().sorted().forEach(members::add);
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO shard_latest (shard_key, graph_id, graph_name, part_id, leader_id, term, applied_index, members, state, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(shard_key) DO UPDATE SET
                  graph_id=excluded.graph_id, graph_name=excluded.graph_name, part_id=excluded.part_id,
                  leader_id=excluded.leader_id, term=excluded.term, applied_index=excluded.applied_index,
                  members=excluded.members, state=excluded.state, updated_at=excluded.updated_at""")) {
            statement.setString(1, key);
            statement.setLong(2, acc.graphId);
            statement.setString(3, acc.graph);
            statement.setInt(4, acc.partId);
            statement.setString(5, acc.leaderId);
            statement.setLong(6, Math.max(0, acc.term));
            statement.setLong(7, Math.max(0, acc.applied));
            statement.setString(8, members.toString());
            statement.setString(9, state);
            statement.setLong(10, now);
            statement.executeUpdate();
        }
    }

    private void upsertNode(NodeSpec node, String state, String health, long now, long successAt, String message, ObjectNode payload) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO node_latest (node_id, role, state, health, collected_at, success_at, message, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                  role=excluded.role, state=excluded.state, health=excluded.health, collected_at=excluded.collected_at,
                  success_at=excluded.success_at, message=excluded.message, payload=excluded.payload""")) {
            statement.setString(1, node.id());
            statement.setString(2, node.role().name());
            statement.setString(3, state);
            statement.setString(4, health);
            statement.setLong(5, now);
            statement.setLong(6, successAt);
            statement.setString(7, message);
            statement.setString(8, payload.toString());
            statement.executeUpdate();
        }
    }

    private void event(long now, String nodeId, String level, String name, String message) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO event_log (ts_ms, node_id, level, event, message) VALUES (?, ?, ?, ?, ?)")) {
            statement.setLong(1, now);
            statement.setString(2, nodeId == null ? "" : nodeId);
            statement.setString(3, level);
            statement.setString(4, name);
            statement.setString(5, trim(message));
            statement.executeUpdate();
        }
    }

    /** 从库里重建页面用的快照。只在写连接上、持有本对象锁时调用。 */
    private Snapshot load() throws SQLException {
        Map<String, NodeRow> nodes = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT node_id, state, health, success_at, message, payload FROM node_latest")) {
            while (rows.next()) {
                nodes.put(rows.getString(1), new NodeRow(rows.getString(2), rows.getString(3), rows.getLong(4), rows.getString(5), payload(rows.getString(6))));
            }
        }
        List<ShardRow> shards = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT graph_id, graph_name, part_id, leader_id, term, applied_index, members, state FROM shard_latest ORDER BY graph_name, part_id")) {
            while (rows.next()) {
                List<String> members = new ArrayList<>();
                for (JsonNode member : members(rows.getString(7))) members.add(member.asText());
                shards.add(new ShardRow(rows.getLong(1), rows.getString(2), rows.getInt(3), rows.getString(4), rows.getLong(5), rows.getLong(6),
                        List.copyOf(members), rows.getString(8)));
            }
        }
        List<EventRow> events = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT ts_ms, node_id, level, event, message FROM event_log ORDER BY id DESC LIMIT " + EVENTS_SHOWN)) {
            while (rows.next()) events.add(new EventRow(rows.getLong(1), rows.getString(2), rows.getString(3), rows.getString(4), rows.getString(5)));
        }
        return new Snapshot(longMeta("last_round"), Map.copyOf(nodes), List.copyOf(shards), List.copyOf(events));
    }

    private String meta(String key) {
        try (PreparedStatement statement = connection.prepareStatement("SELECT value FROM meta WHERE key=?")) {
            statement.setString(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : "";
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private long longMeta(String key) {
        try { return Long.parseLong(meta(key)); }
        catch (NumberFormatException ex) { return 0; }
    }

    private void meta(String key, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    private void rollback() {
        try { connection.rollback(); }
        catch (SQLException ignored) { }
    }

    private void enableAutoCommit() {
        try { if (connection != null) connection.setAutoCommit(true); }
        catch (SQLException ignored) { }
    }

    static String health(String state, long now, long successAt, ObjectNode payload) {
        if ("UNKNOWN".equals(state)) return "UNKNOWN";
        String disk = diskHealth(payload);
        if ("REACHABLE".equals(state)) return worse(ageHealth(now, successAt), disk);
        if (successAt <= 0 || now - successAt >= BAD_MS) return "BAD";
        return worse("CONCERNING", disk);
    }

    static String clusterHealth(List<NodeView> nodes, List<ShardView> shards) {
        if (nodes.isEmpty()) return "UNKNOWN";
        if (nodes.stream().allMatch(node -> "UNKNOWN".equals(node.state))) return "UNKNOWN";
        List<NodeView> storage = nodes.stream().filter(node -> "STORAGE".equals(node.role)).toList();
        List<NodeView> compute = nodes.stream().filter(node -> "COMPUTE".equals(node.role)).toList();
        String lamp = "GOOD";
        if (!storage.isEmpty() && storage.stream().allMatch(node -> "BAD".equals(node.health))) lamp = "BAD";
        else if (storage.stream().anyMatch(OpsStore::needsAttention)) lamp = "CONCERNING";
        if (!compute.isEmpty() && compute.stream().allMatch(node -> "BAD".equals(node.health))) lamp = "BAD";
        else if (compute.stream().anyMatch(OpsStore::needsAttention)) lamp = worse(lamp, "CONCERNING");
        if (shards.stream().anyMatch(shard -> !"HEALTHY".equals(shard.state))) lamp = worse(lamp, "CONCERNING");
        return lamp;
    }

    private static boolean needsAttention(NodeView node) {
        if ("UNKNOWN".equals(node.state) || "UNKNOWN".equals(node.health)) return !"UNKNOWN".equals(node.state);
        return !"REACHABLE".equals(node.state) || !"GOOD".equals(node.health);
    }

    private static String ageHealth(long now, long successAt) {
        if (successAt <= 0) return "BAD";
        long age = now - successAt;
        if (age >= BAD_MS) return "BAD";
        if (age >= CONCERN_MS) return "CONCERNING";
        return "GOOD";
    }

    private static String diskHealth(ObjectNode payload) {
        if (payload == null) return "GOOD";
        double total = payload.path("diskTotalBytes").asDouble(-1);
        double usable = payload.path("diskUsableBytes").asDouble(-1);
        String space = "GOOD";
        if (total > 0 && usable >= 0) {
            double ratio = usable / total;
            if (ratio < DISK_BAD) space = "BAD";
            else if (ratio < DISK_CONCERN) space = "CONCERNING";
        }
        if (payload.path("diskBusy").asDouble(-1) >= BUSY_CONCERN) return worse(space, "CONCERNING");
        return space;
    }

    private static String worse(String left, String right) {
        return rank(left) >= rank(right) ? left : right;
    }

    private static int rank(String health) {
        return switch (health) {
            case "BAD", "UNAVAILABLE" -> 3;
            case "CONCERNING", "DEGRADED" -> 2;
            case "GOOD", "HEALTHY", "REACHABLE" -> 1;
            default -> 0;
        };
    }

    private static String level(String health) {
        return switch (health) {
            case "BAD", "UNAVAILABLE" -> "ERROR";
            case "CONCERNING", "DEGRADED" -> "WARN";
            default -> "INFO";
        };
    }

    private static String label(String health) {
        return switch (health) {
            case "GOOD", "HEALTHY", "REACHABLE" -> "良好";
            case "CONCERNING", "DEGRADED" -> "关注";
            case "BAD", "UNAVAILABLE" -> "故障";
            default -> "等待采集";
        };
    }

    private static String nodeText(String nodeId, String state, String health, String message, ObjectNode payload) {
        String reason = diskReason(payload);
        if (reason.isBlank() && !"REACHABLE".equals(state) && message != null && !message.isBlank()) reason = message;
        if (reason.isBlank() && "BAD".equals(health)) reason = "超过 150 秒没有采集成功";
        String text = nodeId + ("REACHABLE".equals(state) ? " 可达" : " 不可达") + "，健康为" + label(health);
        return reason.isBlank() ? text : text + "（" + reason + "）";
    }

    private static String shardText(ShardAcc acc, String state, Map<String, String> states) {
        String label = shardLabel(acc.graph, acc.graphId, acc.partId);
        if ("UNAVAILABLE".equals(state)) return label + " 没有 Leader";
        String missing = acc.members.stream().filter(member -> !"REACHABLE".equals(states.get(member))).reduce((a, b) -> a + ", " + b).orElse("");
        if ("DEGRADED".equals(state)) return label + " 缺副本" + (missing.isBlank() ? "" : "：" + missing);
        return label + " 已恢复";
    }

    private static String shardLabel(String graph, long graphId, int partId) {
        return (graph == null || graph.isBlank() ? Long.toString(graphId) : graph) + "/" + partId;
    }

    private static String diskReason(ObjectNode payload) {
        if (payload == null) return "";
        double total = payload.path("diskTotalBytes").asDouble(-1);
        double usable = payload.path("diskUsableBytes").asDouble(-1);
        if (total > 0 && usable >= 0) {
            double ratio = usable / total;
            if (ratio < DISK_BAD) return "数据盘剩余不足 10%";
            if (ratio < DISK_CONCERN) return "数据盘剩余不足 20%";
        }
        if (payload.path("diskBusy").asDouble(-1) >= BUSY_CONCERN) return "数据盘繁忙";
        return "";
    }

    private static ObjectNode storedPayload(ObjectNode metrics) {
        ObjectNode copy = metrics == null ? ClusterStore.JSON.createObjectNode() : metrics.deepCopy();
        copy.remove("shards");
        copy.remove("code");
        return copy;
    }

    private static ObjectNode payload(String json) {
        try {
            JsonNode node = ClusterStore.JSON.readTree(json);
            return node instanceof ObjectNode object ? object : ClusterStore.JSON.createObjectNode();
        } catch (IOException ex) {
            return ClusterStore.JSON.createObjectNode();
        }
    }

    private static ArrayNode members(String json) {
        try {
            JsonNode node = ClusterStore.JSON.readTree(json);
            return node instanceof ArrayNode array ? array : ClusterStore.JSON.createArrayNode();
        } catch (IOException ex) {
            return ClusterStore.JSON.createArrayNode();
        }
    }

    private static String host(ClusterState state, NodeSpec node) {
        return state.server(node.serverId()).map(Server::host).orElse("");
    }

    /** 服务器显示名：添加服务器时探测到的主机名，其次节点上报的主机名，最后是地址。 */
    private static String hostName(ClusterState state, String serverId, String reported) {
        String probed = state.server(serverId).map(Server::hostname).orElse("");
        if (!probed.isBlank()) return probed;
        if (reported != null && !reported.isBlank()) return reported;
        return state.server(serverId).map(Server::host).orElse(serverId);
    }

    private String snapshotHostname(String serverId, ClusterState state) {
        Snapshot current = snapshot;
        for (NodeSpec node : state.nodes()) {
            if (!node.serverId().equals(serverId)) continue;
            NodeRow row = current.nodes.get(node.id());
            if (row != null && !row.payload.path("hostname").asText("").isBlank()) return row.payload.path("hostname").asText();
        }
        return "";
    }

    private static String trim(String message) {
        if (message == null) return "";
        String text = message.replace('\n', ' ').trim();
        return text.length() > 300 ? text.substring(0, 300) : text;
    }

    private static void query(Statement statement, String sql) throws SQLException {
        if (!statement.execute(sql)) return;
        try (ResultSet rows = statement.getResultSet()) {
            while (rows != null && rows.next()) { /* PRAGMA 会返回结果行。 */ }
        }
    }

    /** 趋势的时间范围。1h 用原始点；1d、7d 用汇总。 */
    record Range(String key, long spanMs, long stepMs, boolean rollup) {
        static Range of(String key) {
            return switch (key == null ? "" : key) {
                case "1d" -> new Range("1d", TimeUnit.DAYS.toMillis(1), ROLLUP_MS, true);
                case "7d" -> new Range("7d", TimeUnit.DAYS.toMillis(7), TimeUnit.MINUTES.toMillis(30), true);
                default -> new Range("1h", TimeUnit.HOURS.toMillis(1), OpsMonitor.INTERVAL.toMillis(), false);
            };
        }
    }

    /** 一台服务器的曲线，按时间对齐。 */
    private static final class HostSeries {
        final String id;
        final String name;
        final String address;
        final TreeMap<Long, double[]> average = new TreeMap<>();
        final Map<Long, double[]> peak = new HashMap<>();

        HostSeries(String id, String name, String address) {
            this.id = id;
            this.name = name;
            this.address = address;
        }

        void put(String metric, long ts, double value, double max) {
            int index = METRICS.indexOf(metric);
            if (index < 0) return;
            average.computeIfAbsent(ts, ignored -> blank())[index] = value;
            if (!Double.isNaN(max)) peak.computeIfAbsent(ts, ignored -> blank())[index] = max;
        }

        void write(ObjectNode out, boolean withPeak) {
            out.put("id", id).put("name", name).put("address", address);
            ArrayNode ts = out.putArray("ts");
            average.keySet().forEach(ts::add);
            ObjectNode avg = out.putObject("avg");
            ObjectNode max = withPeak ? out.putObject("max") : null;
            for (int i = 0; i < METRICS.size(); i++) {
                String metric = METRICS.get(i);
                ArrayNode values = avg.putArray(metric);
                ArrayNode peaks = max == null ? null : max.putArray(metric);
                for (Long key : average.keySet()) {
                    add(values, metric, average.get(key)[i]);
                    if (peaks != null) add(peaks, metric, peak.getOrDefault(key, BLANK)[i]);
                }
            }
        }

        private static void add(ArrayNode values, String metric, double value) {
            if (Double.isNaN(value)) values.addNull();
            else if (RATES.contains(metric)) values.add(Math.round(value));
            else values.add(Math.round(value * 10000) / 10000.0);
        }

        private static final double[] BLANK = blank();

        private static double[] blank() {
            double[] values = new double[METRICS.size()];
            Arrays.fill(values, Double.NaN);
            return values;
        }
    }

    private record NodeRow(String state, String health, long successAt, String message, ObjectNode payload) { }
    private record ShardRow(long graphId, String graph, int partId, String leaderId, long term, long applied, List<String> members, String state) {
        String key() { return graphId + "/" + partId; }
    }
    private record EventRow(long ts, String nodeId, String level, String event, String message) { }
    private record Snapshot(long collectedAt, Map<String, NodeRow> nodes, List<ShardRow> shards, List<EventRow> events) {
        static final Snapshot EMPTY = new Snapshot(0, Map.of(), List.of(), List.of());
    }
    record NodeView(String role, String state, String health) { }
    private record ShardView(String state) { }

    private static final class ShardAcc {
        final long graphId;
        final int partId;
        String graph = "";
        String leaderId = "";
        long term = -1;
        long applied = -1;
        final LinkedHashSet<String> members = new LinkedHashSet<>();
        ShardAcc(long graphId, int partId) { this.graphId = graphId; this.partId = partId; }
    }
}
