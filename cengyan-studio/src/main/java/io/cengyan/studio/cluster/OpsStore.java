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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Studio 本机的监控库 {@code data/ops.db}。页面只读这里。
 * 样本和事件保留 7 天，节点当前行一直留到节点移出集群。
 */
final class OpsStore implements AutoCloseable {
    static final long RETENTION_MS = TimeUnit.DAYS.toMillis(7);
    static final long PURGE_EVERY_MS = TimeUnit.HOURS.toMillis(1);
    static final long CONCERN_MS = TimeUnit.SECONDS.toMillis(75);
    static final long BAD_MS = TimeUnit.SECONDS.toMillis(150);
    private static final double DISK_BAD = 0.10;
    private static final double DISK_CONCERN = 0.20;
    private static final double BUSY_CONCERN = 0.90;

    private Connection connection;

    OpsStore(Path dataDir) throws IOException {
        Path db = dataDir.resolve("ops.db");
        Files.createDirectories(dataDir);
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath().toString().replace('\\', '/'));
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
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS sample (
                          node_id TEXT NOT NULL,
                          ts_ms INTEGER NOT NULL,
                          payload TEXT NOT NULL,
                          PRIMARY KEY (node_id, ts_ms)
                        )""");
                statement.execute("CREATE INDEX IF NOT EXISTS sample_ts ON sample(ts_ms)");
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
        } catch (ClassNotFoundException ex) {
            throw new IOException("找不到 SQLite 驱动", ex);
        } catch (SQLException ex) {
            throw new IOException("监控库打开失败：" + ex.getMessage(), ex);
        }
        ClusterStore.restrict(db, false);
        purge(System.currentTimeMillis());
    }

    synchronized void record(ClusterState state, Map<String, OpsMonitor.Attempt> attempts, long now) {
        try {
            connection.setAutoCommit(false);
            Map<String, String> states = new LinkedHashMap<>();
            List<NodeView> views = new ArrayList<>();
            for (NodeSpec node : state.nodes()) {
                if (!node.joined()) continue;
                OpsMonitor.Attempt attempt = attempts.get(node.id());
                if (attempt == null) attempt = OpsMonitor.Attempt.fail("未采集");
                Latest previous = latest(node.id());
                boolean reachable = attempt.reachable();
                long successAt = reachable ? now : previous == null ? 0 : previous.successAt;
                String nodeState = reachable ? "REACHABLE" : "UNAVAILABLE";
                ObjectNode payload = reachable || previous == null ? storedPayload(attempt.metrics()) : payload(previous.payload);
                String health = health(nodeState, now, successAt, payload);
                String message = trim(attempt.message());
                upsertNode(node, nodeState, health, now, successAt, message, payload);
                if (reachable) insertSample(node.id(), now, payload);
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
            try (PreparedStatement existing = connection.prepareStatement("SELECT node_id FROM node_latest");
                 ResultSet rows = existing.executeQuery()) {
                List<String> gone = new ArrayList<>();
                while (rows.next()) {
                    String id = rows.getString(1);
                    if (!states.containsKey(id)) gone.add(id);
                }
                for (String id : gone) {
                    try (PreparedStatement delete = connection.prepareStatement("DELETE FROM node_latest WHERE node_id=?")) {
                        delete.setString(1, id);
                        delete.executeUpdate();
                    }
                    event(now, id, "INFO", "NODE_REMOVED", id + " 已移出集群，停止采集");
                }
            }
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
    }

    synchronized ObjectNode overview(ClusterState state, long now) {
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK");
        long collectedAt = longMeta("last_round");
        if (collectedAt > 0) result.put("collectedAt", collectedAt);
        ArrayNode compute = result.putArray("computeNodes");
        ArrayNode storage = result.putArray("storageNodes");
        List<NodeView> views = new ArrayList<>();
        for (NodeSpec node : state.nodes()) {
            if (!node.joined()) continue;
            Latest row = latest(node.id());
            String nodeState = row == null ? "UNKNOWN" : row.state;
            ObjectNode payload = row == null ? ClusterStore.JSON.createObjectNode() : payload(row.payload);
            long successAt = row == null ? 0 : row.successAt;
            String health = row == null ? "UNKNOWN" : health(nodeState, now, successAt, payload);
            ObjectNode item = ClusterStore.JSON.createObjectNode();
            item.setAll(payload);
            item.remove("shards");
            item.put("nodeId", node.id());
            item.put("role", node.role().name());
            item.put("host", host(state, node));
            String hostname = item.path("hostname").asText("");
            if (hostname.isBlank()) hostname = state.server(node.serverId()).map(Server::hostname).filter(name -> !name.isBlank()).orElse(host(state, node));
            item.put("hostname", hostname);
            item.put("state", nodeState);
            item.put("health", health);
            if (successAt > 0) item.put("successAt", successAt);
            if (row != null && !row.message.isBlank()) item.put("message", row.message);
            if (node.role() == Role.COMPUTE) compute.add(item);
            else storage.add(item);
            views.add(new NodeView(node.role().name(), nodeState, health));
        }
        ArrayNode shards = result.putArray("shards");
        List<ShardView> shardViews = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT graph_id, graph_name, part_id, leader_id, term, applied_index, members, state FROM shard_latest ORDER BY graph_name, part_id")) {
            while (rows.next()) {
                ObjectNode shard = shards.addObject();
                shard.put("graphId", rows.getLong(1));
                shard.put("graph", rows.getString(2));
                shard.put("partId", rows.getInt(3));
                shard.put("leaderId", rows.getString(4));
                shard.put("term", rows.getLong(5));
                shard.put("appliedIndex", rows.getLong(6));
                shard.set("members", members(rows.getString(7)));
                shard.put("state", rows.getString(8));
                shardViews.add(new ShardView(rows.getString(8)));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("监控记录读取失败：" + ex.getMessage(), ex);
        }
        result.put("clusterHealth", clusterHealth(views, shardViews));
        ArrayNode events = result.putArray("events");
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT ts_ms, node_id, level, event, message FROM event_log ORDER BY id DESC LIMIT 50")) {
            while (rows.next()) {
                events.addObject().put("ts", rows.getLong(1)).put("nodeId", rows.getString(2))
                        .put("level", rows.getString(3)).put("event", rows.getString(4)).put("message", rows.getString(5));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("监控记录读取失败：" + ex.getMessage(), ex);
        }
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
    }

    int rows(String table) {
        if (!table.equals("sample") && !table.equals("event_log") && !table.equals("node_latest")) throw new IllegalArgumentException(table);
        synchronized (this) {
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
                return rows.next() ? rows.getInt(1) : 0;
            } catch (SQLException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    @Override public synchronized void close() {
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

    private void purgeWithin(long now) throws SQLException {
        long cutoff = now - RETENTION_MS;
        try (PreparedStatement samples = connection.prepareStatement("DELETE FROM sample WHERE ts_ms < ?");
             PreparedStatement events = connection.prepareStatement("DELETE FROM event_log WHERE ts_ms < ?")) {
            samples.setLong(1, cutoff);
            events.setLong(1, cutoff);
            samples.executeUpdate();
            events.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            query(statement, "PRAGMA incremental_vacuum");
        }
        meta("purge_at", Long.toString(now));
    }

    private List<ShardView> mergeShards(Map<String, OpsMonitor.Attempt> attempts, Map<String, String> states, long now) throws SQLException {
        Map<String, ShardAcc> fresh = new LinkedHashMap<>();
        for (var entry : attempts.entrySet()) {
            if (!"REACHABLE".equals(states.get(entry.getKey())) || entry.getValue().metrics() == null) continue;
            for (JsonNode shard : entry.getValue().metrics().path("shards")) {
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
        Map<String, StoredShard> previous = storedShards();
        List<ShardView> views = new ArrayList<>();
        for (var entry : fresh.entrySet()) {
            ShardAcc acc = entry.getValue();
            String state = shardState(acc, states);
            StoredShard before = previous.remove(entry.getKey());
            if (before == null) {
                if (!"HEALTHY".equals(state)) event(now, entry.getKey(), level(state), "SHARD", shardText(acc, state, states));
            } else if (!before.state.equals(state)) {
                event(now, entry.getKey(), level(state), "SHARD", shardText(acc, state, states));
            }
            upsertShard(entry.getKey(), acc, state, now);
            views.add(new ShardView(state));
        }
        for (var entry : previous.entrySet()) {
            StoredShard old = entry.getValue();
            boolean covered = !old.members.isEmpty() && old.members.stream().allMatch(member -> "REACHABLE".equals(states.get(member)));
            if (covered) {
                event(now, entry.getKey(), "INFO", "SHARD_GONE", shardLabel(old.graph, old.graphId, old.partId) + " 已不再由存活节点上报");
                try (PreparedStatement delete = connection.prepareStatement("DELETE FROM shard_latest WHERE shard_key=?")) {
                    delete.setString(1, entry.getKey());
                    delete.executeUpdate();
                }
            } else {
                if (!"UNAVAILABLE".equals(old.state)) {
                    event(now, entry.getKey(), "ERROR", "SHARD", shardLabel(old.graph, old.graphId, old.partId) + " 没有存活副本上报");
                }
                try (PreparedStatement update = connection.prepareStatement("UPDATE shard_latest SET state=?, updated_at=? WHERE shard_key=?")) {
                    update.setString(1, "UNAVAILABLE");
                    update.setLong(2, now);
                    update.setString(3, entry.getKey());
                    update.executeUpdate();
                }
                views.add(new ShardView("UNAVAILABLE"));
            }
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

    private Map<String, StoredShard> storedShards() throws SQLException {
        Map<String, StoredShard> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT shard_key, graph_id, graph_name, part_id, members, state FROM shard_latest")) {
            while (rows.next()) {
                LinkedHashSet<String> members = new LinkedHashSet<>();
                for (JsonNode member : members(rows.getString(5))) members.add(member.asText());
                result.put(rows.getString(1), new StoredShard(rows.getLong(2), rows.getString(3), rows.getInt(4), members, rows.getString(6)));
            }
        }
        return result;
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

    private void insertSample(String nodeId, long now, ObjectNode payload) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO sample (node_id, ts_ms, payload) VALUES (?, ?, ?)")) {
            statement.setString(1, nodeId);
            statement.setLong(2, now);
            statement.setString(3, payload.toString());
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

    private Latest latest(String nodeId) {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state, health, success_at, message, payload FROM node_latest WHERE node_id=?")) {
            statement.setString(1, nodeId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                return new Latest(rows.getString(1), rows.getString(2), rows.getLong(3), rows.getString(4), rows.getString(5));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("监控记录读取失败：" + ex.getMessage(), ex);
        }
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

    private record Latest(String state, String health, long successAt, String message, String payload) { }
    record NodeView(String role, String state, String health) { }
    private record ShardView(String state) { }
    private record StoredShard(long graphId, String graph, int partId, LinkedHashSet<String> members, String state) { }

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
