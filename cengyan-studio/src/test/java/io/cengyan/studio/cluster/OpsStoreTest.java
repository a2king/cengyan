package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.studio.cluster.ClusterState.NodeSpec;
import io.cengyan.studio.cluster.ClusterState.NodeStatus;
import io.cengyan.studio.cluster.ClusterState.Role;
import io.cengyan.studio.cluster.ClusterState.Server;
import io.cengyan.studio.cluster.ClusterState.ServerStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpsStoreTest {
    @TempDir Path tmp;

    @Test void oneStorageFailureKeepsThePageAndTheOtherNode() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        try (OpsStore store = new OpsStore(tmp)) {
            store.record(state, Map.of(
                    "storage-1", OpsMonitor.Attempt.ok(metrics(0.5)),
                    "storage-2", OpsMonitor.Attempt.ok(shardMetrics()),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.8))), now - 1000);
            store.record(state, Map.of(
                    "storage-1", OpsMonitor.Attempt.ok(shardMetrics()),
                    "storage-2", OpsMonitor.Attempt.fail("连接超时"),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.8))), now);
            ObjectNode overview = store.overview(state, now);
            assertEquals("OK", overview.path("code").asText());
            assertEquals("CONCERNING", overview.path("clusterHealth").asText());
            assertEquals(2, overview.path("storageNodes").size());
            assertEquals(1, overview.path("computeNodes").size());
            ObjectNode down = node(overview, "storageNodes", "storage-2");
            assertEquals("UNAVAILABLE", down.path("state").asText());
            assertEquals("CONCERNING", down.path("health").asText());
            assertEquals(0.2, down.path("systemCpuLoad").asDouble());
            ObjectNode shards = store.shards();
            assertEquals("DEGRADED", shards.path("shards").get(0).path("state").asText());
            assertEquals("storage-1", shards.path("shards").get(0).path("leaderId").asText());
            assertEquals(1, overview.path("shardSummary").path("degraded").asInt());
            assertTrue(overview.path("events").size() > 0);
        }
    }

    @Test void diskAndSilenceChangeHealthWithoutHidingTheNode() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        try (OpsStore store = new OpsStore(tmp)) {
            ObjectNode tight = metrics(0.1);
            tight.put("diskTotalBytes", 100);
            tight.put("diskUsableBytes", 5);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(tight), "storage-2", OpsMonitor.Attempt.ok(metrics(0.5)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.1))), now);
            ObjectNode overview = store.overview(state, now);
            assertEquals("BAD", node(overview, "storageNodes", "storage-1").path("health").asText());
            assertEquals("CONCERNING", overview.path("clusterHealth").asText());
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.5)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.5)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.1))), now - OpsStore.BAD_MS - 1);
            overview = store.overview(state, now);
            assertEquals("BAD", node(overview, "storageNodes", "storage-1").path("health").asText());
            assertEquals("storage-1", node(overview, "storageNodes", "storage-1").path("nodeId").asText());
        }
    }

    @Test void pointsRollupsAndEventsExpireOnTheirOwnSchedule() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        long old = now - TimeUnit.DAYS.toMillis(8);
        long yesterday = now - TimeUnit.HOURS.toMillis(30);
        try (OpsStore store = new OpsStore(tmp)) {
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), old);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.fail("连接超时"), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), old + 1000);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), yesterday);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now);
            assertTrue(store.rows("event_log") >= 1);
            store.purge(now);
            int perRound = OpsStore.hostMetrics(metrics(0.2)).size();
            assertEquals(perRound, store.rows("host_point"), "原始点只留 1 天，三个节点在同一台服务器上只记一份");
            assertEquals(perRound * 2, store.rows("host_rollup"), "5 分钟汇总留 7 天");
            assertEquals(3, store.rows("node_latest"));
            assertTrue(store.rows("event_log") >= 1);
            ObjectNode overview = store.overview(state, now);
            assertTrue(overview.path("events").size() > 0);
            for (var event : overview.path("events")) assertTrue(event.path("ts").asLong() >= now - TimeUnit.DAYS.toMillis(7));
        }
        try (OpsStore reopened = new OpsStore(tmp)) {
            assertEquals(3, reopened.rows("node_latest"));
            assertEquals("OK", reopened.overview(state, now).path("code").asText());
        }
    }

    @Test void probeFailureStillReturnsEveryJoinedNode() throws Exception {
        ClusterState state = cluster();
        OpsStore store = new OpsStore(tmp);
        try (OpsMonitor monitor = new OpsMonitor(null, (ignored, node) -> {
            throw new IllegalStateException("down " + node.id());
        }, store, null)) {
            monitor.collect(state);
            ObjectNode overview = store.overview(state, System.currentTimeMillis());
            assertEquals("OK", overview.path("code").asText());
            assertEquals(2, overview.path("storageNodes").size());
            assertEquals("UNAVAILABLE", node(overview, "storageNodes", "storage-2").path("state").asText());
            assertEquals("BAD", overview.path("clusterHealth").asText());
        }
    }

    @Test void seriesIsPerServerAndPrefersStorageForSharedHostMetrics() throws Exception {
        ClusterState state = cluster().withServer(new Server(
                "server-2", "10.0.0.9", 22, "", ServerStatus.READY, "", "", "", "", "db2", "", ""));
        state = state.withNode(new NodeSpec("storage-3", Role.STORAGE, "server-2", 2100, 2101, 0, "/data/storage-3",
                NodeStatus.RUNNING, "", true, "rel", "hash"));
        long now = System.currentTimeMillis();
        try (OpsStore store = new OpsStore(tmp)) {
            ObjectNode storage = metrics(0.5);
            storage.put("systemMemoryBytes", 1000).put("systemFreeMemoryBytes", 100).put("memAvailableBytes", 600);
            storage.put("diskReadBytesPerSec", 2048.0).put("diskBusy", 0.3);
            ObjectNode compute = metrics(0.9);
            compute.put("netInBytesPerSec", 512.0);
            for (int i = 0; i < 3; i++) {
                store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(storage), "storage-2", OpsMonitor.Attempt.ok(storage),
                        "compute-1", OpsMonitor.Attempt.ok(compute), "storage-3", OpsMonitor.Attempt.ok(metrics(0.1))),
                        now - 30_000 + i * 15_000L);
            }
            ObjectNode series = store.series(state, "1h", now);
            assertEquals("1h", series.path("range").asText());
            assertEquals(2, series.path("hosts").size(), "按服务器出曲线");
            var first = series.path("hosts").get(0);
            assertEquals("server-1", first.path("id").asText());
            assertEquals("db", first.path("name").asText());
            assertEquals(3, first.path("ts").size());
            assertEquals(0.5, first.path("avg").path("cpu").get(0).asDouble(), "同机的 CPU 取 Storage 上报的");
            assertEquals(0.4, first.path("avg").path("memory").get(0).asDouble(), 1e-9, "内存占用用 MemAvailable，不用 MemFree");
            assertEquals(0.2, first.path("avg").path("diskUsed").get(0).asDouble(), 1e-9);
            assertEquals(2048, first.path("avg").path("diskRead").get(0).asLong());
            assertEquals(512, first.path("avg").path("netIn").get(0).asLong(), "Storage 没报的项用同机 Compute 补上");
            assertTrue(first.path("avg").path("diskWrite").get(0).isNull(), "没有上报的指标是 null，不是 0");
            assertTrue(first.path("max").isMissingNode(), "原始点没有最大值");
            assertEquals(0.1, series.path("hosts").get(1).path("avg").path("cpu").get(0).asDouble());

            ObjectNode day = store.series(state, "1d", now);
            var rolled = day.path("hosts").get(0);
            assertTrue(rolled.path("ts").size() >= 1 && rolled.path("ts").size() <= 2, "15 秒点并进 5 分钟桶");
            assertEquals(0.5, rolled.path("avg").path("cpu").get(0).asDouble(), 1e-9);
            assertEquals(0.5, rolled.path("max").path("cpu").get(0).asDouble(), 1e-9);
            assertEquals(OpsStore.ROLLUP_MS, day.path("stepMs").asLong());
            assertEquals(TimeUnit.MINUTES.toMillis(30), store.series(state, "7d", now).path("stepMs").asLong());
        }
    }

    @Test void pageReadsTheSnapshotWhileARoundIsBeingWritten() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        try (OpsStore store = new OpsStore(tmp)) {
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now);
            var writer = Thread.ofVirtual().start(() -> {
                synchronized (store) {
                    try { Thread.sleep(1500); } catch (InterruptedException ignored) { }
                }
            });
            Thread.sleep(100);
            long started = System.nanoTime();
            assertEquals(3, store.overview(state, now).path("storageNodes").size() + store.overview(state, now).path("computeNodes").size());
            store.series(state, "1h", now);
            store.shards();
            assertTrue(System.nanoTime() - started < TimeUnit.MILLISECONDS.toNanos(1000), "页面读取不等采集写入");
            writer.join();
        }
    }

    @Test void storageThatSkipsShardsThisRoundKeepsTheShards() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        try (OpsStore store = new OpsStore(tmp)) {
            OpsMonitor.Attempt withShards = OpsMonitor.Attempt.ok(metrics(0.2)).withShards((com.fasterxml.jackson.databind.node.ArrayNode) shardMetrics().get("shards"));
            store.record(state, Map.of("storage-1", withShards, "storage-2", withShards, "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now - 30_000);
            assertEquals("HEALTHY", store.shards().path("shards").get(0).path("state").asText());
            int events = store.overview(state, now).path("events").size();
            // 新版 Storage 不是每轮都报分片：这一轮只有机器指标。
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now - 15_000);
            assertEquals(1, store.shards().path("shards").size(), "没报分片不等于分片消失");
            assertEquals("HEALTHY", store.shards().path("shards").get(0).path("state").asText());
            assertEquals(events, store.overview(state, now).path("events").size(), "不产生分片事件");
            // 一个副本节点不可达：即使这轮没取分片，也要标成缺副本。
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.fail("连接超时"),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now);
            assertEquals("DEGRADED", store.shards().path("shards").get(0).path("state").asText());
            assertEquals("CONCERNING", store.overview(state, now).path("clusterHealth").asText());
        }
    }

    @Test void legacyStorageWithInlineShardsStillWorks() throws Exception {
        ClusterState state = cluster();
        try (OpsStore store = new OpsStore(tmp)) {
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(shardMetrics()), "storage-2", OpsMonitor.Attempt.ok(shardMetrics()),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), System.currentTimeMillis());
            assertEquals(1, store.shards().path("shards").size());
            assertEquals("HEALTHY", store.shards().path("shards").get(0).path("state").asText());
            assertTrue(store.overview(state, System.currentTimeMillis()).path("storageNodes").get(0).path("shards").isMissingNode(),
                    "节点行里不带分片列表");
        }
    }

    @Test void shardsAreFetchedSeparatelyAndOnlyEveryFewRounds() throws Exception {
        ClusterState state = cluster();
        java.util.concurrent.atomic.AtomicInteger shardCalls = new java.util.concurrent.atomic.AtomicInteger();
        OpsMonitor.Probe probe = new OpsMonitor.Probe() {
            @Override public OpsMonitor.Attempt fetch(ClusterState ignored, NodeSpec node) { return OpsMonitor.Attempt.ok(metrics(0.2)); }
            @Override public com.fasterxml.jackson.databind.node.ArrayNode shards(ClusterState ignored, NodeSpec node) {
                shardCalls.incrementAndGet();
                return (com.fasterxml.jackson.databind.node.ArrayNode) shardMetrics().get("shards");
            }
        };
        OpsStore store = new OpsStore(tmp);
        try (OpsMonitor monitor = new OpsMonitor(null, probe, store, null)) {
            monitor.collect(state);
            assertEquals(2, shardCalls.get(), "第一轮两台 Storage 都取分片");
            for (int i = 1; i < OpsMonitor.SHARD_EVERY; i++) monitor.collect(state);
            assertEquals(2, shardCalls.get(), "中间几轮不取");
            monitor.collect(state);
            assertEquals(4, shardCalls.get());
            assertEquals("HEALTHY", store.shards().path("shards").get(0).path("state").asText());
        }
    }

    private static ObjectNode node(ObjectNode overview, String field, String id) {
        for (var row : overview.path(field)) if (row.path("nodeId").asText().equals(id)) return (ObjectNode) row;
        throw new AssertionError("缺少节点 " + id);
    }

    private static ObjectNode metrics(double cpu) {
        ObjectNode metrics = ClusterStore.JSON.createObjectNode();
        metrics.put("hostname", "db");
        metrics.put("systemCpuLoad", cpu);
        metrics.put("diskTotalBytes", 1000);
        metrics.put("diskUsableBytes", 800);
        return metrics;
    }

    private static ObjectNode shardMetrics() {
        ObjectNode metrics = metrics(0.2);
        ObjectNode shard = metrics.putArray("shards").addObject();
        shard.put("graphId", 7).put("graph", "social").put("partId", 0).put("term", 3).put("appliedIndex", 20)
                .put("leaderId", "storage-1").put("leader", true);
        shard.putArray("members").add("storage-1").add("storage-2");
        return metrics;
    }

    private static ClusterState cluster() {
        ClusterState state = ClusterState.empty("cengyan-ops").withServer(new Server(
                "server-1", "10.0.0.8", 22, "", ServerStatus.READY, "", "", "", "", "db", "", ""));
        state = state.withNode(node("storage-1", Role.STORAGE));
        state = state.withNode(node("storage-2", Role.STORAGE));
        return state.withNode(node("compute-1", Role.COMPUTE));
    }

    private static NodeSpec node(String id, Role role) {
        return new NodeSpec(id, role, "server-1", 2100, 2101, role == Role.COMPUTE ? 2201 : 0, "/data/" + id,
                NodeStatus.RUNNING, "", true, "rel", "hash");
    }
}
