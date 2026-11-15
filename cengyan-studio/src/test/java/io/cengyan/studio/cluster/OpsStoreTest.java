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
            assertEquals("DEGRADED", overview.path("shards").get(0).path("state").asText());
            assertEquals("storage-1", overview.path("shards").get(0).path("leaderId").asText());
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

    @Test void samplesAndEventsOlderThanSevenDaysAreRemoved() throws Exception {
        ClusterState state = cluster();
        long now = System.currentTimeMillis();
        long old = now - TimeUnit.DAYS.toMillis(8);
        try (OpsStore store = new OpsStore(tmp)) {
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), old);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.fail("连接超时"), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), old + 1000);
            store.record(state, Map.of("storage-1", OpsMonitor.Attempt.ok(metrics(0.2)), "storage-2", OpsMonitor.Attempt.ok(metrics(0.2)),
                    "compute-1", OpsMonitor.Attempt.ok(metrics(0.2))), now);
            assertTrue(store.rows("sample") >= 4);
            assertTrue(store.rows("event_log") >= 1);
            store.purge(now);
            assertEquals(3, store.rows("sample"));
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
