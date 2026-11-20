package io.cengyan.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** 副本数调整是后台作业：立即返回、按分片推进、同时最多两个分片、可以取消并回滚。 */
class ReplicaJobTest {
    @TempDir Path home;
    static final ObjectMapper JSON = new ObjectMapper();

    @Test @Timeout(180)
    void 增加副本立即返回并按分片推进取消后回滚未完成的分片() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); List<RaftPeer> peers = new ArrayList<>();
        int[] ports = new int[6];
        try {
            for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); }
            for (int i = 0; i < 3; i++) peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress(addr(ports[2*i])).setClientAddress(addr(ports[2*i+1])).build());
        } finally { for (var socket : reserved) socket.close(); }
        List<StorageServer> servers = new ArrayList<>();
        try (var gateway = new StorageGateway("jobs", List.of(addr(ports[1]), addr(ports[3]), addr(ports[5])))) {
            // 后台循环拉长，由测试显式推进，结果可重复。
            for (int i = 0; i < 3; i++) {
                Path dir = home.resolve("s" + i);
                Files.createDirectories(dir);
                Files.writeString(dir.resolve("operations.properties"), "background_interval_ms=86400000\n");
                servers.add(new StorageServer(new StorageConfig("jobs", "s" + i,
                        addr(ports[2*i+1]), addr(ports[2*i]), dir, peers, RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128)));
            }
            var catalog = new DistributedCatalog(gateway);
            catalog.create("create", "G", 6, 1, IdType.STRING, IdType.STRING);
            var graph = catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            var data = new DistributedGraph(gateway);
            List<Entity> nodes = new ArrayList<>();
            for (int i = 0; i < 60; i++) nodes.add(Entity.newBuilder().setNode(true).setType("N").setId(EntityCodec.value("v" + i)).build());
            data.put("seed", "G", nodes, false, Duration.ofSeconds(30));
            assertEquals(60, countAll(data, graph), "导入完成");

            var controller = new ReplicaController(gateway);
            var job = controller.resize("grow", "G", 2);
            assertEquals("RUNNING", job.state());
            assertEquals(job.id(), controller.resize("grow", "G", 2).id(), "同一请求重试返回同一个作业");
            assertEquals("BUSY", assertThrows(ClusterException.class, () -> controller.resize("other", "G", 3)).code(), "同一个图同时只能有一个作业");

            controller.tick();
            JsonNode status = controller.status("G");
            assertEquals(ReplicaController.MAX_ACTIVE, status.path("running").asInt(), "同时最多两个分片在同步：" + status);
            assertEquals(6 - ReplicaController.MAX_ACTIVE, status.path("pending").asInt());

            // 推进到新节点已加入（只读副本或投票成员）再取消，覆盖真正的成员回滚。
            boolean joined = false;
            for (int i = 0; i < 100 && !joined; i++) {
                controller.tick();
                for (JsonNode p : controller.status("G").path("partitions"))
                    if (Set.of("CATCHING_UP", "UPDATING_TOPOLOGY").contains(p.path("state").asText())) joined = true;
                if (!joined) Thread.sleep(100);
            }
            assertTrue(joined, "新节点应该已经加入分片");
            controller.cancel("G");
            controller.runUntilIdle(Duration.ofSeconds(120));
            status = controller.status("G");
            assertEquals("CANCELED", status.path("job").path("state").asText(), status.toString());
            int grown = 0;
            for (int part = 0; part < 6; part++) {
                var route = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                Set<String> routed = new HashSet<>();
                route.getMembersList().forEach(m -> routed.add(m.getNodeId()));
                JsonNode m = JSON.readTree(gateway.call(graph.getId(), part, "data.membership", "", ByteString.EMPTY, Duration.ofSeconds(10)).getBody().toByteArray());
                Set<String> voters = new HashSet<>();
                m.path("voters").forEach(v -> voters.add(v.asText()));
                assertEquals(routed, voters, "分片 " + part + " 的 Raft 成员和路由一致");
                assertEquals(0, m.path("listeners").size(), "回滚后没有残留的只读副本");
                assertNull(gateway.get(0, 0, TransactionEngine.text("tasks/activeReplica/" + graph.getId() + "/" + part)), "分片锁已释放");
                if (route.getMembersCount() == 2) grown++;
                else assertEquals(1, route.getMembersCount());
            }
            assertTrue(grown <= ReplicaController.MAX_ACTIVE, "取消前没开始的分片保持原样");
            assertTrue(java.util.stream.StreamSupport.stream(status.path("partitions").spliterator(), false)
                    .anyMatch(p -> p.path("state").asText().equals("CANCELED")), "有分片真正走了回滚：" + status);
            assertEquals(60, countAll(data, graph), "回滚后数据完整");

            // 取消后可以重新开始，这次完整做完。
            var again = controller.resize("grow-again", "G", 2);
            assertEquals("RUNNING", again.state());
            controller.runUntilIdle(Duration.ofSeconds(150));
            status = controller.status("G");
            assertEquals("DONE", status.path("job").path("state").asText(), status.toString());
            assertEquals(6, status.path("done").asInt());
            assertEquals(2, catalog.graph("G").getReplicas());
            for (int part = 0; part < 6; part++)
                assertEquals(2, DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part))).getMembersCount());
            assertEquals(60, countAll(data, graph));
        } finally { for (var server : servers) if (server != null) server.close(); }
    }

    @Test @Timeout(180)
    void 旧版本失败留下的任务被回滚并释放分片() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); List<RaftPeer> peers = new ArrayList<>();
        int[] ports = new int[4];
        try {
            for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); }
            for (int i = 0; i < 2; i++) peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress(addr(ports[2*i])).setClientAddress(addr(ports[2*i+1])).build());
        } finally { for (var socket : reserved) socket.close(); }
        List<StorageServer> servers = new ArrayList<>();
        try (var gateway = new StorageGateway("legacy", List.of(addr(ports[1]), addr(ports[3])))) {
            for (int i = 0; i < 2; i++) {
                Path dir = home.resolve("l" + i);
                Files.createDirectories(dir);
                Files.writeString(dir.resolve("operations.properties"), "background_interval_ms=86400000\n");
                servers.add(new StorageServer(new StorageConfig("legacy", "s" + i,
                        addr(ports[2*i+1]), addr(ports[2*i]), dir, peers, RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128)));
            }
            var catalog = new DistributedCatalog(gateway);
            catalog.create("create", "L", 2, 1, IdType.STRING, IdType.STRING);
            var graph = catalog.addType("type", "L", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            var before = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), 0)));
            var nodes = catalog.placementNodes();
            Node other = nodes.stream().filter(n -> before.getMembersList().stream().noneMatch(m -> m.getNodeId().equals(n.getNodeId()))).findFirst().orElseThrow();
            // 模拟旧版本：同步调用超时，任务停在 CREATED、占着分片锁，新节点已经作为只读副本加入。
            String taskId = "legacy-request/0";
            var task = ReplicaTask.newBuilder().setTaskId(taskId).setBefore(before)
                    .setAfter(before.toBuilder().addMembers(other)).setStage("CREATED").build();
            ByteString active = TransactionEngine.text("tasks/activeReplica/" + graph.getId() + "/0");
            gateway.write(0, 0, StorageGateway.batch("legacy.task").addMutations(TransactionEngine.put(TransactionEngine.text("tasks/replica/" + taskId), task.toByteString()))
                    .addMutations(TransactionEngine.put(active, TransactionEngine.text(taskId))).build());
            var staging = before.toBuilder().addLearners(other).setControlTaskId(taskId).build();
            assertEquals("OK", gateway.direct(DistributedCatalog.address(other), "data.addGroup", staging.toByteString(), Duration.ofSeconds(30)).getCode());

            var controller = new ReplicaController(gateway);
            controller.resize("grow", "L", 2);
            controller.runUntilIdle(Duration.ofSeconds(150));
            JsonNode status = controller.status("L");
            assertEquals("DONE", status.path("job").path("state").asText(), status.toString());
            assertEquals("CANCELED", ReplicaTask.parseFrom(gateway.get(0, 0, TransactionEngine.text("tasks/replica/" + taskId))).getStage());
            for (int part = 0; part < 2; part++)
                assertEquals(2, DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part))).getMembersCount());
            assertNull(gateway.get(0, 0, active));
        } finally { for (var server : servers) if (server != null) server.close(); }
    }

    /**
     * 2026-11-19 线上：新副本在加入过程中被卸下后，Leader 上加入它的 setConfiguration 永远等不到追平，客户端一直重试，
     * Leader 一直报 configuring；回滚却要等 configuring 结束才发命令，于是卡在回滚中，Leader 不停给已不存在的副本发数据。
     */
    @Test @Timeout(180)
    void 新副本消失后加入命令卡住时回滚仍能完成() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); List<RaftPeer> peers = new ArrayList<>();
        int[] ports = new int[6];
        try {
            for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); }
            for (int i = 0; i < 3; i++) peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress(addr(ports[2*i])).setClientAddress(addr(ports[2*i+1])).build());
        } finally { for (var socket : reserved) socket.close(); }
        // 节点间请求超时调小，Leader 放弃不响应的新成员（staging 超时）只要几秒。
        var settings = new RaftSettings(16L * 1024 * 1024, 2_000, 1_000, 1, 600, 1200, 0, 0, 3_000);
        List<StorageServer> servers = new ArrayList<>();
        try (var gateway = new StorageGateway("stuck", List.of(addr(ports[1]), addr(ports[3]), addr(ports[5])))) {
            for (int i = 0; i < 3; i++) {
                Path dir = home.resolve("k" + i);
                Files.createDirectories(dir);
                Files.writeString(dir.resolve("operations.properties"), "background_interval_ms=86400000\n");
                servers.add(new StorageServer(new StorageConfig("stuck", "s" + i,
                        addr(ports[2*i+1]), addr(ports[2*i]), dir, peers, settings, 8 * 1024 * 1024, 128, 4, 128)));
            }
            var catalog = new DistributedCatalog(gateway);
            catalog.create("create", "S", 1, 1, IdType.STRING, IdType.STRING);
            var graph = catalog.addType("type", "S", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            var before = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), 0)));
            Node other = catalog.placementNodes().stream()
                    .filter(n -> before.getMembersList().stream().noneMatch(m -> m.getNodeId().equals(n.getNodeId()))).findFirst().orElseThrow();

            // 新副本没在接收快照时回答 -1；Leader 的成员状态带上快照大小字段。
            assertEquals("-1", gateway.direct(DistributedCatalog.address(before.getMembers(0)), "data.receiving", before.toByteString(), Duration.ofSeconds(5)).getBody().toStringUtf8());
            JsonNode m0 = membership(gateway, graph);
            assertTrue(m0.has("snapshotBytes"), m0.toString());

            String taskId = "stuck-request/0";
            var task = ReplicaTask.newBuilder().setTaskId(taskId).setBefore(before).setAfter(before.toBuilder().addMembers(other)).setStage("CREATED").build();
            ByteString active = TransactionEngine.text("tasks/activeReplica/" + graph.getId() + "/0");
            gateway.write(0, 0, StorageGateway.batch("stuck.task").addMutations(TransactionEngine.put(TransactionEngine.text("tasks/replica/" + taskId), task.toByteString()))
                    .addMutations(TransactionEngine.put(active, TransactionEngine.text(taskId))).build());
            // 新副本所在节点下线，再让 Leader 开始加入它：这次成员变更永远完成不了。
            servers.get(Integer.parseInt(other.getNodeId().substring(1))).close();
            var staging = before.toBuilder().addLearners(other).setControlTaskId(taskId).build();
            assertEquals("OK", gateway.call(graph.getId(), 0, "data.configure", "", staging.toByteString(), Duration.ofSeconds(30)).getCode());
            JsonNode stuck = membership(gateway, graph);
            assertTrue(stuck.path("configuring").asBoolean(), "加入命令应该还在等：" + stuck);

            // 没有作业认领的任务由控制器回滚（与取消作业走同一条路径）。
            var controller = new ReplicaController(gateway);
            controller.runUntilIdle(Duration.ofSeconds(90));
            assertEquals("CANCELED", ReplicaTask.parseFrom(gateway.get(0, 0, TransactionEngine.text("tasks/replica/" + taskId))).getStage());
            assertNull(gateway.get(0, 0, active), "分片锁已释放");
            JsonNode after = membership(gateway, graph);
            assertFalse(after.path("configuring").asBoolean(), "Leader 上卡住的加入命令已停止：" + after);
            assertEquals(0, after.path("listeners").size(), after.toString());
            assertEquals(1, after.path("voters").size(), after.toString());
        } finally { for (var server : servers) if (server != null) server.close(); }
    }

    private static JsonNode membership(StorageGateway gateway, GraphDefinition graph) throws Exception {
        return JSON.readTree(gateway.call(graph.getId(), 0, "data.membership", "", ByteString.EMPTY, Duration.ofSeconds(10)).getBody().toByteArray());
    }

    private static int countAll(DistributedGraph data, GraphDefinition graph) {
        int found = 0;
        for (int i = 0; i < 60; i++) if (data.get(graph, true, EntityCodec.value("v" + i), data.readScope(graph.getId())) != null) found++;
        return found;
    }
    private static InetSocketAddress addr(int port) { return new InetSocketAddress("127.0.0.1", port); }
}
