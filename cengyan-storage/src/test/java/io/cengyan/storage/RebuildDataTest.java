package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(180)
class RebuildDataTest {
    @TempDir Path home;
    @Test void singleCopyRestartIsLostAndMetadataSurvives() throws Exception {
        var configs = configs(1);
        StorageConfig config = configs.getFirst();
        long graph;
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            waitMode(gateway, 1);
            var catalog = new DistributedCatalog(gateway);
            graph = catalog.create("create", "G", 2, 1, IdType.STRING, IdType.STRING).getId();
            assertThrows(Exception.class, () -> new StorageServer(config), "重复进程不能清空正在使用的业务目录");
            var batch = StorageGateway.batch("write").addMutations(TransactionEngine.put(TransactionEngine.text("hello"), TransactionEngine.text("world"))).build();
            assertEquals("OK", gateway.write(graph, 0, batch).getCode());
            assertEquals("world", gateway.get(graph, 0, TransactionEngine.text("hello")).toStringUtf8());
            assertEquals("OK", gateway.write(graph, 0, batch).getCode());
            assertEquals("REQUEST_CONFLICT", assertThrows(ClusterException.class, () -> gateway.write(graph, 0,
                    batch.toBuilder().setMutations(0, TransactionEngine.put(TransactionEngine.text("hello"), TransactionEngine.text("different"))).build())).code());
            try (var files = Files.walk(config.directory().resolve("rebuild-data"))) {
                assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith("snapshot")
                        || path.getFileName().toString().startsWith("log_")));
            }
            try (var files = Files.walk(config.directory().resolve("rebuild-data"))) {
                for (Path path : files.filter(p -> p.getFileName().toString().matches("[0-9]+\\.log")).toList())
                    assertEquals(0, Files.size(path), "RocksDB 可创建空 WAL 文件，但不能写恢复日志");
            }
        }
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            assertEquals(graph, new DistributedCatalog(gateway).graph("G").getId());
            await(() -> {
                RebuildHead state = RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0)));
                return state.getState().equals("LOST");
            });
            assertEquals("DATA_LOST", assertThrows(ClusterException.class, () -> gateway.get(graph, 0, TransactionEngine.text("hello"))).code());
            new DistributedCatalog(gateway).dropGraph("drop", "G");
            assertNotEquals(graph, new DistributedCatalog(gateway).create("reimport", "G", 2, 1, IdType.STRING, IdType.STRING).getId());
        }
    }
    @Test void threeCopiesRebuildRestartedNodeAndResizeWithNoDataRaft() throws Exception {
        var configs = configs(3);
        List<StorageServer> servers = new ArrayList<>();
        try (var gateway = gateway(configs)) {
            for (var config : configs) servers.add(new StorageServer(config));
            waitMode(gateway, 3);
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create", "G", 2, 3, IdType.STRING, IdType.STRING);
            graph = catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            List<Entity> entities = new ArrayList<>();
            for (int i = 0; i < 20; i++) entities.add(Entity.newBuilder().setNode(true).setType("N").setId(EntityCodec.value("node-" + i)).build());
            var data = new DistributedGraph(gateway);
            assertEquals(20, data.put("batch", "G", entities, false, Duration.ofSeconds(60)).affected());
            assertEquals(20, data.statistics(graph).getTotalNodes());
            long graphId = graph.getId();
            String before = RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graphId, 0))).getReceiptsList().stream()
                    .filter(r -> r.getNodeId().equals("s2")).findFirst().orElseThrow().getBootId();
            servers.get(2).close(); servers.set(2, null);
            servers.set(2, new StorageServer(configs.get(2)));
            await(() -> RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graphId, 0))).getReceiptsList().stream()
                    .anyMatch(r -> r.getNodeId().equals("s2") && !r.getBootId().equals(before)));
            assertEquals(20, data.statistics(graph).getTotalNodes());
            assertEquals(20, data.put("batch", "G", entities, false, Duration.ofSeconds(60)).affected());
            new ReplicaController(gateway).resize("shrink", "G", 1);
            assertEquals(1, catalog.graph("G").getReplicas());
            new ReplicaController(gateway).resize("grow", "G", 3);
            assertEquals(20, data.statistics(graph).getTotalNodes());
        } finally { for (var server : servers) if (server != null) server.close(); }
    }
    @Test void frozenPartitionRejectsWritesAndStaleImportCannotOverwriteSource() throws Exception {
        var configs = configs(1); var config = configs.getFirst();
        // Keep the deterministic FROZEN state until assertions have completed.
        Files.writeString(config.directory().resolve("operations.properties"), "data_storage_mode=rebuild\nbackground_interval_ms=600000\n");
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            waitMode(gateway, 1);
            long graph = new DistributedCatalog(gateway).create("create", "G", 1, 1, IdType.STRING, IdType.STRING).getId();
            ByteString key = TransactionEngine.text("value");
            gateway.write(graph, 0, StorageGateway.batch("original").addMutations(TransactionEngine.put(key, TransactionEngine.text("old"))).build());
            RebuildHead original = RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0)));
            RebuildHead frozen = original.toBuilder().setState("FROZEN").setOperationId("transfer")
                    .setTarget(original.getSpec()).build();
            gateway.write(0, 0, StorageGateway.batch("freeze-test").addConditions(StorageGateway.condition(RebuildData.key(graph, 0), original.toByteString()))
                    .addMutations(TransactionEngine.put(RebuildData.key(graph, 0), frozen.toByteString())).build());
            var write = StorageGateway.batch("blocked").addMutations(TransactionEngine.put(key, TransactionEngine.text("new"))).build();
            assertEquals("PARTITION_FROZEN", assertThrows(ClusterException.class, () -> gateway.write(graph, 0, write)).code());
            var stale = RebuildMessage.newBuilder().setGraphId(graph).setPartId(0).setHead(frozen).setTransferId("stale").build();
            assertEquals("STALE_CONTROLLER", gateway.direct(config.rpcAddress(), "rebuild.beginImport", stale.toByteString(), Duration.ofSeconds(5)).getCode());
            assertEquals("INVALID_ARGUMENT", gateway.direct(config.rpcAddress(), "rebuild.beginImport", stale.toBuilder().setTransferId("transfer").build().toByteString(), Duration.ofSeconds(5)).getCode());
            assertEquals("old", gateway.get(graph, 0, key).toStringUtf8());
            gateway.write(0, 0, StorageGateway.batch("unfreeze-test").addConditions(StorageGateway.condition(RebuildData.key(graph, 0), frozen.toByteString()))
                    .addMutations(TransactionEngine.put(RebuildData.key(graph, 0), original.toByteString())).build());
            assertEquals("OK", gateway.write(graph, 0, write).getCode());
            assertEquals("new", gateway.get(graph, 0, key).toStringUtf8());
        }
    }
    @Test void concurrentWritesOnSamePartitionRetainAllKeys() throws Exception {
        var configs = configs(1); var config = configs.getFirst();
        try (var server = new StorageServer(config); var gateway = gateway(configs);
             var workers = java.util.concurrent.Executors.newFixedThreadPool(10)) {
            waitMode(gateway, 1);
            long graph = new DistributedCatalog(gateway).create("create", "G", 1, 1, IdType.STRING, IdType.STRING).getId();
            List<java.util.concurrent.Future<BatchResult>> results = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                int id = i;
                results.add(workers.submit(() -> gateway.write(graph, 0, StorageGateway.batch("concurrent-" + id)
                        .addMutations(TransactionEngine.put(TransactionEngine.text("key-" + id), TransactionEngine.text("value-" + id))).build())));
            }
            for (var result : results) assertEquals("OK", result.get(30, java.util.concurrent.TimeUnit.SECONDS).getCode());
            for (int i = 0; i < 10; i++) assertEquals("value-" + i, gateway.get(graph, 0, TransactionEngine.text("key-" + i)).toStringUtf8());
        }
    }
    @Test void expiredCoordinatorDecisionUsesPreparedQuorumWithoutPayloadInMetadata() throws Exception {
        var configs = configs(1); var config = configs.getFirst();
        Files.writeString(config.directory().resolve("operations.properties"), "data_storage_mode=rebuild\nbackground_interval_ms=600000\n");
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            waitMode(gateway, 1);
            long graph = new DistributedCatalog(gateway).create("create", "G", 1, 1, IdType.STRING, IdType.STRING).getId();
            ByteString key = TransactionEngine.text("payload");
            WriteBatch command = StorageGateway.batch("prepared-write").addMutations(TransactionEngine.put(key, TransactionEngine.text("complete"))).build();
            RebuildHead old = RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0)));
            ByteString digest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(command.toByteArray()));
            RebuildHead pending = old.toBuilder().setState("WRITING").setOperationId("lost-coordinator").setDigest(digest)
                    .setOperationDeadlineMs(1).build();
            gateway.write(0, 0, StorageGateway.batch("inject-pending").addConditions(StorageGateway.condition(RebuildData.key(graph, 0), old.toByteString()))
                    .addMutations(TransactionEngine.put(RebuildData.key(graph, 0), pending.toByteString())).build());
            var staged = gateway.direct(config.rpcAddress(), "rebuild.stage", RebuildMessage.newBuilder().setGraphId(graph).setPartId(0)
                    .setHead(pending).setCommand(command).build().toByteString(), Duration.ofSeconds(5));
            assertEquals("OK", staged.getCode(), staged.getMessage());
            assertEquals("complete", gateway.get(graph, 0, key).toStringUtf8());
            var complete = RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0)));
            assertEquals("READY", complete.getState()); assertEquals(1, complete.getSequence());
            assertFalse(complete.toString().contains("complete"), "控制面不保存写入正文");
        }
    }
    @Test void newStorageJoinTriggersFrozenFullCopyAndRemovesOldPlacement() throws Exception {
        var configs = configs(2);
        var original = configs.getFirst();
        var first = new StorageConfig(original.clusterId(), original.nodeId(), original.rpcAddress(), original.raftAddress(), original.directory(),
                List.of(original.metadataMembers().getFirst()), original.raftSettings(), original.rpcMaxPayload(), 128, 4, 256);
        var other = configs.get(1);
        var second = new StorageConfig(other.clusterId(), other.nodeId(), other.rpcAddress(), other.raftAddress(), other.directory(),
                first.metadataMembers(), other.raftSettings(), other.rpcMaxPayload(), 128, 4, 256);
        try (var server = new StorageServer(first); var gateway = gateway(List.of(first))) {
            waitMode(gateway, 1);
            var graph = new DistributedCatalog(gateway).create("create", "G", 4, 1, IdType.STRING, IdType.STRING);
            for (int part = 0; part < 4; part++) gateway.write(graph.getId(), part, StorageGateway.batch("part-" + part)
                    .addMutations(TransactionEngine.put(TransactionEngine.text("key"), TransactionEngine.text("value-" + part))).build());
            try (var joining = new StorageServer(second)) {
                waitMode(gateway, 2);
                await(() -> {
                    int placed = 0;
                    for (int part = 0; part < 4; part++) {
                        var spec = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                        if (spec.getMembers(0).getNodeId().equals("s1")) placed++;
                    }
                    return placed == 2;
                });
                for (int part = 0; part < 4; part++) assertEquals("value-" + part, gateway.get(graph.getId(), part, TransactionEngine.text("key")).toStringUtf8());
                await(() -> {
                    try (var dirs = Files.list(first.directory().resolve("rebuild-data"))) { return dirs.count() == 2; }
                });
            }
        }
    }
    @Test void maintenanceRestartReusesSealedFilesForSingleCopy() throws Exception {
        var configs = configs(1); var config = configs.getFirst();
        long graph;
        ByteString key = TransactionEngine.text("hello");
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            waitMode(gateway, 1);
            graph = new DistributedCatalog(gateway).create("create", "G", 2, 1, IdType.STRING, IdType.STRING).getId();
            for (int part = 0; part < 2; part++) assertEquals("OK", gateway.write(graph, part, StorageGateway.batch("w" + part)
                    .addMutations(TransactionEngine.put(key, TransactionEngine.text("world-" + part))).build()).getCode());
            String id = maintenance(gateway, "enter").path("id").asText();
            assertFalse(id.isBlank());
            assertEquals(id, maintenance(gateway, "enter").path("id").asText(), "重复进入返回同一个维护 id");
            assertEquals("CLUSTER_MAINTENANCE", assertThrows(ClusterException.class, () -> gateway.write(graph, 0,
                    StorageGateway.batch("blocked").addMutations(TransactionEngine.put(key, TransactionEngine.text("x"))).build())).code());
            await(() -> maintenance(gateway, "status").path("nodes").get(0).path("sealed").asBoolean());
            assertTrue(Files.exists(config.directory().resolve("rebuild-data/seal.json")));
        }
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            await(() -> {
                var status = maintenance(gateway, "status");
                var node = status.path("nodes").get(0);
                return node.path("resumed").asBoolean() && node.path("untrustedReplicas").asInt() == 0
                        && status.path("partitions").path("ready").asInt() == 2;
            });
            for (int part = 0; part < 2; part++) assertEquals("world-" + part, gateway.get(graph, part, key).toStringUtf8());
            maintenance(gateway, "exit");
            assertFalse(maintenance(gateway, "status").path("active").asBoolean());
            assertEquals("OK", gateway.write(graph, 0, StorageGateway.batch("after").addMutations(TransactionEngine.put(key, TransactionEngine.text("again"))).build()).getCode());
            await(() -> !Files.exists(config.directory().resolve("rebuild-data/seal.json")));
        }
        // Outside maintenance a restart keeps the existing behaviour: files are untrusted.
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            await(() -> RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0))).getState().equals("LOST"));
        }
    }
    @Test void maintenanceRestartWithChangedFilesDoesNotTrustThem() throws Exception {
        var configs = configs(1); var config = configs.getFirst();
        long graph;
        ByteString key = TransactionEngine.text("hello");
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            waitMode(gateway, 1);
            graph = new DistributedCatalog(gateway).create("create", "G", 2, 1, IdType.STRING, IdType.STRING).getId();
            for (int part = 0; part < 2; part++) gateway.write(graph, part, StorageGateway.batch("w" + part)
                    .addMutations(TransactionEngine.put(key, TransactionEngine.text("world-" + part))).build());
            maintenance(gateway, "enter");
            await(() -> maintenance(gateway, "status").path("nodes").get(0).path("sealed").asBoolean());
        }
        // Simulate a local file change: the sealed fingerprint of part 0 no longer matches.
        Path sealFile = config.directory().resolve("rebuild-data/seal.json");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var seal = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(sealFile.toFile());
        for (var part : seal.path("parts")) if (part.path("part").asInt() == 0)
            ((com.fasterxml.jackson.databind.node.ObjectNode) part).put("count", part.path("count").asLong() + 1);
        mapper.writeValue(sealFile.toFile(), seal);
        try (var server = new StorageServer(config); var gateway = gateway(configs)) {
            await(() -> RebuildHead.parseFrom(gateway.get(0, 0, RebuildData.key(graph, 0))).getState().equals("LOST"));
            assertEquals("world-1", gateway.get(graph, 1, key).toStringUtf8());
            assertEquals("DATA_LOST", assertThrows(ClusterException.class, () -> gateway.get(graph, 0, key)).code());
            var node = maintenance(gateway, "status").path("nodes").get(0);
            assertTrue(node.path("resumed").asBoolean());
            assertEquals(1, node.path("untrustedReplicas").asInt());
            maintenance(gateway, "exit");
        }
    }
    private com.fasterxml.jackson.databind.JsonNode maintenance(StorageGateway gateway, String action) throws Exception {
        var response = gateway.call(0, 0, "metadata.maintenance." + action, "maintenance-" + action + "-" + UUID.randomUUID(), ByteString.EMPTY, Duration.ofSeconds(15));
        return new com.fasterxml.jackson.databind.ObjectMapper().readTree(response.getBody().toByteArray());
    }
    private List<StorageConfig> configs(int count) throws Exception {
        List<ServerSocket> sockets = new ArrayList<>(); List<RaftPeer> peers = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                var raft = new ServerSocket(0); var rpc = new ServerSocket(0); sockets.add(raft); sockets.add(rpc);
                peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress("127.0.0.1:" + raft.getLocalPort()).setClientAddress("127.0.0.1:" + rpc.getLocalPort()).build());
            }
        } finally { for (var socket : sockets) socket.close(); }
        List<StorageConfig> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Path dir = home.resolve("s" + i); Files.createDirectories(dir);
            Files.writeString(dir.resolve("operations.properties"), "data_storage_mode=rebuild\nbackground_interval_ms=500\n");
            result.add(new StorageConfig("rebuild-test", "s" + i, StorageConfig.endpoint(peers.get(i).getClientAddress()),
                    StorageConfig.endpoint(peers.get(i).getAddress()), dir, peers, RaftSettings.defaults(), 32 * 1024 * 1024, 128, 4, 256));
        }
        return result;
    }
    private StorageGateway gateway(List<StorageConfig> configs) { return new StorageGateway("rebuild-test", configs.stream().map(StorageConfig::rpcAddress).toList()); }
    private void waitMode(StorageGateway gateway, int count) throws Exception {
        await(() -> { var nodes = new DistributedCatalog(gateway).nodes(); return nodes.size() == count && nodes.stream().allMatch(n -> n.getDataMode().equals("rebuild")); });
    }
    private interface Check { boolean get() throws Exception; }
    private void await(Check check) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(45).toNanos(); Throwable last = null;
        do { try { if (check.get()) return; } catch (Exception ex) { last = ex; } Thread.sleep(100); } while (System.nanoTime() < until);
        throw new AssertionError("等待重建状态超时", last);
    }
}
