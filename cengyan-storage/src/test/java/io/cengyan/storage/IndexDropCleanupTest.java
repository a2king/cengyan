package io.cengyan.storage;

import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.DistributedGraph;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.protocol.IndexFamilyName;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/** 删索引只改 Catalog，物理 Column Family 由拓扑恢复线程随后删除。 */
class IndexDropCleanupTest {
    @TempDir Path home;

    @Test @Timeout(120)
    void 删除索引立即生效并在后台物理删除ColumnFamily() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("index-drop", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 64);
        Path db;
        String family = IndexFamilyName.of("index-req");
        long graphId;
        try (var server = new StorageServer(config); var gateway = new StorageGateway("index-drop", List.of(config.rpcAddress()))) {
            var catalog = new DistributedCatalog(gateway);
            graphId = catalog.create("create-graph", "Idx", 1, 1, IdType.STRING, IdType.STRING).getId();
            catalog.addType("type-node", "Idx", TypeDefinition.newBuilder().setName("Company").setNode(true)
                    .addProperties(PropertyDefinition.newBuilder().setName("name").setType("STRING").setNullable(true)).build());
            var data = new DistributedGraph(gateway);
            var graph = catalog.graph("Idx");
            Entity node = Entity.newBuilder().setNode(true).setType("Company").setId(io.cengyan.distributed.EntityCodec.value("a"))
                    .putProperties("name", io.cengyan.distributed.EntityCodec.value("A")).build();
            assertEquals(1, data.put("seed", "Idx", List.of(node), false, Duration.ofSeconds(30)).affected());
            data.createUniqueIndex("index-req", "Idx", IndexDefinition.newBuilder().setName("by-name").setNode(true)
                    .setTypeName("Company").setUnique(true).addProperties("name").build());
            assertEquals(1, catalog.graph("Idx").getIndexesList().size());
            assertEquals("OK", data.dropIndex("index-drop", "Idx", "by-name").getState().equals("ACTIVE") ? "OK" : "GRAPH_STATE");
            assertTrue(catalog.graph("Idx").getIndexesList().isEmpty(), "删除请求返回时索引已不在 Catalog");
            // 拓扑恢复线程随后完成物理删除，并把任务标成 FINISHED。
            server.recoverTopologyNow();
            IndexBuildTask task = IndexBuildTask.parseFrom(gateway.get(0, 0, TransactionEngine.text("tasks/index/index-drop")));
            assertEquals("FINISHED", task.getStage());
            db = shardDatabase(home, GroupIds.data("index-drop", graphId, 0).getUuid().toString());
        }
        assertFalse(columnFamilies(db).contains(family), "索引 Column Family 必须已从分片库中物理删除：" + columnFamilies(db));
    }

    @Test @Timeout(120)
    void 旧Schema租约未到期时先不删物理数据() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("index-lease", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 64);
        String family = IndexFamilyName.of("lease-req");
        long graphId;
        try (var server = new StorageServer(config); var gateway = new StorageGateway("index-lease", List.of(config.rpcAddress()))) {
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create-graph", "Lease", 1, 1, IdType.STRING, IdType.STRING);
            graphId = graph.getId();
            catalog.addType("type-node", "Lease", TypeDefinition.newBuilder().setName("Company").setNode(true)
                    .addProperties(PropertyDefinition.newBuilder().setName("name").setType("STRING").setNullable(true)).build());
            var data = new DistributedGraph(gateway);
            data.put("seed", "Lease", List.of(Entity.newBuilder().setNode(true).setType("Company")
                    .setId(io.cengyan.distributed.EntityCodec.value("a")).putProperties("name", io.cengyan.distributed.EntityCodec.value("A")).build()),
                    false, Duration.ofSeconds(30));
            data.createUniqueIndex("lease-req", "Lease", IndexDefinition.newBuilder().setName("by-name").setNode(true)
                    .setTypeName("Company").setUnique(true).addProperties("name").build());
            // 删除前签发的租约属于旧版本；删除请求本身会把版本 +1。
            long oldVersion = catalog.graph("Lease").getSchemaVersion();
            data.dropIndex("lease-drop", "Lease", "by-name");
            // 旧版本租约仍可能有查询在走这个索引，保留边界没过前不能删分片上的数据。
            long retainUntil = System.currentTimeMillis() + 60_000;
            gateway.write(0, 0, StorageGateway.batch("lease.horizon/test")
                    .addMutations(TransactionEngine.put(io.cengyan.distributed.SchemaLeases.key(graph.getId(), oldVersion),
                            LeaseHorizon.newBuilder().setGraphId(graph.getId()).setSchemaVersion(oldVersion).setGeneration(graph.getGeneration())
                                    .setEpoch(1).setGrantUntilMs(retainUntil).setRetainUntilMs(retainUntil).build().toByteString())).build());
            server.recoverTopologyNow();
            String uuid = GroupIds.data("index-lease", graphId, 0).getUuid().toString();
            assertEquals("DROPPING", IndexBuildTask.parseFrom(gateway.get(0, 0, TransactionEngine.text("tasks/index/lease-drop"))).getStage(),
                    "租约未到期时任务不能标成完成");
            assertTrue(columnFamilies(shardDatabase(home, uuid)).contains(family), "租约未到期时索引数据必须保留");
            // 把保留边界挪到过去，下一轮恢复就应该删除。
            gateway.write(0, 0, StorageGateway.batch("lease.horizon/expired")
                    .addMutations(TransactionEngine.put(io.cengyan.distributed.SchemaLeases.key(graph.getId(), oldVersion),
                            LeaseHorizon.newBuilder().setGraphId(graph.getId()).setSchemaVersion(oldVersion).setGeneration(graph.getGeneration())
                                    .setEpoch(1).setGrantUntilMs(1).setRetainUntilMs(1).build().toByteString())).build());
            server.recoverTopologyNow();
            assertEquals("FINISHED", IndexBuildTask.parseFrom(gateway.get(0, 0, TransactionEngine.text("tasks/index/lease-drop"))).getStage());
        }
        assertFalse(columnFamilies(shardDatabase(home, GroupIds.data("index-lease", graphId, 0).getUuid().toString())).contains(family));
    }

    private static Path shardDatabase(Path home, String uuid) {
        return home.resolve("raft").resolve(uuid).resolve("sm").resolve("db");
    }

    private static List<String> columnFamilies(Path directory) throws Exception {
        if (!Files.isDirectory(directory)) return List.of();
        try (var options = new Options()) {
            return RocksDB.listColumnFamilies(options, directory.toString()).stream()
                    .map(name -> new String(name, StandardCharsets.UTF_8)).toList();
        }
    }
}
