package io.cengyan.storage;

import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class GraphTransactionsTest {
    @TempDir Path home;
    @Test @Timeout(90)
    void 创建分片后跨分片写点边覆盖和端点跳过() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("graph-test", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 8, 256);
        try (var server = new StorageServer(config); var gateway = new StorageGateway("graph-test", List.of(config.rpcAddress()))) {
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create-graph", "Company", 3, 1, IdType.STRING, IdType.STRING);
            graph = catalog.addType("type-node", "Company", TypeDefinition.newBuilder().setName("Company").setNode(true)
                    .addProperties(PropertyDefinition.newBuilder().setName("name").setType("STRING").setNullable(true)).build());
            graph = catalog.addType("type-edge", "Company", TypeDefinition.newBuilder().setName("holder").build());
            var data = new DistributedGraph(gateway);
            var a = entity(true, "a", "Company").putProperties("name", EntityCodec.value("A")).build();
            var b = entity(true, "b", "Company").putProperties("name", EntityCodec.value("B")).build();
            var edge = entity(false, "e1", "holder").setSource(EntityCodec.value("a")).setTarget(EntityCodec.value("b")).build();
            assertEquals(3, data.put("batch-1", "Company", List.of(a, b, edge), false, Duration.ofSeconds(30)).affected());
            assertEquals(2, data.statistics(graph).getTotalNodes());
            assertEquals(1, data.statistics(graph).getTotalEdges());
            assertEquals(2, data.scan(graph, true, "Company", data.readScope(graph.getId())).size());
            assertEquals(1, data.neighbors(graph, EntityCodec.value("a"), true, "holder", data.readScope(graph.getId())).size());
            assertEquals(3, data.put("batch-1", "Company", List.of(a, b, edge), false, Duration.ofSeconds(30)).affected());
            var updated = a.toBuilder().clearProperties().build();
            data.put("replace", "Company", List.of(updated), false, Duration.ofSeconds(30));
            assertEquals(2, data.statistics(graph).getTotalNodes());
            assertEquals(1, data.statistics(graph).getTotalEdges());
            assertEquals(Value.KindCase.NULL_VALUE, data.get(graph, true, EntityCodec.value("a"), data.readScope(graph.getId())).getPropertiesOrThrow("name").getKindCase());
            var missing = edge.toBuilder().setId(EntityCodec.value("missing-edge")).setTarget(EntityCodec.value("absent")).build();
            assertEquals(1, data.put("skip", "Company", List.of(missing), true, Duration.ofSeconds(30)).skipped());
            var conflict = edge.toBuilder().setTarget(EntityCodec.value("a")).build();
            var ex = assertThrows(ClusterException.class, () -> data.put("bad-structure", "Company", List.of(conflict), false, Duration.ofSeconds(30)));
            assertEquals("ID_CONFLICT", ex.code());
            var badIndex = assertThrows(ClusterException.class, () -> data.createUniqueIndex("index-name", "Company",
                    IndexDefinition.newBuilder().setName("unique-name").setNode(false).setTypeName("holder").setUnique(true).addProperties("missing").build()));
            assertEquals("TYPE_MISMATCH", badIndex.code());
            data.deleteNode("delete-a", graph, EntityCodec.value("a"));
            assertNull(data.get(graph, true, EntityCodec.value("a"), data.readScope(graph.getId())));
            assertNull(data.get(graph, false, EntityCodec.value("e1"), data.readScope(graph.getId())));
            assertEquals(0, data.neighbors(graph, EntityCodec.value("b"), false, "holder", data.readScope(graph.getId())).size());
            assertEquals(1, data.statistics(graph).getTotalNodes());
            assertEquals(0, data.statistics(graph).getTotalEdges());
            catalog.create("unique-graph", "Unique", 3, 1, IdType.STRING, IdType.STRING);
            var uniqueGraph = catalog.addType("unique-type", "Unique", TypeDefinition.newBuilder().setName("Company").setNode(true)
                    .addProperties(PropertyDefinition.newBuilder().setName("name").setType("STRING").setNullable(true)).build());
            data.put("unique-seed", "Unique", List.of(a, b.toBuilder().putProperties("name", EntityCodec.value("A")).build()), false, Duration.ofSeconds(30));
            var duplicateIndex = assertThrows(ClusterException.class, () -> data.createUniqueIndex("duplicate-index", "Unique",
                    IndexDefinition.newBuilder().setName("duplicate-name").setNode(true).setTypeName("Company").setUnique(true).addProperties("name").build()));
            assertEquals("UNIQUE_VIOLATION", duplicateIndex.code());
            assertEquals("ACTIVE", catalog.graph("Unique").getState());
            assertNotNull(data.get(uniqueGraph, true, EntityCodec.value("b"), data.readScope(uniqueGraph.getId())));
            for (int part = 0; part < uniqueGraph.getPartitions(); part++)
                assertNull(gateway.get(uniqueGraph.getId(), part, TransactionEngine.text("__schema/writeGate")));
            data.put("unique-fix", "Unique", List.of(b), false, Duration.ofSeconds(30));
            data.createUniqueIndex("unique-index", "Unique", IndexDefinition.newBuilder().setName("company-name").setNode(true).setTypeName("Company")
                    .setUnique(true).addProperties("name").build());
            data.put("unique-first", "Unique", List.of(a), false, Duration.ofSeconds(30));
            var duplicate = b.toBuilder().putProperties("name", EntityCodec.value("A")).build();
            var uniqueConflict = assertThrows(ClusterException.class, () -> data.put("unique-second", "Unique", List.of(duplicate), false, Duration.ofSeconds(30)));
            assertEquals("UNIQUE_VIOLATION", uniqueConflict.code());
            assertEquals("B", data.get(uniqueGraph, true, EntityCodec.value("b"), data.readScope(uniqueGraph.getId()))
                    .getPropertiesOrThrow("name").getStringValue());
            var alteredContent = a.toBuilder().putProperties("name", EntityCodec.value("DIFFERENT")).build();
            assertEquals("REQUEST_CONFLICT", assertThrows(ClusterException.class, () -> data.put("unique-first", "Unique", List.of(alteredContent), false, Duration.ofSeconds(30))).code());
            data.dropIndex("drop-index", "Unique", "company-name");
            assertTrue(catalog.graph("Unique").getIndexesList().isEmpty());
            // 删除请求重放必须返回同一结果，不能再次推进 Schema 版本。
            long droppedVersion = catalog.graph("Unique").getSchemaVersion();
            assertEquals(droppedVersion, data.dropIndex("drop-index", "Unique", "company-name").getSchemaVersion());
            data.createUniqueIndex("recreate-index", "Unique", IndexDefinition.newBuilder().setName("company-name").setNode(true)
                    .setTypeName("Company").setUnique(true).addProperties("name").build());
            var oldDefinition = catalog.graph("Company");
            var removed = oldDefinition.toBuilder().clearTypes();
            for (TypeDefinition type : oldDefinition.getTypesList()) removed.addTypes(type.getNode() ? type.toBuilder().clearProperties().build() : type);
            var oldScope = data.readScope(oldDefinition.getId());
            new SchemaController(gateway).alter("schema-drop-name", "Company", removed.build());
            var newDefinition = catalog.graph("Company");
            assertEquals(oldDefinition.getGeneration() + 1, newDefinition.getGeneration());
            assertEquals("B", data.get(oldDefinition, true, EntityCodec.value("b"), oldScope).getPropertiesOrThrow("name").getStringValue());
            assertEquals(0, data.get(newDefinition, true, EntityCodec.value("b"), data.readScope(newDefinition.getId())).getPropertiesCount());
            assertEquals(1, data.statistics(oldDefinition).getTotalNodes());
            assertEquals(1, data.statistics(newDefinition).getTotalNodes());
            catalog.create("large-graph", "Large", 3, 1, IdType.STRING, IdType.STRING);
            var largeGraph = catalog.addType("large-type", "Large", TypeDefinition.newBuilder().setName("Blob").setNode(true)
                    .addProperties(PropertyDefinition.newBuilder().setName("body").setType("STRING").setNullable(false)).build());
            List<Entity> large = new ArrayList<>();
            for (int i = 0; i < 40; i++) large.add(Entity.newBuilder().setNode(true).setType("Blob").setId(EntityCodec.value("blob-" + i))
                    .putProperties("body", EntityCodec.value("x".repeat(40_000))).build());
            assertEquals(40, data.put("large-batch", "Large", large, false, Duration.ofSeconds(30)).affected());
            // Tiny budget would fail an entity/transaction scan; Count Store reads metadata only.
            try (var budget = StorageGateway.budget(512, () -> false)) {
                assertEquals(40, data.statistics(largeGraph).getTotalNodes());
            }
            assertEquals(40, data.scan(largeGraph, true, "Blob", data.readScope(largeGraph.getId())).size());
            assertEquals(40, data.put("large-batch", "Large", large, false, Duration.ofSeconds(30)).affected());

            var deleted = catalog.create("recreate-first", "Recreate", 1, 1, IdType.STRING, IdType.STRING);
            var droppedRoute = gateway.get(0, 0, StorageGateway.groupKey(deleted.getId(), 0));
            catalog.dropGraph("recreate-drop", "Recreate");
            assertNotNull(droppedRoute);
            var recreated = catalog.create("recreate-second", "Recreate", 1, 1, IdType.STRING, IdType.STRING);
            assertEquals("ACTIVE", recreated.getState());
            assertNotEquals(deleted.getId(), recreated.getId());
            assertNotNull(catalog.graph("Recreate"));
        }
    }
    private static Entity.Builder entity(boolean node, String id, String type) {
        return Entity.newBuilder().setNode(node).setId(EntityCodec.value(id)).setType(type);
    }
}
