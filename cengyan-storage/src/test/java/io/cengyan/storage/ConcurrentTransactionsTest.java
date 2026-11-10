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
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrentTransactionsTest {
    @TempDir Path home;

    @Test @Timeout(90)
    void 并发唯一索引只有一个所有者且查询不读取半批() throws Exception {
        int raft, rpc;
        try (var first = new ServerSocket(0); var second = new ServerSocket(0)) { raft = first.getLocalPort(); rpc = second.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress(new InetSocketAddress("127.0.0.1", raft))
                .setClientAddress(new InetSocketAddress("127.0.0.1", rpc)).build();
        var config = new StorageConfig("concurrent", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
        try (var server = new StorageServer(config); var storage = new StorageGateway("concurrent", List.of(config.rpcAddress()));
             var workers = Executors.newFixedThreadPool(3)) {
            var catalog = new DistributedCatalog(storage);
            catalog.create("graph", "G", 3, 1, IdType.STRING, IdType.STRING);
            catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N")
                    .addProperties(PropertyDefinition.newBuilder().setName("key").setType("STRING").setNullable(false)).build());
            var data = new DistributedGraph(storage);
            var definition = data.createUniqueIndex("index", "G", IndexDefinition.newBuilder().setNode(true).setTypeName("N")
                    .setName("unique-key").setUnique(true).addProperties("key").build());
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> writers = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                int number = i;
                writers.add(workers.submit(() -> {
                    start.await();
                    try {
                        data.put("writer-" + number, "G", List.of(entity("owner-" + number, "same")), false, Duration.ofSeconds(30));
                        return "OK";
                    } catch (ClusterException ex) { return ex.code(); }
                }));
            }
            start.countDown();
            List<String> outcomes = new ArrayList<>();
            for (var writer : writers) outcomes.add(writer.get(40, TimeUnit.SECONDS));
            assertEquals(1, outcomes.stream().filter("OK"::equals).count(), outcomes.toString());
            assertTrue(outcomes.stream().allMatch(code -> Set.of("OK", "WRITE_CONFLICT", "UNIQUE_VIOLATION").contains(code)), outcomes.toString());
            assertEquals(1, data.scan(definition, true, "N", data.readScope(definition.getId())).size());
            assertEquals(1, data.statistics(definition).getTotalNodes(), "失败/回滚的唯一索引事务不能留下计数");

            List<Entity> batch = new ArrayList<>();
            for (int i = 0; i < 12; i++) batch.add(entity("batch-" + i, "value-" + i));
            var writer = workers.submit(() -> data.put("atomic-batch", "G", batch, false, Duration.ofSeconds(30)));
            int observations = 0;
            do {
                var scope = data.readScope(definition.getId());
                int seen = 0;
                for (Entity entity : batch) if (data.get(definition, true, entity.getId(), scope) != null) seen++;
                assertTrue(seen == 0 || seen == batch.size(), "读取到半批：" + seen);
                observations++;
            } while (!writer.isDone() && observations < 20);
            assertEquals(batch.size(), writer.get(40, TimeUnit.SECONDS).affected());
            var scope = data.readScope(definition.getId());
            for (Entity entity : batch) assertNotNull(data.get(definition, true, entity.getId(), scope));
            assertEquals(13, data.statistics(definition).getTotalNodes());
        }
    }
    private static Entity entity(String id, String key) {
        return Entity.newBuilder().setNode(true).setType("N").setId(EntityCodec.value(id)).putProperties("key", EntityCodec.value(key)).build();
    }

    @Test @Timeout(180)
    void 十路批量写入计数和重放一致() throws Exception {
        int raft, rpc;
        try (var first = new ServerSocket(0); var second = new ServerSocket(0)) { raft = first.getLocalPort(); rpc = second.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress(new InetSocketAddress("127.0.0.1", raft))
                .setClientAddress(new InetSocketAddress("127.0.0.1", rpc)).build();
        var config = new StorageConfig("parallel-bulk", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 256);
        try (var server = new StorageServer(config); var storage = new StorageGateway(config.clusterId(), List.of(config.rpcAddress()));
             var workers = Executors.newFixedThreadPool(10)) {
            var catalog = new DistributedCatalog(storage);
            catalog.create("graph", "G", 32, 1, IdType.STRING, IdType.STRING);
            catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N")
                    .addProperties(PropertyDefinition.newBuilder().setName("key").setType("STRING").setNullable(false)).build());
            var data = new DistributedGraph(storage);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<DistributedGraph.BulkResult>> writes = new ArrayList<>();
            List<List<Entity>> batches = new ArrayList<>();
            for (int writer = 0; writer < 10; writer++) {
                List<Entity> batch = new ArrayList<>();
                for (int i = 0; i < 512; i++) batch.add(entity("writer-" + writer + "-" + i, "value-" + i));
                batches.add(batch);
                String id = "batch-" + writer;
                writes.add(workers.submit(() -> { start.await(); return data.put(id, "G", batch, false, Duration.ofSeconds(90)); }));
            }
            start.countDown();
            for (var write : writes) assertEquals(512, write.get(100, TimeUnit.SECONDS).affected());
            var definition = catalog.graph("G");
            assertEquals(5120, data.statistics(definition).getTotalNodes());
            Set<Integer> coordinators = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                RequestTicket ticket = RequestTicket.parseFrom(storage.get(0, 0, TransactionEngine.text("requests/batch-" + i)));
                coordinators.add(ticket.getPlan().getCoordinatorPart());
                assertEquals(512, data.put("batch-" + i, "G", batches.get(i), false, Duration.ofSeconds(30)).affected());
            }
            assertTrue(coordinators.size() > 1, "协调负载应分散到多个分片");
            assertEquals(5120, data.statistics(definition).getTotalNodes());
            assertEquals(5120, data.scan(definition, true, "N", data.readScope(definition.getId())).size());
            for (int part = 0; part < 32; part++) assertEquals(0,
                    storage.scan(definition.getId(), part, TransactionEngine.text("__lock/"), com.google.protobuf.ByteString.EMPTY, 256).getEntriesCount());
        }
    }

    @Test @Timeout(90)
    void 五千条大属性批次支持事务状态CAS和重放() throws Exception {
        int raft, rpc;
        try (var first = new ServerSocket(0); var second = new ServerSocket(0)) { raft = first.getLocalPort(); rpc = second.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress(new InetSocketAddress("127.0.0.1", raft))
                .setClientAddress(new InetSocketAddress("127.0.0.1", rpc)).build();
        var config = new StorageConfig("large-bulk", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), StorageGateway.MAX_RPC_PAYLOAD, 128, 4, 256);
        try (var server = new StorageServer(config); var storage = new StorageGateway(config.clusterId(), List.of(config.rpcAddress()))) {
            var catalog = new DistributedCatalog(storage);
            catalog.create("graph", "G", 32, 1, IdType.STRING, IdType.STRING);
            var definition = catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N")
                    .addProperties(PropertyDefinition.newBuilder().setName("key").setType("STRING").setNullable(false)).build());
            List<Entity> batch = new ArrayList<>();
            for (int i = 0; i < 5000; i++) batch.add(entity("large-" + i, "x".repeat(740)));
            var data = new DistributedGraph(storage);
            assertEquals(5000, data.put("org-v2-" + "a".repeat(64), "G", batch, false, Duration.ofSeconds(60)).affected());
            RequestTicket ticket = RequestTicket.parseFrom(storage.get(0, 0,
                    TransactionEngine.text("requests/org-v2-" + "a".repeat(64))));
            var preparing = TransactionRecord.newBuilder().setPlan(ticket.getPlan()).setState(TransactionState.PREPARING).build();
            var decided = preparing.toBuilder().setState(TransactionState.COMMITTED).setDecidedAtMs(1).build();
            var txKey = TransactionEngine.transactionKey(ticket.getPlan().getTransactionId());
            var cas = StorageGateway.batch("size-proof").addConditions(StorageGateway.condition(txKey, preparing.toByteString()))
                    .addMutations(TransactionEngine.put(txKey, decided.toByteString())).build();
            assertTrue(cas.getSerializedSize() > 8 * 1024 * 1024, "回归批次必须实际跨过旧 8 MiB CAS 上限");
            assertTrue(cas.getSerializedSize() < StorageGateway.MAX_RPC_PAYLOAD);
            assertEquals(5000, data.statistics(definition).getTotalNodes());
            assertEquals(5000, data.put("org-v2-" + "a".repeat(64), "G", batch, false, Duration.ofSeconds(60)).affected());
        }
    }
}
