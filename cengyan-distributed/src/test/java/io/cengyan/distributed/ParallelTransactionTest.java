package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class ParallelTransactionTest {
    @Test void preparesOverlapAndFinalizeIsBatched() throws Exception { run(false); }
    @Test void abortWaitsForAllInFlightPreparesAndReleasesSuccessfulReservations() throws Exception { run(true); }

    private void run(boolean conflict) throws Exception {
        String cluster = "parallel-test";
        Map<Integer, Map<ByteString, ByteString>> partitions = Map.of(0, new HashMap<>(), 1, new HashMap<>());
        Object mutex = new Object();
        CountDownLatch prepared = new CountDownLatch(2);
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicInteger decisions = new AtomicInteger(), releases = new AtomicInteger(), pointLocks = new AtomicInteger();
        var plan = TransactionPlan.newBuilder().setGraphId(1).setTransactionId("parallel").setCoordinatorPart(1)
                .setDeadlineMs(System.currentTimeMillis() + 60_000);
        for (int part = 0; part < 2; part++) {
            var participant = ParticipantPlan.newBuilder().setPartId(part);
            for (int i = 0; i < 64; i++) participant.addChanges(ConditionalMutation.newBuilder()
                    .setKey(TransactionEngine.text("entity/" + i)).setValue(TransactionEngine.text("value/" + i)));
            plan.addParticipants(participant);
        }
        try (var workers = Executors.newFixedThreadPool(2); var caller = Executors.newSingleThreadExecutor();
             var rpc = new RpcServer(new InetSocketAddress("127.0.0.1", 0), cluster, 2 * 1024 * 1024, 32, request -> {
                 try {
                     int part = request.getContext().getPartId();
                     Map<ByteString, ByteString> values = partitions.get(part);
                     synchronized (mutex) {
                         if (request.getMethod().equals("kv.read")) {
                             ReadKey read = ReadKey.parseFrom(request.getBody());
                             if (read.getKeysCount() > 0) {
                                 var batch = BatchValues.newBuilder();
                                 for (var key : read.getKeysList()) batch.addValues(stored(values.get(key)));
                                 return CompletableFuture.completedFuture(ok(batch.build().toByteString()));
                             }
                             if (read.getKey().toStringUtf8().startsWith("__lock/")) pointLocks.incrementAndGet();
                             return CompletableFuture.completedFuture(ok(stored(values.get(read.getKey())).toByteString()));
                         }
                         WriteBatch batch = WriteBatch.parseFrom(request.getBody());
                         boolean preparing = batch.getRequestId().startsWith("tx.prepare/");
                         if (batch.getRequestId().startsWith("tx.decide/")) decisions.incrementAndGet();
                         if (batch.getRequestId().startsWith("tx.release")) releases.incrementAndGet();
                         boolean matches = !(preparing && conflict && part == 0) && batch.getConditionsList().stream().allMatch(condition -> {
                             ByteString current = values.get(condition.getKey());
                             return condition.getExpectedPresent() == (current != null)
                                     && (current == null || current.equals(condition.getExpectedValue()));
                         });
                         if (matches) for (var mutation : batch.getMutationsList()) {
                             if (mutation.getDelete()) values.remove(mutation.getKey());
                             else values.put(mutation.getKey(), mutation.getValue());
                         }
                         String code = matches ? "OK" : "WRITE_CONFLICT";
                         var response = RpcResponse.newBuilder().setCode(code)
                                 .setBody(BatchResult.newBuilder().setCode(code).build().toByteString()).build();
                         if (preparing) {
                             prepared.countDown();
                             if (matches) return release.thenApply(ignored -> response);
                         }
                         return CompletableFuture.completedFuture(response);
                     }
                 } catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
             }); var gateway = new StorageGateway(cluster, List.of(rpc.address()))) {
            gateway.route(1, 0, List.of(rpc.address())); gateway.route(1, 1, List.of(rpc.address()));
            var engine = new TransactionEngine(gateway, workers);
            Future<TransactionRecord> result = caller.submit(() -> engine.execute(plan.build()));
            try {
                assertTrue(prepared.await(5, TimeUnit.SECONDS), "两个分片必须同时进入 PREPARE，不能串行等待响应");
                assertEquals(0, decisions.get(), "所有在途准备操作结束前不能发布 COMMIT/ABORT");
            } finally { release.complete(null); }
            var finished = result.get(5, TimeUnit.SECONDS);
            assertTrue(finished.getFinalized());
            assertEquals(conflict ? TransactionState.ABORTED : TransactionState.COMMITTED, finished.getState());
            assertEquals(1, finished.getPlan().getCoordinatorPart(), "持久化协调者不能被 normalize 改回分片 0");
            assertEquals(2, releases.get(), "每个参与分片一次收尾；准备失败的分片也要写入终态栅栏");
            assertEquals(0, pointLocks.get());
            synchronized (mutex) {
                for (var values : partitions.values()) {
                    assertTrue(values.keySet().stream().noneMatch(key -> key.toStringUtf8().startsWith("__lock/")));
                    for (int i = 0; i < 64; i++) {
                        ByteString cell = values.get(TransactionEngine.text("entity/" + i));
                        if (conflict) assertNull(cell);
                        else assertTrue(VersionedCell.parseFrom(cell).getVersions(0).getCommittedAtMs() > 0);
                    }
                }
            }
        }
    }
    private static StoredValue stored(ByteString value) {
        var result = StoredValue.newBuilder().setFound(value != null);
        if (value != null) result.setValue(value);
        return result.build();
    }
    private static RpcResponse ok(ByteString body) { return RpcResponse.newBuilder().setCode("OK").setBody(body).build(); }
}
