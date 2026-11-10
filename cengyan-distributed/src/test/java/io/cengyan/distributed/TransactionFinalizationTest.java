package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** Deterministic CAS interleaving: the entity changes while the reservation stays the same. */
@Timeout(20)
class TransactionFinalizationTest {
    @Test void committedFinalizerRetriesChangedCellWithoutReusingPhysicalRequestId() throws Exception {
        finalizeAfterCellConflict(TransactionState.COMMITTED);
    }

    @Test void abortedFinalizerRetriesChangedCellWithoutReusingPhysicalRequestId() throws Exception {
        finalizeAfterCellConflict(TransactionState.ABORTED);
    }

    private void finalizeAfterCellConflict(TransactionState decision) throws Exception {
        String cluster = "finalize-test", txId = "batch-256";
        ByteString key = TransactionEngine.text("entity/a");
        ByteString lock = TransactionEngine.text("__lock/").concat(key);
        var owner = ReservationOwner.newBuilder().setTransactionId(txId).setCoordinatorPart(0).build();
        var plan = TransactionPlan.newBuilder().setTransactionId(txId).setGraphId(1).setCoordinatorPart(0)
                .setDeadlineMs(System.currentTimeMillis() + 60_000)
                .addParticipants(ParticipantPlan.newBuilder().setPartId(0)
                        .addChanges(ConditionalMutation.newBuilder().setKey(key).setValue(TransactionEngine.text("new")))).build();
        var record = TransactionRecord.newBuilder().setPlan(plan).setState(decision).setDecidedAtMs(100).build();
        var cell = VersionedCell.newBuilder()
                .addVersions(CellVersion.newBuilder().setBaseline(true).setValue(TransactionEngine.text("old")))
                .addVersions(CellVersion.newBuilder().setTransactionId(txId).setValue(TransactionEngine.text("new"))).build();
        Map<ByteString, ByteString> values = new HashMap<>();
        values.put(key, cell.toByteString());
        values.put(lock, ReservationState.newBuilder().setWriter(owner).build().toByteString());
        values.put(TransactionEngine.text("__participant/" + txId), TransactionEngine.text(decision.name()));
        values.put(TransactionEngine.transactionKey(txId), record.toByteString());
        Map<String, ByteString> identities = new HashMap<>();
        Map<String, BatchResult> results = new HashMap<>();
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger conflictingIds = new AtomicInteger();
        try (var rpc = new RpcServer(new InetSocketAddress("127.0.0.1", 0), cluster, 1024 * 1024, 16, request -> {
            try {
                if (request.getMethod().equals("kv.read")) {
                    ReadKey read = ReadKey.parseFrom(request.getBody());
                    if (read.getKeysCount() > 0) {
                        var batch = BatchValues.newBuilder();
                        for (var requested : read.getKeysList()) {
                            ByteString value = values.get(requested);
                            var stored = StoredValue.newBuilder().setFound(value != null);
                            if (value != null) stored.setValue(value);
                            batch.addValues(stored);
                        }
                        return CompletableFuture.completedFuture(ok(batch.build().toByteString()));
                    }
                    ByteString value = values.get(read.getKey());
                    var stored = StoredValue.newBuilder().setFound(value != null);
                    if (value != null) stored.setValue(value);
                    return CompletableFuture.completedFuture(ok(stored.build().toByteString()));
                }
                WriteBatch batch = WriteBatch.parseFrom(request.getBody());
                ByteString identity = batch.toBuilder().clearExpiresAtMs().clearControllerEpoch().build().toByteString();
                ByteString previous = identities.putIfAbsent(batch.getRequestId(), identity);
                if (previous != null) {
                    if (!previous.equals(identity)) {
                        conflictingIds.incrementAndGet();
                        return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("REQUEST_CONFLICT")
                                .setMessage("相同 requestId 对应不同写入内容").build());
                    }
                    return CompletableFuture.completedFuture(outcome(results.get(batch.getRequestId())));
                }
                // Simulate a competing maintenance/finalization write after this reader's snapshot.
                if (batch.getRequestId().startsWith("tx.release") && releases.incrementAndGet() == 1) {
                    values.put(key, cell.toBuilder().setVersions(0, cell.getVersions(0).toBuilder().setCommittedAtMs(50))
                            .build().toByteString());
                }
                boolean matches = batch.getConditionsList().stream().allMatch(condition -> {
                    ByteString current = values.get(condition.getKey());
                    return condition.getExpectedPresent() == (current != null)
                            && (current == null || current.equals(condition.getExpectedValue()));
                });
                var result = BatchResult.newBuilder().setCode(matches ? "OK" : "WRITE_CONFLICT").build();
                results.put(batch.getRequestId(), result);
                if (matches) for (Mutation mutation : batch.getMutationsList()) {
                    if (mutation.getDelete()) values.remove(mutation.getKey());
                    else values.put(mutation.getKey(), mutation.getValue());
                }
                return CompletableFuture.completedFuture(outcome(result));
            } catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
        }); var gateway = new StorageGateway(cluster, List.of(rpc.address()))) {
            gateway.route(1, 0, List.of(rpc.address()));
            TransactionRecord finished = new TransactionEngine(gateway).resume(record);
            assertTrue(finished.getFinalized());
            assertEquals(decision, finished.getState());
            assertEquals(2, releases.get());
            assertEquals(0, conflictingIds.get());
            assertNull(values.get(lock));
            var after = VersionedCell.parseFrom(values.get(key));
            assertEquals("old", after.getVersions(0).getValue().toStringUtf8());
            if (decision == TransactionState.COMMITTED) {
                assertEquals(2, after.getVersionsCount());
                assertEquals(100, after.getVersions(1).getCommittedAtMs());
            } else assertEquals(1, after.getVersionsCount());
        }
    }

    private static RpcResponse ok(ByteString body) {
        return RpcResponse.newBuilder().setCode("OK").setBody(body).build();
    }

    private static RpcResponse outcome(BatchResult result) {
        return RpcResponse.newBuilder().setCode(result.getCode()).setMessage(result.getMessage()).setBody(result.toByteString()).build();
    }
}
