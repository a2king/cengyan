package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class StorageGatewayRoutingTest {
    private static final String CLUSTER = "routing-test";
    private static final ByteString KEY = ByteString.copyFromUtf8("key");
    private static RpcResponse ok(ByteString body) { return RpcResponse.newBuilder().setCode("OK").setBody(body).build(); }
    private static RpcResponse code(String code) { return RpcResponse.newBuilder().setCode(code).build(); }
    private static RpcServer server(java.util.function.Function<RpcRequest, RpcResponse> service) throws Exception {
        return new RpcServer(new InetSocketAddress("127.0.0.1", 0), CLUSTER, 8 * 1024 * 1024, 128,
                request -> CompletableFuture.completedFuture(service.apply(request)));
    }
    private static ByteString route(InetSocketAddress address) {
        return DataGroupSpec.newBuilder().setGraphId(1).setPartId(0).addMembers(Node.newBuilder()
                .setClusterId(CLUSTER).setNodeId("s").setRole(NodeRole.STORAGE)
                .setRpc(Endpoint.newBuilder().setHost(address.getHostString()).setPort(address.getPort())))
                .build().toByteString();
    }
    private static RpcResponse stored(ByteString value) {
        return ok(StoredValue.newBuilder().setFound(true).setValue(value).build().toByteString());
    }

    @Test void warmedRouteRefreshesForBothExplicitRoutingFailuresAndPreservesWrites() throws Exception {
        for (String error : List.of("NOT_LEADER", "GROUP_UNAVAILABLE")) {
            AtomicBoolean moved = new AtomicBoolean();
            AtomicInteger metadataReads = new AtomicInteger();
            AtomicReference<RpcRequest> written = new AtomicReference<>();
            try (var old = server(request -> moved.get() ? code(error) : stored(ByteString.copyFromUtf8("old")));
                 var next = server(request -> {
                     if (request.getMethod().equals("kv.write")) {
                         written.set(request);
                         return ok(BatchResult.newBuilder().setCode("OK").build().toByteString());
                     }
                     return stored(ByteString.copyFromUtf8("new"));
                 });
                 var metadata = server(request -> {
                     metadataReads.incrementAndGet();
                     return stored(route(moved.get() ? next.address() : old.address()));
                 });
                 var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
                assertEquals("old", gateway.get(1, 0, KEY).toStringUtf8());
                moved.set(true);
                var command = StorageGateway.batch("same-write-id").addMutations(Mutation.newBuilder().setKey(KEY).setValue(KEY)).build();
                assertEquals("OK", gateway.write(1, 0, command).getCode());
                assertEquals("same-write-id", written.get().getContext().getRequestId());
                assertEquals(command.toByteString(), written.get().getBody());
                assertEquals("new", gateway.get(1, 0, KEY).toStringUtf8());
                assertTrue(metadataReads.get() >= 2);
                assertTrue(metadataReads.get() <= 4, "refresh must be throttled");
            }
        }
    }

    @Test void concurrentFailuresShareMetadataRefresh() throws Exception {
        AtomicBoolean moved = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        try (var old = server(request -> moved.get() ? code("NOT_LEADER") : stored(KEY));
             var next = server(request -> stored(KEY));
             var metadata = server(request -> { reads.incrementAndGet(); return stored(route(moved.get() ? next.address() : old.address())); });
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()));
             var workers = Executors.newFixedThreadPool(12)) {
            gateway.get(1, 0, KEY);
            moved.set(true);
            List<Future<ByteString>> results = new ArrayList<>();
            for (int i = 0; i < 12; i++) results.add(workers.submit(() -> gateway.get(1, 0, KEY)));
            for (Future<ByteString> result : results) assertEquals(KEY, result.get(5, TimeUnit.SECONDS));
            assertTrue(reads.get() <= 4, "one failed partition must not flood Metadata: " + reads.get());
        }
    }

    @Test void metadataLookupConsumesOriginalDeadline() throws Exception {
        try (var metadata = new RpcServer(new InetSocketAddress("127.0.0.1", 0), CLUSTER, 1024 * 1024, 16,
                request -> new CompletableFuture<>());
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            long started = System.nanoTime();
            assertThrows(ClusterException.class, () -> gateway.call(1, 0, "kv.read", "",
                    ReadKey.newBuilder().setKey(KEY).build().toByteString(), Duration.ofMillis(200)));
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2), "Metadata must not start a new 15s timeout");
        }
    }

    @Test void failedMetadataRefreshRecoversAfterCooldown() throws Exception {
        AtomicBoolean unavailable = new AtomicBoolean(true);
        try (var target = server(request -> stored(KEY));
             var metadata = server(request -> unavailable.get() ? code("FAILED") : stored(route(target.address())));
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            assertThrows(ClusterException.class, () -> gateway.get(1, 0, KEY));
            unavailable.set(false);
            Thread.sleep(150);
            assertEquals(KEY, gateway.get(1, 0, KEY), "a previous Metadata failure must not poison the route forever");
        }
    }

    @Test void invalidHintAndUnavailableOldOwnerStillRefreshTopology() throws Exception {
        try (var old = server(request -> RpcResponse.newBuilder().setCode("NOT_LEADER")
                     .setLeaderHint(LeaderHint.newBuilder().setRpc(Endpoint.newBuilder().setPort(0))).build());
             var next = server(request -> stored(KEY));
             var metadata = server(request -> stored(route(next.address())));
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            gateway.route(1, 0, List.of(old.address()));
            assertEquals(KEY, gateway.get(1, 0, KEY));
        }
    }

    @Test void refusedOldConnectionRefreshesBeforeSendingTheWrite() throws Exception {
        InetSocketAddress dead;
        try (var socket = new java.net.ServerSocket(0)) { dead = new InetSocketAddress("127.0.0.1", socket.getLocalPort()); }
        AtomicInteger writes = new AtomicInteger();
        try (var next = server(request -> { writes.incrementAndGet(); return ok(KEY); });
             var metadata = server(request -> stored(route(next.address())));
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            gateway.route(1, 0, List.of(dead));
            assertEquals(KEY, gateway.call(1, 0, "kv.write", "stable-id", KEY, Duration.ofSeconds(3)).getBody());
            assertEquals(1, writes.get());
        }
    }

    @Test void cyclicLeaderHintsRefreshMetadataAndRemainBounded() throws Exception {
        AtomicReference<InetSocketAddress> other = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        try (var a = server(request -> {
                 attempts.incrementAndGet();
                 var endpoint = other.get();
                 return RpcResponse.newBuilder().setCode("NOT_LEADER").setLeaderHint(LeaderHint.newBuilder()
                         .setRpc(Endpoint.newBuilder().setHost(endpoint.getHostString()).setPort(endpoint.getPort()))).build();
             });
             var b = server(request -> {
                 attempts.incrementAndGet();
                 return RpcResponse.newBuilder().setCode("NOT_LEADER").setLeaderHint(LeaderHint.newBuilder()
                         .setRpc(Endpoint.newBuilder().setHost(a.address().getHostString()).setPort(a.address().getPort()))).build();
             });
             var metadata = server(request -> { reads.incrementAndGet(); return stored(route(a.address())); });
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            other.set(b.address());
            assertThrows(ClusterException.class, () -> gateway.call(1, 0, "kv.read", "",
                    ReadKey.newBuilder().setKey(KEY).build().toByteString(), Duration.ofMillis(450)));
            assertTrue(reads.get() >= 2);
            assertTrue(attempts.get() < 30, "redirect loop must back off");
        }
    }

    @Test void startedScanNeverReopensOnMigration() throws Exception {
        AtomicBoolean moved = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        try (var old = server(request -> moved.get() ? code("GROUP_UNAVAILABLE")
                    : ok(ScanPage.newBuilder().setMore(true).setCursorId("cursor").setLastKey(KEY).build().toByteString()));
             var metadata = server(request -> { reads.incrementAndGet(); return stored(route(old.address())); });
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            gateway.call(1, 0, "kv.read", "", ReadKey.newBuilder().setScan(true).setScanId("cursor").build().toByteString(), Duration.ofSeconds(2));
            int before = reads.get();
            moved.set(true);
            ClusterException ex = assertThrows(ClusterException.class, () -> gateway.scan(1, 0, KEY, KEY, 100, "cursor"));
            assertEquals("QUERY_CANCELED", ex.code());
            assertEquals(before, reads.get(), "a continued cursor must not open another snapshot on a new owner");
        }
    }

    @Test void missingRouteDoesNotCreateDataGroupOrProbeSeedsAsDataNodes() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var metadata = server(request -> {
                 calls.incrementAndGet();
                 assertEquals(0, request.getContext().getGraphId());
                 assertEquals("kv.read", request.getMethod());
                 return ok(StoredValue.newBuilder().setFound(false).build().toByteString());
             }); var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            assertEquals("GROUP_UNAVAILABLE", assertThrows(ClusterException.class, () -> gateway.get(1, 0, KEY)).code());
            assertEquals(1, calls.get());
        }
    }

    @Test void ambiguousWriteTimeoutIsNotReplayedAtAnotherReplica() throws Exception {
        AtomicInteger writes = new AtomicInteger();
        try (var target = new RpcServer(new InetSocketAddress("127.0.0.1", 0), CLUSTER, 1024 * 1024, 16, request -> {
                 writes.incrementAndGet(); return new CompletableFuture<>();
             });
             var metadata = server(request -> stored(route(target.address())));
             var gateway = new StorageGateway(CLUSTER, List.of(metadata.address()))) {
            ClusterException ex = assertThrows(ClusterException.class, () -> gateway.call(1, 0, "kv.write", "stable-id", KEY, Duration.ofMillis(250)));
            assertEquals("FAILED", ex.code());
            assertEquals(1, writes.get());
        }
    }
}
