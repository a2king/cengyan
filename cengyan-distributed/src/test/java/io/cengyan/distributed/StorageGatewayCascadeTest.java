package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One Storage connection is shared by every request a Compute sends to that node. A single request
 * timing out, or being rejected by the local in-flight cap, used to close that shared connection,
 * which failed every other in-flight write with "Storage 响应中断 ... cause=IOException".
 */
@Timeout(60)
class StorageGatewayCascadeTest {
    private static final String CLUSTER = "cascade-test";
    private static final ScheduledExecutorService DELAY = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "cascade-delay"); t.setDaemon(true); return t;
    });

    private static RpcResponse ok() {
        return RpcResponse.newBuilder().setCode("OK").setBody(BatchResult.newBuilder().setCode("OK").build().toByteString()).build();
    }

    /** "hang" never answers, "slow" answers after delayMs, everything else answers at once. */
    private static RpcServer server(long delayMs) throws Exception {
        return new RpcServer(new InetSocketAddress("127.0.0.1", 0), CLUSTER, 8 * 1024 * 1024, 4096, request -> {
            CompletableFuture<RpcResponse> reply = new CompletableFuture<>();
            switch (request.getMethod()) {
                case "hang" -> { }
                case "slow" -> DELAY.schedule(() -> reply.complete(ok()), delayMs, TimeUnit.MILLISECONDS);
                default -> reply.complete(ok());
            }
            return reply;
        });
    }

    @Test void oneTimedOutRequestDoesNotBreakTheOthersOnTheSameConnection() throws Exception {
        try (var storage = server(1500); var gateway = new StorageGateway(CLUSTER, List.of(storage.address()))) {
            assertEquals("OK", gateway.call(0, 0, "warm", "warm", ByteString.EMPTY, Duration.ofSeconds(5)).getCode());
            ExecutorService callers = Executors.newFixedThreadPool(8);
            try {
                // A slow write in flight while another request on the same connection times out.
                Future<RpcResponse> slow = callers.submit(() -> gateway.call(0, 0, "slow", "slow-write", ByteString.EMPTY, Duration.ofSeconds(10)));
                // Keep traffic flowing so the connection is visibly healthy.
                Future<?> chatter = callers.submit(() -> {
                    for (int i = 0; i < 40; i++) {
                        gateway.call(0, 0, "fast", "fast-" + i, ByteString.EMPTY, Duration.ofSeconds(5));
                        try { Thread.sleep(50); } catch (InterruptedException ex) { return; }
                    }
                });
                ClusterException timedOut = assertThrows(ClusterException.class,
                        () -> gateway.call(0, 0, "hang", "hung-write", ByteString.EMPTY, Duration.ofMillis(300)));
                assertEquals("FAILED", timedOut.code(), "超时的写入仍报告状态未知");
                assertEquals("OK", slow.get(15, TimeUnit.SECONDS).getCode(), "其他请求不受影响");
                chatter.get(15, TimeUnit.SECONDS);
            } finally { callers.shutdownNow(); }
        }
    }

    @Test void requestsBeyondTheLocalInFlightCapWaitInsteadOfBreakingTheConnection() throws Exception {
        // The client allows 128 in-flight requests per connection; send far more concurrent writes than that.
        try (var storage = server(200); var gateway = new StorageGateway(CLUSTER, List.of(storage.address()))) {
            assertEquals("OK", gateway.call(0, 0, "warm", "warm", ByteString.EMPTY, Duration.ofSeconds(5)).getCode());
            ExecutorService callers = Executors.newFixedThreadPool(300);
            try {
                List<Future<RpcResponse>> writes = new ArrayList<>();
                for (int i = 0; i < 300; i++) {
                    int id = i;
                    writes.add(callers.submit(() -> gateway.call(0, 0, "slow", "write-" + id, ByteString.EMPTY, Duration.ofSeconds(30))));
                }
                AtomicInteger ok = new AtomicInteger();
                List<String> failures = new ArrayList<>();
                for (var write : writes) {
                    try { if (write.get(40, TimeUnit.SECONDS).getCode().equals("OK")) ok.incrementAndGet(); }
                    catch (ExecutionException ex) { failures.add(ex.getCause().getMessage()); }
                }
                assertEquals(List.of(), failures, "超过并发上限的请求应等待重试，而不是断开共享连接");
                assertEquals(300, ok.get());
            } finally { callers.shutdownNow(); }
        }
    }
}
