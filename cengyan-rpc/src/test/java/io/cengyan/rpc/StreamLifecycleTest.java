package io.cengyan.rpc;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class StreamLifecycleTest {
    private static RpcRequest request() {
        return RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId("lifecycle")).setMethod("stream").build();
    }

    @Test void 消费后取消关闭服务端数据源且其它RPC仍可调用() throws Exception {
        CountDownLatch released = new CountDownLatch(1);
        AtomicInteger produced = new AtomicInteger();
        RpcServer.StreamingService service = new RpcServer.StreamingService() {
            public CompletionStage<RpcResponse> handle(RpcRequest request) { return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("OK").build()); }
            public CompletionStage<RpcServer.StreamSource> open(RpcRequest request) {
                return CompletableFuture.completedFuture(new RpcServer.StreamSource() {
                    public CompletionStage<RpcServer.Chunk> next(int maxBytes) {
                        produced.incrementAndGet();
                        return CompletableFuture.completedFuture(new RpcServer.Chunk(ByteString.copyFromUtf8("value"), false));
                    }
                    public void close() { released.countDown(); }
                });
            }
        };
        try (var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "lifecycle", 1024 * 1024, 2, service);
             var client = new RpcClient(server.address(), 1024 * 1024, 2)) {
            var stream = client.open(request(), Duration.ofSeconds(5));
            assertEquals("value", stream.next().get(5, TimeUnit.SECONDS).toStringUtf8());
            stream.close();
            assertTrue(released.await(5, TimeUnit.SECONDS));
            assertEquals(1, produced.get());
            assertEquals("OK", client.call(request(), Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS).getCode());
        }
    }

    @Test void 数据源中途失败不重放且归还客户端并发额度() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RpcServer.StreamingService service = new RpcServer.StreamingService() {
            public CompletionStage<RpcResponse> handle(RpcRequest request) { return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("OK").build()); }
            public CompletionStage<RpcServer.StreamSource> open(RpcRequest request) {
                return CompletableFuture.completedFuture(new RpcServer.StreamSource() {
                    public CompletionStage<RpcServer.Chunk> next(int maxBytes) {
                        return calls.incrementAndGet() == 1
                                ? CompletableFuture.completedFuture(new RpcServer.Chunk(ByteString.copyFromUtf8("first"), false))
                                : CompletableFuture.failedFuture(new IllegalStateException("source failed"));
                    }
                    public void close() { }
                });
            }
        };
        try (var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "lifecycle", 1024 * 1024, 1, service);
             var client = new RpcClient(server.address(), 1024 * 1024, 1);
             var stream = client.open(request(), Duration.ofSeconds(5))) {
            assertEquals("first", stream.next().get(5, TimeUnit.SECONDS).toStringUtf8());
            assertThrows(ExecutionException.class, () -> stream.next().get(5, TimeUnit.SECONDS));
            assertEquals(2, calls.get());
            assertEquals("OK", client.call(request(), Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS).getCode());
        }
    }
}
