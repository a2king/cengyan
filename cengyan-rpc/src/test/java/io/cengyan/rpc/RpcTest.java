package io.cengyan.rpc;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class RpcTest {
    private static final int MAX = 1024 * 1024;

    @Test void 流只在消费后发送下一批且结束释放额度() throws Exception {
        var produced = new java.util.concurrent.atomic.AtomicInteger();
        var released = new java.util.concurrent.atomic.AtomicBoolean();
        RpcServer.StreamingService service = new RpcServer.StreamingService() {
            public CompletionStage<RpcResponse> handle(RpcRequest request) { return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("OK").build()); }
            public CompletionStage<RpcServer.StreamSource> open(RpcRequest request) {
                return CompletableFuture.completedFuture(new RpcServer.StreamSource() {
                    public CompletionStage<RpcServer.Chunk> next(int bytes) {
                        int sequence = produced.incrementAndGet();
                        return CompletableFuture.completedFuture(new RpcServer.Chunk(ByteString.copyFromUtf8("batch-" + sequence), sequence == 3));
                    }
                    public void close() { released.set(true); }
                });
            }
        };
        try (var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test", MAX, 4, service);
             var client = new RpcClient(server.address(), MAX, 4);
             var stream = client.open(request("test", "stream", ""), Duration.ofSeconds(5))) {
            assertEquals(0, produced.get());
            assertEquals("batch-1", stream.next().get(5, TimeUnit.SECONDS).toStringUtf8());
            assertEquals(1, produced.get());
            assertEquals("batch-2", stream.next().get(5, TimeUnit.SECONDS).toStringUtf8());
            assertEquals("batch-3", stream.next().get(5, TimeUnit.SECONDS).toStringUtf8());
            assertNull(stream.next().get(5, TimeUnit.SECONDS));
            assertTrue(released.get());
        }
    }

    @Test void 帧支持分段与粘包并拒绝超限头() {
        var frame = new RpcFrame(RpcFrame.Type.REQUEST, 7, 0, ByteString.copyFromUtf8("hello"));
        var encoder = new EmbeddedChannel(new RpcFrameCodec.Encoder(MAX));
        var decoder = new EmbeddedChannel(new RpcFrameCodec.Decoder(MAX));
        try {
            encoder.writeOutbound(frame);
            io.netty.buffer.ByteBuf encoded = encoder.readOutbound();
            try {
                assertFalse(decoder.writeInbound(encoded.readRetainedSlice(10)));
                assertTrue(decoder.writeInbound(encoded.readRetainedSlice(encoded.readableBytes())));
                assertEquals(frame, decoder.readInbound());
                encoder.writeOutbound(frame);
                encoder.writeOutbound(frame);
                io.netty.buffer.ByteBuf first = encoder.readOutbound(), second = encoder.readOutbound();
                decoder.writeInbound(Unpooled.wrappedBuffer(first, second));
                assertEquals(frame, decoder.readInbound());
                assertEquals(frame, decoder.readInbound());
            } finally { encoded.release(); }
        } finally { encoder.finishAndReleaseAll(); decoder.finishAndReleaseAll(); }
        var bad = new EmbeddedChannel(new RpcFrameCodec.Decoder(10));
        var header = Unpooled.buffer().writeInt(RpcFrameCodec.MAGIC).writeShort(1).writeByte(1).writeByte(0)
                .writeLong(1).writeLong(0).writeInt(Integer.MAX_VALUE);
        try { assertThrows(DecoderException.class, () -> bad.writeInbound(header)); }
        finally { try { bad.finishAndReleaseAll(); } catch (DecoderException ignored) { } }
    }

    @Test void 真正TCP多路复用集群校验超时和连接断开() throws Exception {
        CompletableFuture<RpcResponse> blocked = new CompletableFuture<>();
        try (var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test", MAX, 64,
                request -> request.getMethod().equals("block") ? blocked : CompletableFuture.completedFuture(
                        RpcResponse.newBuilder().setCode("OK").setBody(request.getBody()).build()));
             var client = new RpcClient(server.address(), MAX, 64)) {
            var calls = new ArrayList<CompletableFuture<RpcResponse>>();
            for (int i = 0; i < 32; i++) calls.add(client.call(request("test", "echo", "v" + i), Duration.ofSeconds(5)));
            for (int i = 0; i < calls.size(); i++) assertEquals("v" + i, calls.get(i).get(5, TimeUnit.SECONDS).getBody().toStringUtf8());
            assertEquals("CLUSTER_MISMATCH", client.call(request("other", "echo", ""), Duration.ofSeconds(5)).get().getCode());
            var timeout = client.call(request("test", "block", ""), Duration.ofMillis(100));
            assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, () -> timeout.get(5, TimeUnit.SECONDS)).getCause());
        }
        CompletableFuture<RpcResponse> never = new CompletableFuture<>();
        var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test", MAX, 1, req -> never);
        try (var client = new RpcClient(server.address(), MAX, 2)) {
            var outstanding = client.call(request("test", "block", ""), Duration.ofSeconds(30));
            server.close();
            assertThrows(ExecutionException.class, () -> outstanding.get(5, TimeUnit.SECONDS));
        } finally { server.close(); }
    }

    @Test void 双额度不可溢出且关闭后不能发送() {
        var window = new CreditWindow(100, 2);
        window.grant(100, 2);
        assertTrue(window.tryConsume(60));
        assertFalse(window.tryConsume(50));
        assertTrue(window.tryConsume(40));
        assertFalse(window.tryConsume(0));
        window.grant(100, 2);
        assertThrows(IllegalArgumentException.class, () -> window.grant(Long.MAX_VALUE, 1));
        window.close();
        assertFalse(window.tryConsume(1));
        assertThrows(IllegalStateException.class, () -> window.grant(1, 1));
    }

    @Test void 多连接共享节点并发上限而不是每连接无限放大() throws Exception {
        var started = new CountDownLatch(1);
        var blocked = new CompletableFuture<RpcResponse>();
        try (var server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test", MAX, 1, req -> { started.countDown(); return blocked; });
             var first = new RpcClient(server.address(), MAX, 2);
             var second = new RpcClient(server.address(), MAX, 2)) {
            var pending = first.call(request("test", "wait", ""), Duration.ofSeconds(5));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertEquals("BUSY", second.call(request("test", "wait", ""), Duration.ofSeconds(5)).get().getCode());
            blocked.complete(RpcResponse.newBuilder().setCode("OK").build());
            assertEquals("OK", pending.get(5, TimeUnit.SECONDS).getCode());
            assertEquals("OK", second.call(request("test", "wait", ""), Duration.ofSeconds(5)).get().getCode());
        }
    }

    private static RpcRequest request(String cluster, String method, String body) {
        return RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId(cluster))
                .setMethod(method).setBody(ByteString.copyFromUtf8(body)).build();
    }
}
