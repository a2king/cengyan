package io.cengyan.rpc;

import io.cengyan.protocol.proto.ClusterProtocol.RpcRequest;
import io.cengyan.protocol.proto.ClusterProtocol.RpcResponse;
import io.cengyan.protocol.proto.ClusterProtocol.Credit;
import com.google.protobuf.ByteString;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** 内部 RPC 接入。服务实现必须异步提交工作，不在 I/O 线程扫描数据。 */
public final class RpcServer implements AutoCloseable {
    @FunctionalInterface
    public interface Service { CompletionStage<RpcResponse> handle(RpcRequest request); }
    public interface StreamingService extends Service { CompletionStage<StreamSource> open(RpcRequest request); }
    public interface StreamSource extends AutoCloseable {
        CompletionStage<Chunk> next(int maxBytes);
        default void updateLease(io.cengyan.protocol.proto.ClusterProtocol.SchemaLease lease) { }
        @Override void close();
    }
    public record Chunk(ByteString payload, boolean end) { }

    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers = new NioEventLoopGroup(2);
    private final ChannelGroup children = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final AtomicBoolean closed = new AtomicBoolean();
    private java.util.concurrent.Semaphore nodeInflight;
    private Channel listener;

    public RpcServer(InetSocketAddress address, String clusterId, int maxPayload, int maxInflight, Service service)
            throws InterruptedException {
        if (clusterId == null || clusterId.isBlank() || maxInflight < 1 || maxPayload < 1) {
            close();
            throw new IllegalArgumentException("RPC 集群与容量配置无效");
        }
        if (service == null) {
            close();
            throw new IllegalArgumentException("RPC 服务不能为空");
        }
        nodeInflight = new java.util.concurrent.Semaphore(maxInflight);
        try {
            listener = new ServerBootstrap().group(boss, workers).channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel channel) {
                            children.add(channel);
                            channel.pipeline().addLast(new RpcFrameCodec.Decoder(maxPayload),
                                    new RpcFrameCodec.Encoder(maxPayload), new SimpleChannelInboundHandler<RpcFrame>() {
                                // 仅在该 channel 的 event loop 中操作。
                                private final Map<Long, CompletableFuture<RpcResponse>> active = new HashMap<>();
                                private final Map<Long, StreamState> streams = new HashMap<>();

                                final class StreamState {
                                    final long id;
                                    StreamSource source;
                                    long bytes;
                                    int batches;
                                    boolean pulling;
                                    boolean terminal;
                                    io.cengyan.protocol.proto.ClusterProtocol.SchemaLease lease;
                                    io.netty.util.concurrent.ScheduledFuture<?> idle;
                                    final ChannelHandlerContext ctx;
                                    StreamState(long id, ChannelHandlerContext ctx) { this.id = id; this.ctx = ctx; touch(); }
                                    void touch() {
                                        if (idle != null) idle.cancel(false);
                                        idle = ctx.executor().schedule(() -> {
                                            ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.ERROR, id, id,
                                                    error("STREAM_TIMEOUT", "结果流长时间未获得消费额度").toByteString()));
                                            close();
                                        }, 30, java.util.concurrent.TimeUnit.SECONDS);
                                    }
                                    void close() {
                                        if (terminal) return;
                                        terminal = true; streams.remove(id); nodeInflight.release();
                                        if (idle != null) idle.cancel(false);
                                        if (source != null) source.close();
                                    }
                                    void pump() {
                                        if (terminal || pulling || source == null || bytes < 1024 || batches <= 0 || !ctx.channel().isWritable()) return;
                                        pulling = true;
                                        int limit = (int) Math.min(bytes, Math.min(maxPayload - 64, 1024 * 1024));
                                        try {
                                            source.next(limit).whenComplete((chunk, failure) -> ctx.executor().execute(() -> {
                                                pulling = false;
                                                if (terminal) return;
                                                if (failure != null || chunk == null || chunk.payload().size() > limit) {
                                                    ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.ERROR, id, id,
                                                            error("STREAM_FAILED", "分片结果流执行失败，不能续传").toByteString())); close(); return;
                                                }
                                                // 单批拉取信用：未使用的字节不自动累计到下一批。
                                                bytes = 0; batches--;
                                                ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.DATA_BATCH, id, id, chunk.payload()))
                                                        .addListener(sent -> { if (!sent.isSuccess()) close(); });
                                                if (chunk.end()) {
                                                    ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.END, id, id, ByteString.EMPTY)); close();
                                                } else pump();
                                            }));
                                        } catch (RuntimeException ex) {
                                            ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.ERROR, id, id, error("STREAM_FAILED", "分片结果流执行失败").toByteString())); close();
                                        }
                                    }
                                }

                                @Override protected void channelRead0(ChannelHandlerContext ctx, RpcFrame frame) throws Exception {
                                    if (frame.streamId() != 0) {
                                        if (frame.streamId() != frame.correlationId()) throw new IllegalArgumentException("流标识无效");
                                        if (frame.type() == RpcFrame.Type.CANCEL) {
                                            StreamState stream = streams.get(frame.streamId()); if (stream != null) stream.close(); return;
                                        }
                                        if (frame.type() == RpcFrame.Type.CREDIT) {
                                            StreamState stream = streams.get(frame.streamId());
                                            if (stream == null) return;
                                            Credit credit = Credit.parseFrom(frame.payload());
                                            if (credit.getBytes() < 0 || credit.getBatches() < 0 || credit.getBytes() > 4 * 1024 * 1024L - stream.bytes
                                                    || credit.getBatches() > 8 - stream.batches) throw new IllegalArgumentException("流窗口超限");
                                            if (credit.hasSchemaLease()) {
                                                stream.lease = credit.getSchemaLease();
                                                if (stream.source != null) stream.source.updateLease(stream.lease);
                                            }
                                            stream.touch();
                                            stream.bytes += credit.getBytes(); stream.batches += credit.getBatches(); stream.pump(); return;
                                        }
                                        if (frame.type() != RpcFrame.Type.OPEN || !(service instanceof StreamingService streaming)) throw new IllegalArgumentException("不支持的流消息");
                                        RpcRequest request = RpcRequest.parseFrom(frame.payload());
                                        if (!clusterId.equals(request.getContext().getClusterId())) throw new IllegalArgumentException("流集群身份不匹配");
                                        if (streams.containsKey(frame.streamId()) || streams.size() + active.size() >= maxInflight || !nodeInflight.tryAcquire()) {
                                            ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.ERROR, frame.correlationId(), frame.streamId(), error("BUSY", "连接结果流数量超限").toByteString())); return;
                                        }
                                        StreamState state = new StreamState(frame.streamId(), ctx); streams.put(state.id, state);
                                        CompletionStage<StreamSource> opening;
                                        try { opening = streaming.open(request); }
                                        catch (RuntimeException ex) { state.close(); throw ex; }
                                        opening.whenComplete((source, failure) -> ctx.executor().execute(() -> {
                                            if (state.terminal) { if (source != null) source.close(); return; }
                                            if (failure != null || source == null) {
                                                ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.ERROR, state.id, state.id, error("STREAM_FAILED", "分片结果流打开失败").toByteString())); state.close();
                                            } else {
                                                state.source = source;
                                                if (state.lease != null) source.updateLease(state.lease);
                                                state.pump();
                                            }
                                        }));
                                        return;
                                    }
                                    if (frame.type() == RpcFrame.Type.CANCEL) {
                                        CompletableFuture<RpcResponse> task = active.remove(frame.correlationId());
                                        if (task != null) { nodeInflight.release(); task.cancel(true); }
                                        return;
                                    }
                                    if (frame.type() != RpcFrame.Type.REQUEST) throw new IllegalArgumentException("不支持的 RPC 消息");
                                    if (active.containsKey(frame.correlationId())) throw new IllegalArgumentException("重复的 RPC 关联号");
                                    RpcRequest request = RpcRequest.parseFrom(frame.payload());
                                    if (!clusterId.equals(request.getContext().getClusterId())) {
                                        respond(ctx, frame.correlationId(), error("CLUSTER_MISMATCH", "目标集群不匹配"));
                                        return;
                                    }
                                    if (active.size() >= maxInflight || !nodeInflight.tryAcquire()) {
                                        respond(ctx, frame.correlationId(), error("BUSY", "连接并发请求已达上限"));
                                        return;
                                    }
                                    CompletableFuture<RpcResponse> task;
                                    try {
                                        task = Objects.requireNonNull(service.handle(request)).toCompletableFuture();
                                    } catch (RuntimeException ex) {
                                        nodeInflight.release();
                                        respond(ctx, frame.correlationId(), error("INTERNAL", "RPC 执行失败"));
                                        return;
                                    }
                                    active.put(frame.correlationId(), task);
                                    task.whenComplete((response, failure) -> ctx.executor().execute(() -> {
                                        if (active.remove(frame.correlationId(), task)) {
                                            nodeInflight.release();
                                            if (ctx.channel().isActive()) respond(ctx, frame.correlationId(), failure == null && response != null
                                                    ? response : error("INTERNAL", "RPC 执行失败"));
                                        }
                                    }));
                                }

                                @Override public void channelInactive(ChannelHandlerContext ctx) {
                                    var tasks = java.util.List.copyOf(active.values());
                                    nodeInflight.release(active.size());
                                    active.clear();
                                    tasks.forEach(task -> task.cancel(true));
                                    java.util.List.copyOf(streams.values()).forEach(StreamState::close);
                                }
                                @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) {
                                    if (ctx.channel().isWritable()) java.util.List.copyOf(streams.values()).forEach(StreamState::pump);
                                    ctx.fireChannelWritabilityChanged();
                                }
                                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
                            });
                        }
                    }).bind(address).sync().channel();
        } catch (RuntimeException | InterruptedException ex) {
            close();
            throw ex;
        }
    }

    private static RpcResponse error(String code, String message) {
        return RpcResponse.newBuilder().setCode(code).setMessage(message).build();
    }

    private static void respond(ChannelHandlerContext ctx, long id, RpcResponse response) {
        ctx.writeAndFlush(new RpcFrame(RpcFrame.Type.RESPONSE, id, 0, response.toByteString()))
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    public InetSocketAddress address() { return (InetSocketAddress) listener.localAddress(); }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (listener != null) listener.close().awaitUninterruptibly();
        children.close().awaitUninterruptibly();
        workers.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).awaitUninterruptibly();
        boss.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
