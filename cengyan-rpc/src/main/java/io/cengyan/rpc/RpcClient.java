package io.cengyan.rpc;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.RpcRequest;
import io.cengyan.protocol.proto.ClusterProtocol.RpcResponse;
import io.cengyan.protocol.proto.ClusterProtocol.Credit;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 一条多路复用连接。重试由调用方根据读写语义决定，本层不自动重放。 */
public final class RpcClient implements AutoCloseable {
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final ConcurrentMap<Long, CompletableFuture<RpcResponse>> pending = new ConcurrentHashMap<>();
    private final Semaphore capacity;
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private Channel channel;
    private final ConcurrentMap<Long, ResultStream> streams = new ConcurrentHashMap<>();
    /** System.nanoTime() of the last frame received; lets callers tell a dead connection from one slow request. */
    private volatile long lastReceivedNanos = System.nanoTime();
    public boolean isActive() { return !closed.get() && channel != null && channel.isActive(); }
    public long lastReceivedNanos() { return lastReceivedNanos; }

    /** 一个在途 batch 的消费拉取器：消费后才补发 Credit，内存窗口不随扫描总量增长。 */
    public final class ResultStream implements AutoCloseable {
        private final long id;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private CompletableFuture<ByteString> waiting;
        private ByteString delivered;
        private boolean ended;
        private boolean canceled;
        private Throwable failure;
        private boolean granted;
        private java.util.function.Supplier<io.cengyan.protocol.proto.ClusterProtocol.SchemaLease> leaseSupplier = () -> null;
        public synchronized void leaseSupplier(java.util.function.Supplier<io.cengyan.protocol.proto.ClusterProtocol.SchemaLease> supplier) {
            this.leaseSupplier = java.util.Objects.requireNonNull(supplier);
        }
        private final ScheduledFuture<?> timeout;
        private ResultStream(long id, Duration deadline) {
            this.id = id;
            timeout = channel.eventLoop().schedule(() -> fail(new TimeoutException("结果流超时")), deadline.toNanos(), TimeUnit.NANOSECONDS);
        }
        public synchronized CompletableFuture<ByteString> next() {
            if (waiting != null) throw new IllegalStateException("同一流只能有一个未完成消费请求");
            if (failure != null) return CompletableFuture.failedFuture(failure);
            if (delivered != null) { ByteString result = delivered; delivered = null; return CompletableFuture.completedFuture(result); }
            if (ended || canceled) return CompletableFuture.completedFuture(null);
            waiting = new CompletableFuture<>();
            CompletableFuture<ByteString> result = waiting;
            if (!granted) {
                granted = true;
                var credit = Credit.newBuilder().setBytes(1024 * 1024).setBatches(1);
                var lease = leaseSupplier.get();
                if (lease != null) credit.setSchemaLease(lease);
                channel.writeAndFlush(new RpcFrame(RpcFrame.Type.CREDIT, id, id,
                        credit.build().toByteString()));
            }
            return result;
        }
        private synchronized void receive(ByteString bytes) {
            if (canceled || ended || failure != null) return;
            if (!granted || delivered != null) { fail(new IOException("服务端超额发送结果流")); return; }
            // 每次只授予一批额度，下一次消费才补充；服务端仍有的字节余额必须清零才能精确续授。
            granted = false;
            if (waiting != null) { var consumer = waiting; waiting = null; consumer.complete(bytes); }
            else delivered = bytes;
        }
        private synchronized void end() {
            if (ended || canceled || failure != null) return;
            ended = true; timeout.cancel(false);
            if (streams.remove(id, this)) capacity.release();
            if (waiting != null) { waiting.complete(null); waiting = null; }
            completion.complete(null);
        }
        private synchronized void fail(Throwable cause) {
            if (canceled || ended || failure != null) return;
            failure = cause; timeout.cancel(false);
            if (streams.remove(id, this)) capacity.release();
            if (waiting != null) { waiting.completeExceptionally(cause); waiting = null; }
            completion.completeExceptionally(cause);
            if (channel.isActive()) channel.writeAndFlush(new RpcFrame(RpcFrame.Type.CANCEL, id, id, ByteString.EMPTY));
        }
        @Override public synchronized void close() {
            if (canceled || ended) return;
            canceled = true; timeout.cancel(false);
            if (streams.remove(id, this)) capacity.release();
            if (waiting != null) { waiting.completeExceptionally(new CancellationException("结果流取消")); waiting = null; }
            delivered = null;
            if (channel.isActive()) channel.writeAndFlush(new RpcFrame(RpcFrame.Type.CANCEL, id, id, ByteString.EMPTY));
        }
    }

    public ResultStream open(RpcRequest request, Duration timeout) {
        java.util.Objects.requireNonNull(request);
        if (timeout == null || timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("结果流必须有正数截止时间");
        if (closed.get() || !channel.isActive() || !capacity.tryAcquire()) throw new RejectedExecutionException("RPC 流不可接纳");
        long id = sequence.incrementAndGet();
        ResultStream stream;
        try { stream = new ResultStream(id, timeout); }
        catch (RuntimeException ex) { capacity.release(); throw ex; }
        streams.put(id, stream);
        channel.writeAndFlush(new RpcFrame(RpcFrame.Type.OPEN, id, id, request.toByteString()))
                .addListener(sent -> { if (!sent.isSuccess()) stream.fail(sent.cause()); });
        return stream;
    }

    public RpcClient(InetSocketAddress address, int maxPayload, int maxInflight) throws InterruptedException {
        this(address, maxPayload, maxInflight, Duration.ofSeconds(3));
    }
    public RpcClient(InetSocketAddress address, int maxPayload, int maxInflight, Duration connectTimeout) throws InterruptedException {
        capacity = new Semaphore(Math.max(0, maxInflight));
        if (maxInflight < 1 || maxPayload < 1 || connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
            close();
            throw new IllegalArgumentException("RPC 容量必须为正数");
        }
        try {
            channel = new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.max(1, Math.min(3000, connectTimeout.toMillis()))).option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel socket) {
                            socket.pipeline().addLast(new RpcFrameCodec.Decoder(maxPayload), new RpcFrameCodec.Encoder(maxPayload),
                                    new SimpleChannelInboundHandler<RpcFrame>() {
                                @Override protected void channelRead0(ChannelHandlerContext ctx, RpcFrame frame) throws Exception {
                                    lastReceivedNanos = System.nanoTime();
                                    if (frame.streamId() != 0) {
                                        ResultStream stream = streams.get(frame.streamId());
                                        if (stream == null) return;
                                        if (frame.type() == RpcFrame.Type.DATA_BATCH) stream.receive(frame.payload());
                                        else if (frame.type() == RpcFrame.Type.END) stream.end();
                                        else if (frame.type() == RpcFrame.Type.ERROR) stream.fail(new IOException(RpcResponse.parseFrom(frame.payload()).getMessage()));
                                        else throw new IOException("结果流响应类型无效");
                                        return;
                                    }
                                    if (frame.type() != RpcFrame.Type.RESPONSE || frame.streamId() != 0) {
                                        throw new IOException("RPC 响应类型无效");
                                    }
                                    RpcResponse response = RpcResponse.parseFrom(frame.payload());
                                    CompletableFuture<RpcResponse> result = pending.get(frame.correlationId());
                                    if (result != null) result.complete(response);
                                }
                                @Override public void channelInactive(ChannelHandlerContext ctx) {
                                    failPending(new IOException("RPC 连接已断开"));
                                }
                                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    failPending(cause);
                                    ctx.close();
                                }
                            });
                        }
                    }).connect(address).sync().channel();
        } catch (InterruptedException ex) {
            close();
            throw ex;
        } catch (Exception ex) {
            close();
            throw new IllegalStateException("RPC 连接失败：" + address, ex);
        }
    }

    public CompletableFuture<RpcResponse> call(RpcRequest request, Duration timeout) {
        java.util.Objects.requireNonNull(request, "RPC 请求不能为空");
        if (timeout == null || timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("RPC 必须有有效截止时间");
        long nanos = timeout.toNanos();
        if (closed.get() || !channel.isActive()) return CompletableFuture.failedFuture(new IOException("RPC 连接不可用"));
        if (!capacity.tryAcquire()) return CompletableFuture.failedFuture(new RejectedExecutionException("RPC 并发请求已达上限"));
        long id = sequence.incrementAndGet();
        if (id <= 0) {
            capacity.release();
            return CompletableFuture.failedFuture(new IllegalStateException("RPC 关联号已用尽"));
        }
        CompletableFuture<RpcResponse> result = new CompletableFuture<>();
        pending.put(id, result);
        final ScheduledFuture<?> deadline;
        try {
            deadline = channel.eventLoop().schedule(() -> result.completeExceptionally(new TimeoutException("RPC 等待超时")), nanos, TimeUnit.NANOSECONDS);
        } catch (RuntimeException ex) {
            pending.remove(id);
            capacity.release();
            result.completeExceptionally(ex);
            return result;
        }
        result.whenComplete((response, failure) -> {
            deadline.cancel(false);
            if (pending.remove(id, result)) capacity.release();
            if (failure != null && channel.isActive()) {
                channel.writeAndFlush(new RpcFrame(RpcFrame.Type.CANCEL, id, 0, ByteString.EMPTY));
            }
        });
        if (closed.get() || !channel.isActive()) {
            result.completeExceptionally(new IOException("RPC 连接已断开"));
        } else {
            try {
                channel.eventLoop().execute(() -> {
                    if (result.isDone()) return;
                    channel.writeAndFlush(new RpcFrame(RpcFrame.Type.REQUEST, id, 0, request.toByteString()))
                            .addListener(sent -> { if (!sent.isSuccess()) result.completeExceptionally(sent.cause()); });
                });
            } catch (RejectedExecutionException ex) { result.completeExceptionally(ex); }
        }
        return result;
    }

    private void failPending(Throwable cause) {
        pending.values().forEach(task -> task.completeExceptionally(cause));
        streams.values().forEach(stream -> stream.fail(cause));
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (channel != null) channel.close().awaitUninterruptibly();
        failPending(new IOException("RPC 客户端已关闭"));
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
