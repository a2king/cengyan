package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcClient;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** 连接池及 Leader 缓存。路由错误可重定向，不在此层重算查询或生成新的写 requestId。 */
public final class StorageGateway implements AutoCloseable {
    // A transaction CAS carries both the old and new record (each may contain a 6 MiB plan).
    public static final int MAX_RPC_PAYLOAD = 32 * 1024 * 1024;
    public record Partition(long graph, int part) { }
    private final String cluster;
    private final List<InetSocketAddress> seeds;
    private final ConcurrentMap<InetSocketAddress, RpcClient> connections = new ConcurrentHashMap<>();
    private final java.util.concurrent.locks.ReentrantLock[] connectionLocks = java.util.stream.IntStream.range(0, 32)
            .mapToObj(i -> new java.util.concurrent.locks.ReentrantLock()).toArray(java.util.concurrent.locks.ReentrantLock[]::new);
    private record Route(List<InetSocketAddress> members, long refreshedAt) { }
    private static final class RouteState {
        final java.util.concurrent.locks.ReentrantLock refresh = new java.util.concurrent.locks.ReentrantLock();
        volatile Route route;
        volatile long attemptedAt;
        volatile ClusterException failure;
    }
    private static final long ROUTE_TTL = TimeUnit.SECONDS.toNanos(30);
    private static final long REFRESH_INTERVAL = TimeUnit.MILLISECONDS.toNanos(100);
    private final ConcurrentMap<Partition, RouteState> routes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Partition, InetSocketAddress> leaders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InetSocketAddress> cursorOwners = new ConcurrentHashMap<>();
    private volatile boolean closed;
    private static final ThreadLocal<RequestBudget> CURRENT = new ThreadLocal<>();
    private static final ThreadLocal<ControllerGuard> CONTROL = new ThreadLocal<>();
    private static final ExecutorService READERS = new ThreadPoolExecutor(16, 16, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(256), task -> {
                Thread thread = new Thread(task, "distributed-partition-reader"); thread.setDaemon(true); return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
    private record ControllerGuard(long epoch, java.util.function.BooleanSupplier valid) { }
    public static AutoCloseable controller(long epoch, java.util.function.BooleanSupplier valid) {
        ControllerGuard previous = CONTROL.get();
        CONTROL.set(new ControllerGuard(epoch, valid));
        return () -> { if (previous == null) CONTROL.remove(); else CONTROL.set(previous); };
    }
    private static long controllerEpoch() {
        ControllerGuard guard = CONTROL.get();
        if (guard == null) return 0;
        if (!guard.valid().getAsBoolean()) throw new ClusterException("STALE_CONTROLLER", "控制面任期已经变化，停止原 Leader 任务");
        return guard.epoch();
    }
    private static final class RequestBudget {
        final long memory;
        final java.util.function.BooleanSupplier canceled;
        final java.util.concurrent.atomic.AtomicLong charged = new java.util.concurrent.atomic.AtomicLong();
        final long deadline;
        final java.util.function.Supplier<SchemaLease> lease;
        RequestBudget(long memory, java.util.function.BooleanSupplier canceled, long deadline, java.util.function.Supplier<SchemaLease> lease) {
            this.memory = memory; this.canceled = canceled; this.deadline = deadline; this.lease = lease;
        }
    }
    public static AutoCloseable budget(long memory, java.util.function.BooleanSupplier canceled) {
        return budget(memory, canceled, System.nanoTime() + TimeUnit.MINUTES.toNanos(30));
    }
    public static AutoCloseable budget(long memory, java.util.function.BooleanSupplier canceled, long deadline) {
        return budget(memory, canceled, deadline, () -> null);
    }
    public static AutoCloseable budget(long memory, java.util.function.BooleanSupplier canceled, long deadline,
                                       java.util.function.Supplier<SchemaLease> lease) {
        RequestBudget previous = CURRENT.get();
        CURRENT.set(new RequestBudget(memory, canceled, deadline, lease));
        return () -> { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); };
    }
    private RequestContext.Builder context(long graph, int part, String requestId, long remainingMs) {
        var context = RequestContext.newBuilder().setClusterId(cluster).setGraphId(graph).setPartId(part)
                .setRequestId(requestId).setControllerEpoch(controllerEpoch()).setRemainingTimeoutMs(remainingMs);
        RequestBudget budget = CURRENT.get();
        SchemaLease lease = budget == null ? null : budget.lease.get();
        if (graph > 0 && lease != null) {
            if (lease.getGraphId() != graph) {
                throw new ClusterException("SCHEMA_VERSION_MISMATCH", "分片请求与 Schema Lease 身份不匹配");
            }
            context.setSchemaLease(lease).setLeaseId(lease.getLeaseId()).setSchemaVersion(lease.getSchemaVersion());
        }
        return context;
    }
    public static Duration remaining(Duration requested) {
        RequestBudget budget = CURRENT.get();
        if (budget == null) return requested;
        long nanos = Math.min(requested.toNanos(), budget.deadline - System.nanoTime());
        if (nanos <= 0) throw new ClusterException("QUERY_TIMEOUT", "请求已到达原始执行截止时间");
        return Duration.ofNanos(nanos);
    }
    public static void charge(long bytes) {
        RequestBudget budget = CURRENT.get();
        if (budget == null) return;
        if (System.nanoTime() - budget.deadline >= 0) throw new ClusterException("QUERY_TIMEOUT", "请求已到达原始执行截止时间");
        if (budget.canceled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new ClusterException("QUERY_CANCELED", "请求已取消或超时");
        if (bytes < 0) throw new IllegalArgumentException("预算不能为负数");
        for (;;) {
            long previous = budget.charged.get();
            if (bytes > budget.memory - previous) throw new ClusterException("QUERY_MEMORY_EXCEEDED", "查询中间数据超过内存额度");
            if (budget.charged.compareAndSet(previous, previous + bytes)) break;
        }
    }

    /** 每个请求最多四个并行分片，所有工作共享一个预算对象；首个失败取消其余任务。 */
    public static <T> List<T> parallel(List<java.util.function.Supplier<T>> tasks) {
        RequestBudget budget = CURRENT.get(); ControllerGuard controller = CONTROL.get();
        List<Future<T>> submitted = new ArrayList<>(); List<T> results = new ArrayList<>();
        try {
            int next = 0;
            for (; next < Math.min(4, tasks.size()); next++) submitted.add(submit(tasks.get(next), budget, controller));
            for (int completed = 0; completed < tasks.size(); completed++) {
                Future<T> task = submitted.get(completed);
                for (;;) {
                    charge(0);
                    try { results.add(task.get(200, TimeUnit.MILLISECONDS)); break; }
                    catch (TimeoutException waiting) { }
                }
                if (next < tasks.size()) submitted.add(submit(tasks.get(next++), budget, controller));
            }
            return results;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "并行分片任务被取消", ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) throw cause;
            throw new ClusterException("FAILED", "并行分片任务失败", ex.getCause());
        } catch (RejectedExecutionException ex) { throw new ClusterException("BUSY", "节点并行分片扫描队列已满", ex); }
        finally { submitted.forEach(task -> { if (!task.isDone()) task.cancel(true); }); }
    }
    private static <T> Future<T> submit(java.util.function.Supplier<T> task, RequestBudget budget, ControllerGuard controller) {
        return READERS.submit(() -> {
            if (budget != null) CURRENT.set(budget);
            if (controller != null) CONTROL.set(controller);
            try { return task.get(); }
            finally { CURRENT.remove(); CONTROL.remove(); }
        });
    }

    /** Capture request/controller fencing for work handed to a dedicated write executor. */
    static Runnable contextual(Runnable task) {
        RequestBudget budget = CURRENT.get(); ControllerGuard controller = CONTROL.get();
        return () -> {
            RequestBudget previous = CURRENT.get(); ControllerGuard oldController = CONTROL.get();
            if (budget == null) CURRENT.remove(); else CURRENT.set(budget);
            if (controller == null) CONTROL.remove(); else CONTROL.set(controller);
            try { task.run(); }
            finally {
                if (previous == null) CURRENT.remove(); else CURRENT.set(previous);
                if (oldController == null) CONTROL.remove(); else CONTROL.set(oldController);
            }
        };
    }

    public StorageGateway(String cluster, List<InetSocketAddress> seeds) {
        if (cluster == null || cluster.isBlank() || seeds.isEmpty()) throw new IllegalArgumentException("Storage 种子不能为空");
        this.cluster = cluster;
        this.seeds = List.copyOf(seeds);
    }
    public String clusterId() { return cluster; }
    public void route(long graph, int part, List<InetSocketAddress> replicas) {
        if (replicas.isEmpty()) throw new IllegalArgumentException("副本列表不能为空");
        Partition partition = new Partition(graph, part);
        RouteState state = routes.computeIfAbsent(partition, ignored -> new RouteState());
        state.refresh.lock();
        try { publish(partition, state, replicas); }
        finally { state.refresh.unlock(); }
    }
    private Route publish(Partition partition, RouteState state, List<InetSocketAddress> replicas) {
        Route next = new Route(List.copyOf(replicas), System.nanoTime());
        state.route = next;
        state.failure = null;
        state.attemptedAt = next.refreshedAt();
        leaders.computeIfPresent(partition, (key, leader) -> replicas.contains(leader) ? leader : null);
        return next;
    }
    private static Duration timeLeft(long deadline) {
        charge(0);
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw new ClusterException("QUERY_TIMEOUT", "Storage 调用已到达原始截止时间");
        return Duration.ofNanos(nanos);
    }
    /** Per-partition single-flight refresh; waiting and Metadata reads share the caller's deadline. */
    private Route refreshRoute(Partition partition, long deadline, boolean force, Route observed) {
        RouteState state = routes.computeIfAbsent(partition, ignored -> new RouteState());
        Route cached = state.route;
        if (!force && cached != null && state.failure == null && System.nanoTime() - cached.refreshedAt() < ROUTE_TTL) return cached;
        try {
            while (!state.refresh.tryLock(Math.min(timeLeft(deadline).toNanos(), TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS)) {
                timeLeft(deadline);
            }
            try {
                Route current = state.route;
                long now = System.nanoTime();
                // Another request already refreshed this snapshot, or refresh is still in its cooldown.
                if (current != null && (force ? current != observed || now - state.attemptedAt < REFRESH_INTERVAL
                        : now - current.refreshedAt() < ROUTE_TTL)) {
                    if (state.failure != null && now - state.attemptedAt < REFRESH_INTERVAL) throw state.failure;
                    if (state.failure == null) return current;
                }
                if (state.failure != null && now - state.attemptedAt < REFRESH_INTERVAL) throw state.failure;
                try {
                    RpcResponse response = callUntil(0, 0, "kv.read", "", ReadKey.newBuilder()
                            .setKey(groupKey(partition.graph(), partition.part())).build().toByteString(), deadline);
                    StoredValue value = StoredValue.parseFrom(response.getBody());
                    if (!value.getFound()) throw new ClusterException("GROUP_UNAVAILABLE", "分片路由不存在：" + partition);
                    DataGroupSpec spec = DataGroupSpec.parseFrom(value.getValue());
                    if (spec.getGraphId() != partition.graph() || spec.getPartId() != partition.part() || spec.getMembersCount() == 0) {
                        throw new ClusterException("CORRUPT_DATA", "分片路由身份无效：" + partition);
                    }
                    List<InetSocketAddress> replicas = new ArrayList<>();
                    Set<String> ids = new HashSet<>();
                    for (Node node : spec.getMembersList()) {
                        if (!node.getClusterId().equals(cluster) || node.getRole() != NodeRole.STORAGE || !ids.add(node.getNodeId())
                                || !validEndpoint(node.getRpc())) throw new ClusterException("CORRUPT_DATA", "分片路由成员无效");
                        replicas.add(new InetSocketAddress(node.getRpc().getHost(), node.getRpc().getPort()));
                    }
                    timeLeft(deadline);
                    return publish(partition, state, replicas);
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                    throw new ClusterException("CORRUPT_DATA", "分片路由元数据损坏", ex);
                } catch (ClusterException ex) {
                    state.failure = ex;
                    throw ex;
                } finally { state.attemptedAt = System.nanoTime(); }
            } finally { state.refresh.unlock(); }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "路由刷新已取消", ex);
        }
    }
    private static boolean validEndpoint(Endpoint endpoint) {
        return !endpoint.getHost().isBlank() && endpoint.getPort() > 0 && endpoint.getPort() <= 65535;
    }
    private static void backoff(long deadline, int attempt) throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(Math.min(timeLeft(deadline).toNanos(),
                TimeUnit.MILLISECONDS.toNanos(Math.min(100, 10L * (1 + attempt)))));
    }

    public RpcResponse call(long graph, int part, String method, String requestId, ByteString body, Duration timeout) {
        charge(0);
        controllerEpoch();
        return callUntil(graph, part, method, requestId, body, System.nanoTime() + remaining(timeout).toNanos());
    }
    private RpcResponse callUntil(long graph, int part, String method, String requestId, ByteString body, long deadline) {
        Partition partition = new Partition(graph, part);
        String continuingCursor = "", openingScan = "";
        if (method.equals("kv.read")) {
            try {
                ReadKey read = ReadKey.parseFrom(body);
                continuingCursor = read.getCursorId();
                openingScan = read.getScanId();
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("INVALID_ARGUMENT", "读取请求编码无效", ex); }
        }
        boolean pinned = !continuingCursor.isBlank();
        Route route = graph > 0 && !pinned ? refreshRoute(partition, deadline, false, null) : null;
        List<InetSocketAddress> candidates = route == null ? seeds : route.members();
        InetSocketAddress address = pinned ? cursorOwners.get(continuingCursor) : leaders.getOrDefault(partition, candidates.getFirst());
        if (address == null) throw new ClusterException("QUERY_CANCELED", "分片扫描游标所属连接已失效");
        Set<InetSocketAddress> visited = new HashSet<>();
        Throwable last = null;
        // Leader Hint 失效时允许有限跳转；实际等待仍严格受本次请求 deadline 约束。
        // 选举期间单次 NOT_LEADER 可能只间隔数十毫秒，16 次不足以覆盖启动窗口。
        for (int attempt = 0; attempt < 2048; attempt++) {
            charge(0);
            if (closed) break;
            timeLeft(deadline);
            boolean submitted = false;
            RpcClient client = null;
            CompletableFuture<RpcResponse> pendingCall = null;
            long sentAt = 0;
            try {
                client = connection(address, deadline);
                long remaining = timeLeft(deadline).toNanos();
                RpcRequest request = RpcRequest.newBuilder().setContext(context(graph, part, requestId,
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining))))
                        .setMethod(method).setBody(body).build();
                if (request.getSerializedSize() > MAX_RPC_PAYLOAD) throw new ClusterException("BATCH_TOO_LARGE",
                        "Storage RPC 超过上限；method=" + method + " bytes=" + request.getSerializedSize() + " limit=" + MAX_RPC_PAYLOAD);
                submitted = true;
                sentAt = System.nanoTime();
                pendingCall = client.call(request, Duration.ofNanos(remaining));
                RpcResponse response = pendingCall.get(remaining, TimeUnit.NANOSECONDS);
                if (Set.of("NOT_LEADER", "GROUP_UNAVAILABLE", "REPLICA_UNTRUSTED").contains(response.getCode())) {
                    if (pinned) throw new ClusterException("QUERY_CANCELED", "扫描已返回数据后分片位置变化，当前查询失败，不重新扫描");
                    last = new IllegalStateException(response.toString());
                    leaders.remove(partition, address);
                    visited.add(address);
                    InetSocketAddress hinted = null;
                    if (response.hasLeaderHint() && response.getLeaderHint().hasRpc() && validEndpoint(response.getLeaderHint().getRpc())) {
                        Endpoint hint = response.getLeaderHint().getRpc();
                        hinted = new InetSocketAddress(hint.getHost(), hint.getPort());
                    }
                    if (hinted != null && !visited.contains(hinted)) address = hinted;
                    else {
                        if (graph > 0) {
                            route = refreshRoute(partition, deadline, true, route);
                            candidates = route.members();
                        }
                        address = candidates.get((attempt + 1) % candidates.size());
                        visited.clear();
                    }
                    backoff(deadline, attempt);
                    continue;
                }
                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
                leaders.put(partition, address);
                if (!openingScan.isBlank()) cursorOwners.put(openingScan, address);
                return response;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new ClusterException("QUERY_CANCELED", "Storage 调用被取消", ex);
            } catch (ExecutionException | TimeoutException ex) {
                Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
                if (pendingCall != null) pendingCall.cancel(false);
                // The local in-flight cap rejected the call before it was written: the server never saw it.
                // Safe to retry for writes too; never tear down a connection other requests are using.
                if (cause instanceof RejectedExecutionException && System.nanoTime() < deadline) {
                    last = cause;
                    try { backoff(deadline, attempt); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "Storage 调用已取消", interrupted); }
                    continue;
                }
                // One request failing must not close the connection shared by every other in-flight request:
                // that turned a single slow or rejected call into "connection broken" for all of them.
                // Replace the connection only when it is really dead, or nothing arrived on it since this request was sent.
                if (client != null && (!client.isActive() || client.lastReceivedNanos() - sentAt < 0)) {
                    if (connections.remove(address, client)) client.close();
                }
                if (method.equals("kv.read") && !pinned && System.nanoTime() < deadline) {
                    leaders.remove(partition);
                    if (graph > 0) {
                        route = refreshRoute(partition, deadline, true, route);
                        candidates = route.members();
                    }
                    address = candidates.get((attempt + 1) % candidates.size());
                    last = ex;
                    try { backoff(deadline, attempt); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "Storage 调用已取消", interrupted); }
                    continue;
                }
                throw new ClusterException("FAILED", "Storage 响应中断，写入状态可能未知；method=" + method
                        + " graph=" + graph + " part=" + part + " requestId=" + requestId
                        + " bodyBytes=" + body.size() + " cause=" + cause.getClass().getSimpleName()
                        + (cause.getMessage() == null ? "" : "(" + cause.getMessage() + ")"), ex);
            } catch (ClusterException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                if (pinned) throw new ClusterException("QUERY_CANCELED", "扫描连接不可用，不重新扫描", ex);
                if (submitted && !method.equals("kv.read")) {
                    throw new ClusterException("FAILED", "Storage 提交后发生异常，写入状态可能未知，不自动重放", ex);
                }
                last = ex;
                if (client != null && !client.isActive() && connections.remove(address, client)) client.close();
                if (graph > 0) {
                    route = refreshRoute(partition, deadline, true, route);
                    candidates = route.members();
                }
                address = candidates.get((attempt + 1) % candidates.size());
                try { backoff(deadline, attempt); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "Storage 调用已取消", interrupted); }
            }
        }
        throw new ClusterException("GROUP_UNAVAILABLE", "无法在截止时间内定位可用分片 Leader", last);
    }

    public RpcResponse direct(InetSocketAddress address, String method, ByteString body, Duration timeout) {
        long deadline = System.nanoTime() + remaining(timeout).toNanos();
        try {
            RpcClient client = connection(address, deadline);
            Duration left = timeLeft(deadline);
            return client.call(RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId(cluster)
                    .setControllerEpoch(controllerEpoch())
                    .setRemainingTimeoutMs(Math.max(1, left.toMillis()))).setMethod(method).setBody(body).build(), left)
                    .get(left.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "RPC 已取消", ex);
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("FAILED", "节点 RPC 失败", ex); }
    }

    public ByteString get(long graph, int part, ByteString key) {
        try {
            StoredValue value = StoredValue.parseFrom(call(graph, part, "kv.read", "",
                    ReadKey.newBuilder().setKey(key).build().toByteString(), Duration.ofSeconds(15)).getBody());
            return value.getFound() ? value.getValue() : null;
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("PROTOCOL_ERROR", "Storage 读响应损坏", ex); }
    }

    public EntityCounts entityCounts(long graph, int part, long generation) {
        try {
            EntityCounts counts = EntityCounts.parseFrom(call(graph, part, "kv.read", "",
                    ReadKey.newBuilder().setEntityCounts(true).setCountsGeneration(generation).build().toByteString(),
                    Duration.ofSeconds(15)).getBody());
            if (counts.getFormatVersion() != 1 || counts.getGeneration() != generation) {
                throw new ClusterException("PROTOCOL_ERROR", "分片不支持当前实体计数格式，请升级所有 Storage 节点");
            }
            return counts;
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
            throw new ClusterException("PROTOCOL_ERROR", "分片计数元数据响应损坏", ex);
        }
    }
    public List<ByteString> getMany(long graph, int part, List<ByteString> keys) {
        if (keys.isEmpty()) return List.of();
        List<ByteString> result = new ArrayList<>();
        for (int offset = 0; offset < keys.size(); offset += 256) {
            List<ByteString> batch = keys.subList(offset, Math.min(offset + 256, keys.size()));
            try {
                BatchValues values = BatchValues.parseFrom(call(graph, part, "kv.read", "",
                        ReadKey.newBuilder().addAllKeys(batch).build().toByteString(), Duration.ofSeconds(15)).getBody());
                if (values.getValuesCount() != batch.size()) throw new ClusterException("PROTOCOL_ERROR", "批量读响应数量不匹配");
                for (StoredValue value : values.getValuesList()) result.add(value.getFound() ? value.getValue() : null);
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("PROTOCOL_ERROR", "批量读响应损坏", ex); }
        }
        return result;
    }
    public ScanPage scan(long graph, int part, ByteString prefix, ByteString after, int limit) {
        return scan(graph, part, prefix, after, limit, "");
    }
    public ScanPage scan(long graph, int part, ByteString prefix, ByteString after, int limit, String cursorId) {
        return scan(graph, part, prefix, after, limit, cursorId, false);
    }
    public ScanPage scan(long graph, int part, ByteString prefix, ByteString after, int limit, String cursorId, boolean transactionSummaries) {
        try {
            String opening = cursorId.isEmpty() ? UUID.randomUUID().toString() : "";
            ScanPage page = ScanPage.parseFrom(call(graph, part, "kv.read", "", ReadKey.newBuilder().setKey(prefix)
                    .setScan(true).setAfterKey(after).setCursorId(cursorId).setScanId(opening).setTransactionSummaries(transactionSummaries)
                    .setLimit(limit).setMaxBytes(transactionSummaries ? 1024 * 1024 : 7 * 1024 * 1024).build().toByteString(), Duration.ofSeconds(15)).getBody());
            if (!page.getMore()) { cursorOwners.remove(cursorId); cursorOwners.remove(opening); }
            return page;
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("PROTOCOL_ERROR", "Storage 扫描响应损坏", ex); }
    }
    public void closeCursor(long graph, int part, String cursorId) {
        try {
            call(graph, part, "kv.read", "", ReadKey.newBuilder().setCloseCursor(true).setCursorId(cursorId).build().toByteString(), Duration.ofSeconds(2));
        } catch (Exception ignored) { /* 节点故障时由状态机 TTL 或关闭回收。 */ }
        finally { cursorOwners.remove(cursorId); }
    }
    public BatchResult write(long graph, int part, WriteBatch command) {
        long epoch = controllerEpoch();
        if (epoch > 0) command = command.toBuilder().setControllerEpoch(epoch).build();
        try {
            return BatchResult.parseFrom(call(graph, part, "kv.write", command.getRequestId(), command.toByteString(), Duration.ofSeconds(15)).getBody());
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("PROTOCOL_ERROR", "Storage 写响应损坏", ex); }
    }
    public interface ScanConsumer { void accept(ScanPage page); }
    /** 首次读屏障定位 Leader，随后 OPEN 流固定该节点，部分结果失败不重新开流。 */
    public void stream(long graph, int part, ByteString prefix, ScanConsumer consumer) {
        stream(graph, part, prefix, "", List.of(), consumer);
    }
    public void stream(long graph, int part, ByteString prefix, String type, List<PropertyEquals> filters, ScanConsumer consumer) {
        charge(0);
        long deadline = System.nanoTime() + remaining(Duration.ofMinutes(5)).toNanos();
        get(graph, part, ByteString.copyFromUtf8("__route_probe"));
        Partition partition = new Partition(graph, part);
        InetSocketAddress endpoint = leaders.get(partition);
        RpcClient client = connection(endpoint, deadline);
        Duration left = timeLeft(deadline);
        RpcRequest request = RpcRequest.newBuilder().setContext(context(graph, part, "", Math.max(1, left.toMillis()))).setMethod("scan.stream")
                .setBody(ReadKey.newBuilder().setKey(prefix).setScan(true).setScanId(UUID.randomUUID().toString()).setEntityType(type == null ? "" : type)
                        .addAllFilters(filters).setLimit(256).build().toByteString()).build();
        try (RpcClient.ResultStream stream = client.open(request, left)) {
            RequestBudget currentBudget = CURRENT.get();
            if (currentBudget != null) stream.leaseSupplier(currentBudget.lease);
            for (;;) {
                CompletableFuture<ByteString> batch = stream.next();
                ByteString bytes;
                for (;;) {
                    Duration wait = timeLeft(deadline);
                    try { bytes = batch.get(Math.min(wait.toNanos(), TimeUnit.MILLISECONDS.toNanos(200)), TimeUnit.NANOSECONDS); break; }
                    catch (TimeoutException waiting) { /* 等待期间继续观察查询取消和硬截止时间。 */ }
                }
                if (bytes == null) break;
                consumer.accept(ScanPage.parseFrom(bytes));
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "分片流被取消", ex);
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("STREAM_FAILED", "分片流中断，当前查询失败，不自动重算", ex); }
    }
    /**
     * 幂等记录保留窗口。只有在窗口内重复提交同一个 requestId 才会被判为重复；
     * 窗口越短，分片库里为去重保存的记录越少。由各节点启动时按配置设置，默认 1 小时。
     */
    private static volatile long idempotencyWindowMs = TimeUnit.HOURS.toMillis(1);

    public static void setIdempotencyWindowMs(long millis) {
        if (millis < TimeUnit.MINUTES.toMillis(1) || millis > TimeUnit.DAYS.toMillis(7)) {
            throw new IllegalArgumentException("幂等窗口必须在 1 分钟到 7 天之间：" + millis);
        }
        idempotencyWindowMs = millis;
    }
    public static long idempotencyWindowMs() { return idempotencyWindowMs; }

    public static WriteBatch.Builder batch(String id) {
        return WriteBatch.newBuilder().setRequestId(id).setExpiresAtMs(System.currentTimeMillis() + idempotencyWindowMs);
    }
    public static ByteString groupKey(long graph, int part) { return ByteString.copyFromUtf8("catalog/group/" + graph + "/" + part); }
    public static Condition condition(ByteString key, ByteString expected) {
        var condition = Condition.newBuilder().setKey(key).setExpectedPresent(expected != null);
        if (expected != null) condition.setExpectedValue(expected);
        return condition.build();
    }
    private RpcClient connection(InetSocketAddress address, long deadline) {
        if (closed) throw new IllegalStateException("StorageGateway 已关闭");
        RpcClient previous = connections.get(address);
        if (previous != null && previous.isActive()) return previous;
        var lock = connectionLocks[Math.floorMod(address.hashCode(), connectionLocks.length)];
        try {
            while (!lock.tryLock(Math.min(timeLeft(deadline).toNanos(), TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS)) {
                timeLeft(deadline);
            }
            try {
                if (closed) throw new IllegalStateException("StorageGateway 已关闭");
                previous = connections.get(address);
                if (previous != null && previous.isActive()) return previous;
                if (previous != null && connections.remove(address, previous)) previous.close();
                RpcClient created = new RpcClient(address, MAX_RPC_PAYLOAD, 128, timeLeft(deadline));
                if (closed) { created.close(); throw new IllegalStateException("StorageGateway 已关闭"); }
                connections.put(address, created);
                return created;
            } finally { lock.unlock(); }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new ClusterException("QUERY_CANCELED", "连接被取消", ex);
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) {
            // Netty's sync() can rethrow a checked ConnectException without declaring it.
            // Normalize it so pre-send connection failures enter bounded route recovery.
            throw new IllegalStateException("Storage 连接不可用：" + address, ex);
        }
    }
    @Override public void close() {
        closed = true;
        connections.values().forEach(RpcClient::close);
        connections.clear();
        cursorOwners.clear();
        routes.clear();
    }
}
