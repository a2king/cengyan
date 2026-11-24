package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.RequestContext;
import io.cengyan.protocol.proto.ClusterProtocol.RpcRequest;
import io.cengyan.protocol.proto.ClusterProtocol.RpcResponse;
import io.cengyan.rpc.RpcClient;
import io.cengyan.studio.cluster.ClusterState.NodeSpec;
import io.cengyan.studio.cluster.ClusterState.Role;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 按节点并行采集 ops.metrics。单节点超时只记这一行，不挡住其余节点，也不挡住页面读取。
 * 分片列表在 Storage 上要扫 Metadata 目录，比机器指标重得多：每 {@value #SHARD_EVERY} 轮取一次，
 * Storage 可达性一变就马上补一轮。旧版 Storage 把分片放在 ops.metrics 里，直接用。
 */
final class OpsMonitor implements AutoCloseable {
    static final Duration INTERVAL = Duration.ofSeconds(15);
    static final Duration ATTEMPT = Duration.ofMillis(2000);
    static final Duration SHARD_ATTEMPT = Duration.ofMillis(5000);
    static final int SHARD_EVERY = 4;
    private static final Logger LOG = Logger.getLogger(OpsMonitor.class.getName());

    private final ClusterManager manager;
    private final Probe probe;
    private final OpsStore store;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong rounds = new AtomicLong();
    private final Map<String, Boolean> lastReachable = new ConcurrentHashMap<>();

    static OpsMonitor start(ClusterManager manager) throws java.io.IOException {
        OpsStore store = new OpsStore(manager.store.dataDir());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "cengyan-ops");
            thread.setDaemon(true);
            return thread;
        });
        OpsMonitor monitor = new OpsMonitor(manager, new RpcProbe(), store, scheduler);
        scheduler.scheduleWithFixedDelay(monitor::collectSafely, 1, INTERVAL.toSeconds(), TimeUnit.SECONDS);
        return monitor;
    }

    OpsMonitor(ClusterManager manager, Probe probe, OpsStore store, ScheduledExecutorService scheduler) {
        this.manager = manager;
        this.probe = probe;
        this.store = store;
        this.scheduler = scheduler;
    }

    ObjectNode overview() {
        return store.overview(manager.state(), System.currentTimeMillis());
    }

    ObjectNode series(String range) {
        return store.series(manager.state(), range, System.currentTimeMillis());
    }

    ObjectNode shards() {
        return store.shards();
    }

    void collect(ClusterState state) {
        if (closed.get()) return;
        List<NodeSpec> nodes = state.nodes().stream().filter(NodeSpec::joined).toList();
        Map<String, Attempt> attempts = new LinkedHashMap<>();
        if (!nodes.isEmpty()) {
            ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
            try {
                Map<String, Future<Attempt>> futures = new LinkedHashMap<>();
                for (NodeSpec node : nodes) futures.put(node.id(), pool.submit(() -> fetch(state, node)));
                for (var entry : futures.entrySet()) {
                    try { attempts.put(entry.getKey(), entry.getValue().get(ATTEMPT.toMillis() + 2500, TimeUnit.MILLISECONDS)); }
                    catch (Exception ex) { attempts.put(entry.getKey(), Attempt.fail(text(ex))); }
                }
                if (shardRound(nodes, attempts)) {
                    Map<String, Future<ArrayNode>> shards = new LinkedHashMap<>();
                    for (NodeSpec node : nodes) {
                        Attempt attempt = attempts.get(node.id());
                        if (node.role() != Role.STORAGE || !attempt.reachable() || attempt.shardList() != null) continue;
                        shards.put(node.id(), pool.submit(() -> probe.shards(state, node)));
                    }
                    for (var entry : shards.entrySet()) {
                        try {
                            ArrayNode list = entry.getValue().get(SHARD_ATTEMPT.toMillis() + 2500, TimeUnit.MILLISECONDS);
                            if (list != null) attempts.computeIfPresent(entry.getKey(), (id, attempt) -> attempt.withShards(list));
                        } catch (Exception ex) {
                            LOG.log(Level.FINE, "分片采集失败：" + entry.getKey(), ex);
                        }
                    }
                }
            } finally {
                pool.close();
            }
        }
        store.record(state, attempts, System.currentTimeMillis());
    }

    /** 到了固定轮次，或者有 Storage 的可达性和上一轮不同。 */
    private boolean shardRound(List<NodeSpec> nodes, Map<String, Attempt> attempts) {
        boolean changed = false;
        for (NodeSpec node : nodes) {
            if (node.role() != Role.STORAGE) continue;
            boolean reachable = attempts.get(node.id()).reachable();
            Boolean before = lastReachable.put(node.id(), reachable);
            if (before == null || before != reachable) changed = true;
        }
        lastReachable.keySet().retainAll(nodes.stream().map(NodeSpec::id).toList());
        return rounds.getAndIncrement() % SHARD_EVERY == 0 || changed;
    }

    private Attempt fetch(ClusterState state, NodeSpec node) {
        try { return probe.fetch(state, node); }
        catch (Exception ex) { return Attempt.fail(text(ex)); }
    }

    private void collectSafely() {
        if (closed.get() || manager == null) return;
        try { collect(manager.state()); }
        catch (Exception ex) { LOG.log(Level.WARNING, "监控采集失败", ex); }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (scheduler != null) scheduler.shutdownNow();
        try { probe.close(); }
        finally { store.close(); }
    }

    private static String text(Throwable failure) {
        Throwable cause = failure instanceof java.util.concurrent.ExecutionException && failure.getCause() != null ? failure.getCause() : failure;
        if (cause instanceof java.util.concurrent.TimeoutException) return "采集超时";
        String message = cause.getMessage();
        return message == null || message.isBlank() ? "采集失败" : message;
    }

    interface Probe extends AutoCloseable {
        Attempt fetch(ClusterState state, NodeSpec node);
        /** Storage 当前承载的分片；拿不到返回 null。 */
        default ArrayNode shards(ClusterState state, NodeSpec node) throws Exception { return null; }
        @Override default void close() { }
    }

    /** 一个节点这一轮的结果。shards 为 null 表示这轮没取到分片。 */
    record Attempt(boolean reachable, String message, ObjectNode metrics, ArrayNode shards) {
        static Attempt ok(ObjectNode metrics) {
            return new Attempt(true, "", metrics == null ? ClusterStore.JSON.createObjectNode() : metrics, null);
        }
        static Attempt fail(String message) {
            return new Attempt(false, message == null ? "采集失败" : message, ClusterStore.JSON.createObjectNode(), null);
        }
        Attempt withShards(ArrayNode list) {
            return new Attempt(reachable, message, metrics, list);
        }
        /** 单独取到的分片，或者旧版 Storage 放在 ops.metrics 里的分片。 */
        ArrayNode shardList() {
            if (shards != null) return shards;
            return metrics != null && metrics.get("shards") instanceof ArrayNode inline ? inline : null;
        }
    }

    /** 直连节点 RPC。连接和调用都有独立的短超时，失败后丢掉这条连接。 */
    static final class RpcProbe implements Probe {
        private final ConcurrentHashMap<String, RpcClient> clients = new ConcurrentHashMap<>();

        @Override public Attempt fetch(ClusterState state, NodeSpec node) {
            try { return Attempt.ok(call(state, node, node.role() == Role.COMPUTE ? "compute.metrics" : "ops.metrics", ATTEMPT)); }
            catch (Exception ex) { return Attempt.fail(text(ex)); }
        }

        @Override public ArrayNode shards(ClusterState state, NodeSpec node) throws Exception {
            return call(state, node, "ops.shards", SHARD_ATTEMPT).get("shards") instanceof ArrayNode list ? list : null;
        }

        /** 节点回了错误码不丢连接；超时或连接出错才丢，下次重连。 */
        private ObjectNode call(ClusterState state, NodeSpec node, String method, Duration timeout) throws Exception {
            String host = ConfigRenderer.host(state, node);
            String key = node.id() + "@" + host + ":" + node.rpcPort();
            RpcResponse response;
            try {
                RpcClient client = client(key, host, node.rpcPort());
                RpcRequest request = RpcRequest.newBuilder()
                        .setContext(RequestContext.newBuilder().setClusterId(state.clusterId()).setRemainingTimeoutMs(timeout.toMillis()))
                        .setMethod(method).setBody(ByteString.copyFromUtf8("{}")).build();
                response = client.call(request, timeout).get(timeout.toMillis() + 200, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                drop(key);
                throw ex;
            }
            if (!"OK".equals(response.getCode())) throw new IllegalStateException(response.getMessage().isBlank() ? response.getCode() : response.getMessage());
            JsonNode body = response.getBody().isEmpty() ? ClusterStore.JSON.createObjectNode() : ClusterStore.JSON.readTree(response.getBody().toByteArray());
            if (!(body instanceof ObjectNode object)) throw new IllegalStateException("采集结果无法解析");
            return object;
        }

        private RpcClient client(String key, String host, int port) throws Exception {
            RpcClient existing = clients.get(key);
            if (existing != null && existing.isActive()) return existing;
            if (existing != null && clients.remove(key, existing)) existing.close();
            RpcClient created = new RpcClient(new InetSocketAddress(host, port), 1024 * 1024, 2, ATTEMPT);
            RpcClient raced = clients.putIfAbsent(key, created);
            if (raced != null) {
                created.close();
                return raced;
            }
            return created;
        }

        private void drop(String key) {
            RpcClient client = clients.remove(key);
            if (client != null) client.close();
        }

        @Override public void close() {
            List<RpcClient> open = List.copyOf(clients.values());
            clients.clear();
            if (open.isEmpty()) return;
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (RpcClient client : open) pool.submit(() -> { try { client.close(); } catch (Exception ignored) { } });
            }
        }
    }
}
