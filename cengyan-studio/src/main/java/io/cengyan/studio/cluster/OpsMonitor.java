package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 按节点并行采集 ops.metrics。单节点超时只记这一行，不挡住其余节点，也不挡住页面读取。
 */
final class OpsMonitor implements AutoCloseable {
    static final Duration ATTEMPT = Duration.ofMillis(1000);
    private static final Logger LOG = Logger.getLogger(OpsMonitor.class.getName());

    private final ClusterManager manager;
    private final Probe probe;
    private final OpsStore store;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();

    static OpsMonitor start(ClusterManager manager) throws java.io.IOException {
        OpsStore store = new OpsStore(manager.store.dataDir());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "cengyan-ops");
            thread.setDaemon(true);
            return thread;
        });
        OpsMonitor monitor = new OpsMonitor(manager, new RpcProbe(), store, scheduler);
        scheduler.scheduleWithFixedDelay(monitor::collectSafely, 1, 15, TimeUnit.SECONDS);
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
            } finally {
                pool.close();
            }
        }
        store.record(state, attempts, System.currentTimeMillis());
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
        @Override default void close() { }
    }

    record Attempt(boolean reachable, String message, ObjectNode metrics) {
        static Attempt ok(ObjectNode metrics) {
            return new Attempt(true, "", metrics == null ? ClusterStore.JSON.createObjectNode() : metrics);
        }
        static Attempt fail(String message) {
            return new Attempt(false, message == null ? "采集失败" : message, ClusterStore.JSON.createObjectNode());
        }
    }

    /** 直连节点 RPC。连接和调用都有独立的短超时，失败后丢掉这条连接。 */
    static final class RpcProbe implements Probe {
        private final ConcurrentHashMap<String, RpcClient> clients = new ConcurrentHashMap<>();

        @Override public Attempt fetch(ClusterState state, NodeSpec node) {
            String host;
            try { host = ConfigRenderer.host(state, node); }
            catch (RuntimeException ex) { return Attempt.fail(ex.getMessage()); }
            String method = node.role() == Role.COMPUTE ? "compute.metrics" : "ops.metrics";
            String key = node.id() + "@" + host + ":" + node.rpcPort();
            try {
                RpcClient client = client(key, host, node.rpcPort());
                RpcRequest request = RpcRequest.newBuilder()
                        .setContext(RequestContext.newBuilder().setClusterId(state.clusterId()).setRemainingTimeoutMs(ATTEMPT.toMillis()))
                        .setMethod(method).setBody(ByteString.copyFromUtf8("{}")).build();
                RpcResponse response = client.call(request, ATTEMPT).get(ATTEMPT.toMillis() + 200, TimeUnit.MILLISECONDS);
                if (!"OK".equals(response.getCode())) return Attempt.fail(response.getMessage().isBlank() ? response.getCode() : response.getMessage());
                JsonNode body = response.getBody().isEmpty() ? ClusterStore.JSON.createObjectNode() : ClusterStore.JSON.readTree(response.getBody().toByteArray());
                if (!(body instanceof ObjectNode object)) return Attempt.fail("采集结果无法解析");
                return Attempt.ok(object);
            } catch (Exception ex) {
                drop(key);
                return Attempt.fail(text(ex));
            }
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
