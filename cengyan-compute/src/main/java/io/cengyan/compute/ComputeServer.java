package io.cengyan.compute;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.google.protobuf.ByteString;
import io.cengyan.distributed.*;
import io.cengyan.parser.Gql;
import io.cengyan.parser.Stmt;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.query.RemoteQueryEngine;
import io.cengyan.query.QueryResult;
import io.cengyan.raft.*;
import io.cengyan.rpc.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.*;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.ratis.protocol.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 独立 Compute：外部 JSON 接入、内部代理、单查询单执行节点、本地临时文件。 */
public final class ComputeServer implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final ComputeConfig config;
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    private final DistributedGraph graph;
    private final RaftNode control;
    private final ComputeMembership membership;
    private final RpcServer rpc;
    private final ExecutionSlots slots;
    private final AdmissionQueue<Work> queue;
    private final ConcurrentMap<String, Long> active = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RpcClient> peers = new ConcurrentHashMap<>();
    private record LoadSample(JsonNode load, long receivedAt) { }
    private final ConcurrentMap<String, LoadSample> loadSamples = new ConcurrentHashMap<>();
    private final ExecutorService execution;
    private final ExecutorService ingress;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final EventLoopGroup boss = new NioEventLoopGroup(1), io = new NioEventLoopGroup(2);
    private final ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final Channel listener;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentMap<String, CompletableFuture<RpcResponse>> runningRpc = new ConcurrentHashMap<>();
    private record Running(String user, String query, long startedAt) { }
    private final ConcurrentMap<String, Running> runningDescriptions = new ConcurrentHashMap<>();
    private final AtomicLong clientRequests = new AtomicLong();
    private final AtomicLong clientCompleted = new AtomicLong();
    private final AtomicLong clientFailed = new AtomicLong();
    private final AtomicLong clientBytesIn = new AtomicLong();
    private final AtomicLong clientBytesOut = new AtomicLong();
    private record AuthCache(TokenAuthority.User user, long expiresAt) { }
    private final ConcurrentMap<String, AuthCache> authCache = new ConcurrentHashMap<>();
    private volatile String publicKey;
    private record Work(ObjectNode request, boolean write, long deadline, CompletableFuture<ObjectNode> result) { }
    private volatile String maintenanceId = "";
    /** Admin commands that stay available while data writes are paused for a rolling restart. */
    private static final Set<String> MAINTENANCE_ALLOWED = Set.of("maintenanceEnter", "maintenanceExit", "maintenanceStatus",
            "transferMetadataLeader", "drainCompute", "resumeCompute", "removeCompute", "createUser", "setPassword", "setRole", "dropUser");
    private static final Map<String, String> MAINTENANCE_METHODS = Map.of("maintenanceEnter", "metadata.maintenance.enter",
            "maintenanceExit", "metadata.maintenance.exit", "maintenanceStatus", "metadata.maintenance.status",
            "transferMetadataLeader", "metadata.transferLeader");

    public ComputeServer(ComputeConfig config) throws Exception {
        this.config = config;
        storage = new StorageGateway(config.clusterId(), config.storageSeeds());
        catalog = new DistributedCatalog(storage); graph = new DistributedGraph(storage);
        var bootstrap = new ArrayList<>(config.members().stream().map(ComputeConfig.Peer::raftPeer).toList());
        boolean joining = bootstrap.stream().noneMatch(peer -> peer.getId().toString().equals(config.nodeId()));
        if (joining) bootstrap.add(RaftPeer.newBuilder().setId(config.nodeId()).setAddress(config.raft()).setClientAddress(config.rpc()).setAdminAddress(config.client())
                .setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build());
        var group = RaftGroup.valueOf(GroupIds.compute(config.clusterId()), bootstrap);
        control = new RaftNode(config.clusterId(), config.nodeId(), config.raft().getHostString(), config.raft().getPort(), config.directory().resolve("control"), group, RaftSettings.defaults());
        membership = new ComputeMembership(config, control);
        slots = new ExecutionSlots(config.slots(), config.nodeMemory(), config.nodeDisk(), Duration.ofSeconds(5));
        queue = new AdmissionQueue<>(config.queueCapacity(), Duration.ofMillis(config.queueTimeoutMs()));
        execution = new ThreadPoolExecutor(config.slots(), config.slots(), 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), daemon("compute-execution"), new ThreadPoolExecutor.AbortPolicy());
        ingress = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(config.queueCapacity()), daemon("compute-ingress"), new ThreadPoolExecutor.AbortPolicy());
        Path requests = config.directory().resolve("requests"); Files.createDirectories(requests);
        try (var files = Files.list(requests)) { for (Path old : files.toList()) if (Files.isDirectory(old) && !Files.isSymbolicLink(old)) deleteTemporary(old); }
        rpc = new RpcServer(config.rpc(), config.clusterId(), Math.max(8 * 1024 * 1024, config.resultMaxBytes() + 1024), 128, this::internal);
        try {
            listener = new ServerBootstrap().group(boss, io).channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true).childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel channel) {
                            clients.add(channel);
                            channel.pipeline().addLast(new LineBasedFrameDecoder(8 * 1024 * 1024), new StringDecoder(StandardCharsets.UTF_8),
                                    new StringEncoder(StandardCharsets.UTF_8), new SimpleChannelInboundHandler<String>() {
                                private final Set<CompletableFuture<ObjectNode>> outstanding = ConcurrentHashMap.newKeySet();
                                @Override protected void channelRead0(ChannelHandlerContext ctx, String line) {
                                    clientRequests.incrementAndGet();
                                    clientBytesIn.addAndGet(line.getBytes(StandardCharsets.UTF_8).length);
                                    try {
                                        JsonNode parsed = JSON.readTree(line);
                                        if (!parsed.isObject()) throw new IllegalArgumentException("请求必须为 JSON 对象");
                                        CompletableFuture<ObjectNode> response = external((ObjectNode) parsed);
                                        outstanding.add(response);
                                        response.whenComplete((result, error) -> {
                                            outstanding.remove(response);
                                            if (ctx.channel().isActive()) {
                                                try {
                                                    ObjectNode output = error == null ? result : error(error);
                                                    if (error == null && "OK".equals(output.path("code").asText())) clientCompleted.incrementAndGet();
                                                    else clientFailed.incrementAndGet();
                                                    String encoded = JSON.writeValueAsString(output) + "\n";
                                                    clientBytesOut.addAndGet(encoded.getBytes(StandardCharsets.UTF_8).length);
                                                    ctx.writeAndFlush(encoded);
                                                }
                                                catch (Exception failure) { ctx.close(); }
                                            }
                                        });
                                    } catch (Exception ex) {
                                        ctx.writeAndFlush(error(ex).toString() + "\n");
                                    }
                                }
                                @Override public void channelInactive(ChannelHandlerContext ctx) { outstanding.forEach(future -> future.cancel(true)); outstanding.clear(); }
                                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
                            });
                        }
                    }).bind(config.client()).sync().channel();
        } catch (Exception ex) {
            rpc.close(); control.close(); storage.close(); ingress.shutdownNow(); execution.shutdownNow(); scheduler.shutdownNow();
            boss.shutdownGracefully(); io.shutdownGracefully(); throw ex;
        }
        scheduler.scheduleWithFixedDelay(this::dispatch, 10, 20, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> { slots.sweep(); queue.sweep(); }, 100, 100, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::refreshLoads, 0, 500, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::refreshMaintenance, 0, 1000, TimeUnit.MILLISECONDS);
        if (joining) ingress.execute(() -> {
            var body = JSON.createObjectNode();
            body.put("nodeId", config.nodeId()).put("rpc", config.rpc().getHostString() + ":" + config.rpc().getPort())
                    .put("raft", config.raft().getHostString() + ":" + config.raft().getPort())
                    .put("client", config.client().getHostString() + ":" + config.client().getPort());
            for (int attempt = 0; attempt < 30 && !closed.get(); attempt++) {
                for (var seed : config.members()) {
                    try { JsonNode result = peerCall(seed, "compute.join", body, Duration.ofSeconds(3)); if (result.path("code").asText().equals("OK")) return; }
                    catch (Exception ignored) { }
                }
                try { TimeUnit.MILLISECONDS.sleep(250); }
                catch (InterruptedException ex) { Thread.currentThread().interrupt(); return; }
            }
            org.slf4j.LoggerFactory.getLogger(ComputeServer.class).error("Compute 节点未能加入控制面");
        });
    }

    private CompletableFuture<ObjectNode> external(ObjectNode original) {
        CompletableFuture<ObjectNode> response = new CompletableFuture<>();
        long receivedAt = System.nanoTime();
        long requestedMs = original.path("remainingTimeoutMs").asLong(original.path("timeoutMs").asLong(config.executionTimeoutMs()));
        if (requestedMs <= 0) requestedMs = config.executionTimeoutMs();
        final long ingressDeadline = receivedAt + TimeUnit.MILLISECONDS.toNanos(Math.min(requestedMs, config.executionTimeoutMs()));
        try {
            ingress.execute(() -> {
                try (var ingressBudget = StorageGateway.budget(Long.MAX_VALUE, response::isCancelled, ingressDeadline)) {
                    ObjectNode request = original.deepCopy();
                    var untrustedFields = new ArrayList<String>();
                    request.fieldNames().forEachRemaining(field -> { if (field.startsWith("_")) untrustedFields.add(field); });
                    request.remove(untrustedFields);
                    String op = request.path("op").asText();
                    if (op.equals("ping")) { response.complete(ok()); return; }
                    if (op.equals("login")) {
                        auth("auth.initialize", JSON.createObjectNode().put("rootPassword", config.rootPassword()), "init");
                        JsonNode signed = auth("auth.login", request, UUID.randomUUID().toString());
                        String token = signed.path("token").asText();
                        authCache.remove(request.path("user").asText());
                        JsonNode claims = verifyExternal(token);
                        ObjectNode result = ok().put("token", token).put("session", token);
                        result.set("columns", JSON.valueToTree(List.of("user", "role")));
                        result.set("rows", JSON.valueToTree(List.of(List.of(claims.path("user").asText(), claims.path("role").asText()))));
                        response.complete(result); return;
                    }
                    String token = request.path("token").asText(request.path("session").asText());
                    JsonNode claims = verifyExternal(token);
                    if (op.equals("session")) { response.complete(ok().put("session", token).put("user", claims.path("user").asText()).put("role", claims.path("role").asText())); return; }
                    if (op.equals("logout")) { response.complete(ok()); return; }
                    if (op.equals("ops.metrics")) { response.complete(opsMetrics()); return; }
                    if (op.equals("ops.overview")) { response.complete(opsOverview()); return; }
                    if (op.equals("members")) {
                        var result = ok();
                        // Ratis may reverse-resolve configured IPs to private short hostnames.
                        // External clients need routable addresses, not server-only /etc/hosts aliases.
                        result.set("addresses", JSON.valueToTree(membership.members().stream().map(peer -> Map.of(
                                "host", peer.client().getAddress() == null ? peer.client().getHostString() : peer.client().getAddress().getHostAddress(),
                                "port", peer.client().getPort())).toList()));
                        response.complete(result); return;
                    }
                    if (op.equals("queries")) {
                        if (!claims.path("role").asText().equals("write")) throw new ClusterException("FORBIDDEN", "只读用户不能管理正在执行的查询");
                        var result = ok(); result.set("columns", JSON.valueToTree(List.of("queryId", "user", "elapsedMs", "query")));
                        var rows = result.putArray("rows");
                        for (var entry : runningDescriptions.entrySet()) rows.addArray().add(entry.getKey()).add(entry.getValue().user())
                                .add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entry.getValue().startedAt())).add(entry.getValue().query());
                        response.complete(result); return;
                    }
                    if (op.equals("cancel")) {
                        if (!claims.path("role").asText().equals("write")) throw new ClusterException("FORBIDDEN", "只读用户不能终止查询");
                        var pending = runningRpc.get(request.path("queryId").asText());
                        if (pending != null) pending.cancel(true);
                        response.complete(pending == null ? JSON.createObjectNode().put("code", "QUERY_CANCELED").put("message", "没有找到该查询") : ok()); return;
                    }
                    boolean write = isWrite(request);
                    if (write && !claims.path("role").asText().equals("write")) throw new ClusterException("FORBIDDEN", "只读用户不能修改数据");
                    if (write && !maintenanceId.isEmpty() && !MAINTENANCE_ALLOWED.contains(op.equals("admin") ? request.path("command").asText() : "")) {
                        throw ClusterMaintenance.rejected();
                    }
                    if (!op.equals("admin") && membership.draining(config.nodeId())) {
                        throw new ClusterException("BUSY", "入口计算节点正在排空，不再接纳新请求");
                    }
                    if (op.equals("admin")) {
                        String command = request.path("command").asText();
                        if (Set.of("addStorage", "addCompute", "setPartCount", "rebuildIndex").contains(command)) {
                            throw new ClusterException("UNSUPPORTED", "旧单机管理命令已移除；新增节点请在目标机器配置角色并启动自动 Join，分片数建图后固定");
                        }
                        if (command.equals("drainCompute") || command.equals("resumeCompute")) {
                            String target = request.path("nodeId").asText();
                            if (membership.members().stream().noneMatch(peer -> peer.id().equals(target))) {
                                throw new ClusterException("NODE_NOT_FOUND", "计算节点不存在：" + target);
                            }
                            membership.setDraining(target, command.equals("drainCompute")); response.complete(ok()); return;
                        }
                        if (command.equals("computeLoad")) {
                            String target = request.path("nodeId").asText();
                            var peer = membership.members().stream().filter(p -> p.id().equals(target)).findFirst()
                                    .orElseThrow(() -> new ClusterException("NODE_NOT_FOUND", "计算节点不存在：" + target));
                            ObjectNode load = (ObjectNode) peerCall(peer, "compute.load", JSON.createObjectNode(), Duration.ofSeconds(2));
                            response.complete(load.put("code", "OK")); return;
                        }
                        if (command.equals("removeCompute")) {
                            String target = request.path("nodeId").asText();
                            var peer = membership.members().stream().filter(p -> p.id().equals(target)).findFirst()
                                    .orElseThrow(() -> new ClusterException("NODE_NOT_FOUND", "计算节点不存在"));
                            JsonNode load;
                            try { load = peerCall(peer, "compute.load", JSON.createObjectNode(), Duration.ofSeconds(2)); }
                            catch (Exception ex) {
                                // A stopped node has nothing in flight; removal of an unreachable node needs an explicit force.
                                if (!request.path("force").asBoolean(false)) throw ex;
                                load = JSON.createObjectNode();
                            }
                            if (load.path("active").asInt() != 0 || load.path("reserved").asInt() != 0
                                    || load.path("queued").asInt() != 0 || load.path("proxied").asInt() != 0) {
                                throw new ClusterException("NODE_NOT_DRAINED", "计算节点仍有排队、代理或执行任务");
                            }
                            membership.remove(target); response.complete(ok()); return;
                        }
                        if (Set.of("createUser", "setPassword", "setRole", "dropUser").contains(command)) {
                            ObjectNode user = JSON.createObjectNode().put("user", request.path("user").asText());
                            if (command.equals("createUser") || command.equals("setPassword")) user.put("password", request.path("password").asText());
                            if (command.equals("createUser") || command.equals("setRole")) user.put("role", request.path("role").asText());
                            if (command.equals("dropUser")) user.put("enabled", false);
                            auth(command.equals("createUser") ? "auth.create" : "auth.update", user, request.path("requestId").asText(UUID.randomUUID().toString()));
                            authCache.remove(user.path("user").asText());
                            response.complete(ok()); return;
                        }
                    }
                    request.put("_user", claims.path("user").asText());
                    request.remove(List.of("token", "session"));
                    String id = request.path("requestId").asText(UUID.randomUUID().toString());
                    if (!id.matches("[A-Za-z0-9_-]{1,120}")) throw new ClusterException("INVALID_ARGUMENT", "requestId 格式无效");
                    request.put("requestId", id);
                    if (request.path("query").asText().stripLeading().toUpperCase(Locale.ROOT).startsWith("USE ")) {
                        var statements = Gql.parseAll(request.path("query").asText());
                        if (statements.size() == 1 && statements.getFirst() instanceof Stmt.Use use) request.put("graph", use.name());
                    }
                    long timeout = request.path("remainingTimeoutMs").asLong(request.path("timeoutMs").asLong(config.executionTimeoutMs()));
                    if (timeout <= 0) timeout = config.executionTimeoutMs();
                    timeout = Math.min(timeout, config.executionTimeoutMs());
                    long deadline = ingressDeadline;
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) throw new ClusterException("QUERY_TIMEOUT", "请求在入口校验期间已超时");
                    if (request.hasNonNull("graph") && !request.path("graph").asText().isBlank()
                            && !op.equals("createGraph") && !(op.equals("execute") && request.path("query").asText().stripLeading().toUpperCase(Locale.ROOT).startsWith("CREATE GRAPH"))) {
                        ObjectNode leaseBody = JSON.createObjectNode().put("graph", request.path("graph").asText())
                                .put("deadline", System.currentTimeMillis() + TimeUnit.NANOSECONDS.toMillis(remainingNanos)).put("write", write);
                        RpcResponse acquired = storage.call(0, 0, "schema.acquire", id, ByteString.copyFrom(JSON.writeValueAsBytes(leaseBody)),
                                Duration.ofNanos(Math.min(TimeUnit.SECONDS.toNanos(15), remainingNanos)));
                        request.put("_schemaLease", Base64.getEncoder().encodeToString(acquired.getBody().toByteArray()));
                    }
                    remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) throw new ClusterException("QUERY_TIMEOUT", "请求在 Schema 校验期间已超时");
                    Work work = new Work(request, write, deadline, response);
                    var ticket = queue.submit(id, work, Duration.ofNanos(remainingNanos));
                    ticket.completion().thenAccept(outcome -> {
                        if (!outcome.code().equals("OK")) {
                            var running = runningRpc.get(id);
                            if (running != null && !write) running.cancel(true);
                            boolean unknownWrite = write && outcome.code().equals("QUERY_TIMEOUT");
                            response.complete(JSON.createObjectNode().put("code", unknownWrite ? "FAILED" : outcome.code())
                                    .put("message", unknownWrite ? "写请求等待超过截止时间，提交状态无法确认，Storage 将继续完成提交或回滚" : outcome.message()));
                        }
                    });
                    response.whenComplete((done, failure) -> { if (response.isCancelled()) queue.cancel(ticket); });
                } catch (Throwable ex) { response.complete(error(ex)); }
            });
        } catch (RejectedExecutionException ex) { response.complete(JSON.createObjectNode().put("code", "BUSY").put("message", "入口接纳队列已满")); }
        return response;
    }

    private void dispatch() {
        if (closed.get()) return;
        AdmissionQueue.Ticket<Work> ticket = queue.poll();
        if (ticket == null) return;
        try {
            Work work = ticket.request();
            if (System.nanoTime() - work.deadline >= 0) { queue.sweep(); return; }
            record Candidate(ComputeConfig.Peer peer, int available) { }
            List<Candidate> candidates = new ArrayList<>();
            for (var peer : membership.members()) {
                if (membership.draining(peer.id())) continue;
                try {
                    LoadSample sample = loadSamples.get(peer.id());
                    JsonNode load;
                    if (peer.id().equals(config.nodeId())) load = JSON.valueToTree(slots.load());
                    else {
                        if (sample == null || System.nanoTime() - sample.receivedAt() >= TimeUnit.SECONDS.toNanos(2)) continue;
                        load = sample.load();
                    }
                    if (load.path("available").asInt() > 0) candidates.add(new Candidate(peer, load.path("available").asInt()));
                } catch (Exception ignored) { }
            }
            candidates.sort(Comparator.comparingInt(Candidate::available).reversed().thenComparing(c -> c.peer.id()));
            for (Candidate candidate : candidates) {
                JsonNode reservation;
                try {
                    long left = work.deadline - System.nanoTime();
                    if (left <= 0) { queue.sweep(); return; }
                    reservation = peerCall(candidate.peer, "compute.reserve", JSON.createObjectNode().put("requestId", ticket.requestId()),
                            Duration.ofNanos(Math.min(TimeUnit.MILLISECONDS.toNanos(500), left)));
                } catch (Exception ex) { continue; }
                if (!reservation.hasNonNull("token")) continue;
                if (!queue.start(ticket)) { peerCall(candidate.peer, "compute.release", (ObjectNode) reservation, Duration.ofMillis(500)); return; }
                ObjectNode forwarded = work.request.deepCopy();
                forwarded.put("_reservation", reservation.path("token").asText());
                forwarded.put("remainingTimeoutMs", Math.max(1, TimeUnit.NANOSECONDS.toMillis(work.deadline - System.nanoTime())));
                CompletableFuture<RpcResponse> execution = peerClient(candidate.peer).call(rpcRequest("compute.execute", forwarded), Duration.ofMillis(forwarded.path("remainingTimeoutMs").asLong()));
                runningRpc.put(ticket.requestId(), execution);
                runningDescriptions.put(ticket.requestId(), new Running(work.request.path("_user").asText(),
                        work.request.path("query").asText(work.request.path("op").asText()), System.nanoTime()));
                work.result.whenComplete((value, failure) -> { if (work.result.isCancelled() && !work.write) execution.cancel(true); });
                execution.whenComplete((result, failure) -> {
                    try {
                        ObjectNode output = failure == null ? (ObjectNode) JSON.readTree(result.getBody().toByteArray()) : error(failure);
                        queue.finish(ticket, new AdmissionQueue.Outcome(output.path("code").asText(), output.path("message").asText()));
                        work.result.complete(output);
                    } catch (Exception ex) { queue.finish(ticket, new AdmissionQueue.Outcome("FAILED", ex.getMessage())); work.result.complete(error(ex)); }
                    finally { runningRpc.remove(ticket.requestId(), execution); runningDescriptions.remove(ticket.requestId()); queue.cleanupComplete(ticket); }
                });
                return;
            }
            queue.retryReservation(ticket);
        } catch (Exception ex) { queue.retryReservation(ticket); }
    }
    /** Cached cluster maintenance id; Storage remains the authority for rejecting data writes. */
    private void refreshMaintenance() {
        if (closed.get()) return;
        try {
            var state = ClusterMaintenance.read(storage);
            maintenanceId = state == null ? "" : state.id();
        } catch (Exception ignored) { /* keep the last known value */ }
    }
    private void refreshLoads() {
        if (closed.get()) return;
        for (var peer : membership.members()) {
            if (peer.id().equals(config.nodeId())) continue;
            try { loadSamples.put(peer.id(), new LoadSample(peerCall(peer, "compute.load", JSON.createObjectNode(), Duration.ofMillis(300)), System.nanoTime())); }
            catch (Exception ex) { loadSamples.remove(peer.id()); }
        }
    }

    private CompletionStage<RpcResponse> internal(RpcRequest request) {
        try {
            ObjectNode body = (ObjectNode) JSON.readTree(request.getBody().toByteArray());
            if (request.getMethod().equals("compute.join")) {
                CompletableFuture<RpcResponse> result = new CompletableFuture<>();
                ingress.execute(() -> {
                    try {
                        var rpc = ComputeConfig.address(body.path("rpc").asText()); var raft = ComputeConfig.address(body.path("raft").asText()); var client = ComputeConfig.address(body.path("client").asText());
                        membership.join(Node.newBuilder().setClusterId(config.clusterId()).setNodeId(body.path("nodeId").asText()).setRole(NodeRole.COMPUTE)
                                .setRpc(Endpoint.newBuilder().setHost(rpc.getHostString()).setPort(rpc.getPort()))
                                .setRaft(Endpoint.newBuilder().setHost(raft.getHostString()).setPort(raft.getPort()))
                                .setClient(Endpoint.newBuilder().setHost(client.getHostString()).setPort(client.getPort())).build());
                        result.complete(completed(ok()).join());
                    } catch (Exception ex) { result.complete(completed(error(ex)).join()); }
                });
                return result;
            }
            if (request.getMethod().equals("compute.load")) {
                ObjectNode load = JSON.valueToTree(slots.load());
                load.put("queued", queue.pending()).put("proxied", runningRpc.size());
                return completed(load);
            }
            if (request.getMethod().equals("compute.metrics")) {
                return completed(opsMetrics());
            }
            if (request.getMethod().equals("compute.reserve")) {
                if (membership.draining(config.nodeId())) return completed(JSON.createObjectNode());
                var reservation = slots.reserve(body.path("requestId").asText(), config.queryMemory(), config.queryDisk());
                return completed(reservation == null ? JSON.createObjectNode() : JSON.valueToTree(reservation));
            }
            if (request.getMethod().equals("compute.release")) {
                slots.cancelReservation(JSON.treeToValue(body, ExecutionSlots.Reservation.class)); return completed(ok());
            }
            if (!request.getMethod().equals("compute.execute")) return completed(error(new ClusterException("UNSUPPORTED", "未知 Compute RPC")));
            var reservation = new ExecutionSlots.Reservation(body.path("_reservation").asText(), body.path("requestId").asText());
            if (!slots.start(reservation)) return completed(error(new ClusterException("RESERVATION_EXPIRED", "执行名额预留已失效或已消费")));
            CompletableFuture<RpcResponse> result = new CompletableFuture<>();
            final boolean writing = isWrite(body);
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.min(config.executionTimeoutMs(), body.path("remainingTimeoutMs").asLong(config.executionTimeoutMs())));
            try {
                execution.execute(() -> {
                    Path work = config.directory().resolve("requests").resolve(reservation.requestId());
                    active.put(reservation.requestId(), deadline);
                    RpcResponse finished;
                    AtomicReference<SchemaLease> heldLease = new AtomicReference<>();
                    AtomicReference<Throwable> renewalFailure = new AtomicReference<>();
                    ScheduledFuture<?> renewal = null;
                    try (var validationBudget = StorageGateway.budget(Long.MAX_VALUE,
                            () -> !writing && result.isCancelled(), deadline)) {
                        Files.createDirectory(work);
                        if (body.hasNonNull("_schemaLease")) {
                            SchemaLease lease = SchemaLease.parseFrom(Base64.getDecoder().decode(body.path("_schemaLease").asText()));
                            new SchemaLeases(storage).validate(lease, lease.getGraphId(), lease.getSchemaVersion());
                            heldLease.set(lease);
                            renewal = scheduler.scheduleWithFixedDelay(() -> {
                                try {
                                    RpcResponse renewed = storage.call(0, 0, "schema.renew", reservation.requestId(), heldLease.get().toByteString(), Duration.ofSeconds(5));
                                    heldLease.set(SchemaLease.parseFrom(renewed.getBody()));
                                } catch (Throwable ex) { renewalFailure.set(ex); }
                            }, 10, 10, TimeUnit.SECONDS);
                        }
                        java.util.function.BooleanSupplier canceled = () -> (!writing && result.isCancelled()) || System.nanoTime() - deadline >= 0
                                || renewalFailure.get() != null || heldLease.get() != null && heldLease.get().getExpiresAtMs() <= System.currentTimeMillis();
                        ObjectNode response;
                        try (var budget = StorageGateway.budget(config.queryMemory(), canceled, deadline, heldLease::get)) {
                            response = execute(body, work, canceled);
                        }
                        response.put("queryId", reservation.requestId());
                        byte[] encoded = JSON.writeValueAsBytes(response);
                        if (encoded.length > config.resultMaxBytes() || response.path("rows").size() > config.resultMaxRows()) throw new ClusterException("RESULT_TOO_LARGE", "查询结果超过行数或字节上限");
                        Files.write(work.resolve("result.json"), encoded);
                        finished = RpcResponse.newBuilder().setCode("OK").setBody(ByteString.copyFrom(encoded)).build();
                    } catch (Throwable ex) {
                        finished = RpcResponse.newBuilder().setCode("OK").setBody(ByteString.copyFromUtf8(error(ex).toString())).build();
                    } finally {
                        if (renewal != null) renewal.cancel(false);
                        active.remove(reservation.requestId());
                        try { deleteTemporary(work); } catch (IOException ignored) { }
                        slots.finish(reservation);
                    }
                    result.complete(finished);
                });
            } catch (RejectedExecutionException ex) { slots.finish(reservation); return completed(error(new ClusterException("BUSY", "执行器尚未释放名额"))); }
            return result;
        } catch (Exception ex) { return completed(error(ex)); }
    }

    private ObjectNode execute(ObjectNode request, Path work, java.util.function.BooleanSupplier canceled) throws Exception {
        String op = request.path("op").asText(), graphName = request.path("graph").asText();
        String id = request.path("requestId").asText();
        if (op.equals("createGraph")) {
            var definition = catalog.create(id, request.path("name").asText(), request.path("partitions").asInt(16), request.path("replicas").asInt(1),
                    IdType.valueOf(request.path("vertexIdType").asText("INT64")), IdType.valueOf(request.path("edgeIdType").asText("INT64")));
            return ok().put("graphId", definition.getId()).put("rowsAffected", 1);
        }
        if (op.equals("bulkInsert")) {
            if (!Set.of("node", "edge").contains(request.path("kind").asText()) || !request.path("rows").isArray()
                    || request.path("rows").isEmpty() || request.path("rows").size() > 10000) {
                throw new ClusterException("INVALID_ARGUMENT", "批次必须指定 node/edge，包含 1 到 10000 条记录");
            }
            if (JSON.writeValueAsBytes(request).length > 4 * 1024 * 1024) throw new ClusterException("BATCH_TOO_LARGE", "网络导入批次超过 4MB 上限");
            List<Entity> entities = new ArrayList<>(); boolean node = request.path("kind").asText().equals("node");
            for (JsonNode row : request.path("rows")) {
                if (!row.isObject()) throw new ClusterException("INVALID_ARGUMENT", "批次每条记录必须为对象");
                var entity = Entity.newBuilder().setNode(node).setType(request.path("type").asText()).setId(EntityCodec.value(JSON.convertValue(row.get("id"), Object.class)));
                if (!node) entity.setSource(EntityCodec.value(JSON.convertValue(row.get("src"), Object.class))).setTarget(EntityCodec.value(JSON.convertValue(row.get("dst"), Object.class)));
                JsonNode properties = row.has("properties") ? row.get("properties") : row;
                properties.properties().forEach(entry -> { if (!Set.of("id", "src", "dst", "rank").contains(entry.getKey())) entity.putProperties(entry.getKey(), EntityCodec.value(JSON.convertValue(entry.getValue(), Object.class))); });
                Entity encoded = entity.build();
                entities.add(encoded);
                StorageGateway.charge(encoded.getSerializedSize() * 3L + 128);
            }
            var result = graph.put(id, graphName, entities, request.path("skipMissing").asBoolean(false), Duration.ofMillis(request.path("remainingTimeoutMs").asLong()));
            return ok().put("rowsAffected", result.affected()).put("duplicates", result.duplicates()).put("skipped", result.skipped());
        }
        if (op.equals("admin") && request.path("command").asText().equals("addReplica")) {
            storage.call(0, 0, "metadata.resize", id, ByteString.copyFrom(JSON.writeValueAsBytes(request)), Duration.ofMinutes(5));
            return ok();
        }
        if (op.equals("admin") && Set.of("drainStorage", "removeStorage").contains(request.path("command").asText())) {
            storage.call(0, 0, request.path("command").asText().equals("drainStorage") ? "metadata.drain" : "metadata.remove", id,
                    ByteString.copyFrom(JSON.writeValueAsBytes(request)), Duration.ofMinutes(5));
            return ok();
        }
        if (op.equals("admin") && MAINTENANCE_METHODS.containsKey(request.path("command").asText())) {
            RpcResponse answer = storage.call(0, 0, MAINTENANCE_METHODS.get(request.path("command").asText()), id,
                    ByteString.copyFrom(JSON.writeValueAsBytes(request)), Duration.ofMinutes(1));
            ObjectNode response = (ObjectNode) JSON.readTree(answer.getBody().toByteArray());
            if (request.path("command").asText().startsWith("maintenance")) refreshMaintenance();
            return response;
        }
        if (op.equals("admin") && request.path("command").asText().equals("showMembers")) {
            GraphDefinition definition = catalog.graph(graphName);
            var response = ok(); response.set("columns", JSON.valueToTree(List.of("nodeId", "partId", "role")));
            var rows = response.putArray("rows");
            for (int part = 0; part < definition.getPartitions(); part++) {
                DataGroupSpec spec = DataGroupSpec.parseFrom(storage.get(0, 0, StorageGateway.groupKey(definition.getId(), part)));
                for (Node member : spec.getMembersList()) rows.addArray().add(member.getNodeId()).add(part).add("voter");
            }
            return response;
        }
        if (op.equals("admin") && request.path("command").asText().equals("showNodes")) {
            var response = ok();
             response.set("columns", JSON.valueToTree(List.of("nodeId", "role", "state", "address", "healthy")));
            var rows = response.putArray("rows");
            for (Node node : catalog.nodes()) {
                boolean draining = storage.get(0, 0, TransactionEngine.text("nodes/draining/" + node.getNodeId())) != null;
                boolean reachable;
                try { reachable = storage.direct(DistributedCatalog.address(node), "ping", ByteString.EMPTY, Duration.ofMillis(500)).getCode().equals("OK"); }
                catch (ClusterException ex) { reachable = false; }
                 rows.addArray().add(node.getNodeId()).add("STORAGE").add(draining ? "DRAINING" : reachable ? "ACTIVE" : "OFFLINE")
                         .add(endpointText(DistributedCatalog.address(node))).add(reachable);
            }
            for (ComputeConfig.Peer peer : membership.members()) {
                boolean draining = membership.draining(peer.id());
                boolean reachable;
                try { peerCall(peer, "compute.load", JSON.createObjectNode(), Duration.ofMillis(500)); reachable = true; }
                catch (Exception ex) { reachable = false; }
                 rows.addArray().add(peer.id()).add("COMPUTE").add(draining ? "DRAINING" : reachable ? "ACTIVE" : "OFFLINE")
                         .add(endpointText(peer.client())).add(reachable);
            }
            return response;
        }
        if (op.equals("admin") && request.path("command").asText().equals("stats")) {
            GraphDefinition definition = catalog.graph(graphName);
            EntityCounts counts = graph.statistics(definition);
            var response = ok(); response.set("columns", JSON.valueToTree(List.of("target", "name", "count")));
            var rows = response.putArray("rows");
            for (TypeDefinition type : definition.getTypesList()) {
                long count = counts.getTypesList().stream().filter(item -> item.getNode() == type.getNode()
                        && item.getType().equals(type.getName())).mapToLong(EntityTypeCount::getCount).sum();
                rows.addArray().add(type.getNode() ? "NODE" : "REL").add(type.getName()).add(count);
            }
            response.put("statisticsSource", "DATA_GROUP_COUNT_STORE");
            response.put("statisticsConsistency", "PARTITION_COMMITTED");
            response.put("collectedAt", System.currentTimeMillis()); return response;
        }
        if (op.equals("admin") && request.path("command").asText().equals("storage")) {
            long bytes = 0;
            for (Node node : catalog.nodes()) {
                RpcResponse size = storage.direct(DistributedCatalog.address(node), "node.storageSize", ByteString.EMPTY, Duration.ofSeconds(10));
                if (!size.getCode().equals("OK")) throw new ClusterException(size.getCode(), size.getMessage());
                bytes = Math.addExact(bytes, Long.parseLong(size.getBody().toStringUtf8()));
            }
            var response = ok(); response.set("columns", JSON.valueToTree(List.of("bytes")));
            response.set("rows", JSON.valueToTree(List.of(List.of(bytes)))); return response;
        }
        if (op.equals("execute")) {
            Map<String, Object> params = request.has("params") ? JSON.convertValue(request.get("params"), new TypeReference<>() { }) : Map.of();
            var engine = new RemoteQueryEngine(storage, work.resolve("duck"), config.queryMemory() + "B", config.queryDisk() + "B", 1);
            long started = System.nanoTime();
            long schemaVersion = request.hasNonNull("_schemaLease")
                    ? SchemaLease.parseFrom(Base64.getDecoder().decode(request.path("_schemaLease").asText())).getSchemaVersion() : 0;
            QueryResult result = engine.execute(id, graphName, request.path("query").asText(), params, canceled, schemaVersion);
            ObjectNode response = ok().put("rowsAffected", result.rowsAffected()).put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            response.set("columns", JSON.valueToTree(result.columns())); response.set("rows", JSON.valueToTree(result.rows())); return response;
        }
        throw new ClusterException("UNSUPPORTED", "分布式入口尚不支持该操作：" + op);
    }

    private JsonNode auth(String method, ObjectNode body, String id) throws IOException {
        RpcResponse response = storage.call(0, 0, method, id, ByteString.copyFrom(JSON.writeValueAsBytes(body)), Duration.ofSeconds(15));
        return response.getBody().isEmpty() ? JSON.createObjectNode() : JSON.readTree(response.getBody().toByteArray());
    }
    private JsonNode verifyExternal(String token) throws IOException {
        if (publicKey == null) publicKey = auth("auth.publicKey", JSON.createObjectNode(), "public-key").path("publicKey").asText();
        TokenAuthority.Claims claims = TokenAuthority.verifySignature(token, publicKey, config.clusterId());
        long now = System.nanoTime();
        AuthCache cache = authCache.get(claims.user());
        if (cache == null || now - cache.expiresAt() >= 0) {
            JsonNode user = auth("auth.user", JSON.createObjectNode().put("user", claims.user()), "user-state");
            if (user.isNull()) throw new ClusterException("UNAUTHENTICATED", "用户不存在或被禁用");
            TokenAuthority.User record = JSON.treeToValue(user, TokenAuthority.User.class);
            cache = new AuthCache(record, now + TimeUnit.SECONDS.toNanos(2));
            authCache.put(claims.user(), cache);
        }
        if (!cache.user().enabled() || cache.user().version() != claims.version() || !cache.user().role().equals(claims.role())) {
            throw new ClusterException("UNAUTHENTICATED", "认证版本或权限已更新，请重新登录");
        }
        return JSON.valueToTree(claims);
    }

    private ObjectNode opsMetrics() {
        ObjectNode result = ok();
        result.put("nodeId", config.nodeId());
        result.put("role", "COMPUTE");
        result.set("slots", JSON.valueToTree(slots.load()));
        result.put("queuePending", queue.pending());
        result.put("proxied", runningRpc.size());
        result.put("runningQueries", runningDescriptions.size());
        result.put("clientRequestsTotal", clientRequests.get());
        result.put("clientCompletedTotal", clientCompleted.get());
        result.put("clientFailedTotal", clientFailed.get());
        result.put("clientBytesInTotal", clientBytesIn.get());
        result.put("clientBytesOutTotal", clientBytesOut.get());
        var memory = ManagementFactory.getMemoryMXBean();
        var threads = ManagementFactory.getThreadMXBean();
        result.put("heapUsedBytes", memory.getHeapMemoryUsage().getUsed());
        result.put("heapCommittedBytes", memory.getHeapMemoryUsage().getCommitted());
        result.put("heapMaxBytes", memory.getHeapMemoryUsage().getMax());
        result.put("threadCount", threads.getThreadCount());
        result.put("daemonThreadCount", threads.getDaemonThreadCount());
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean platform) {
            result.put("processCpuLoad", platform.getProcessCpuLoad());
            result.put("systemCpuLoad", platform.getCpuLoad());
            result.put("systemMemoryBytes", platform.getTotalMemorySize());
            result.put("systemFreeMemoryBytes", platform.getFreeMemorySize());
        }
        try {
            FileStore disk = Files.getFileStore(config.directory());
            result.put("diskTotalBytes", disk.getTotalSpace());
            result.put("diskUsableBytes", disk.getUsableSpace());
        } catch (IOException ignored) { }
        return result;
    }

    private ObjectNode opsOverview() {
        ObjectNode result = opsMetrics();
        var computeNodes = result.putArray("computeNodes");
        for (var peer : membership.members()) {
            try { computeNodes.add(peerCall(peer, "compute.metrics", JSON.createObjectNode(), Duration.ofSeconds(2))); }
            catch (Exception ex) { computeNodes.addObject().put("nodeId", peer.id()).put("state", "UNAVAILABLE"); }
        }
        var storageNodes = result.putArray("storageNodes");
        for (var node : catalog.nodes()) {
            ObjectNode item = JSON.createObjectNode().put("nodeId", node.getNodeId()).put("role", "STORAGE")
                    .put("rpc", node.getRpc().getHost() + ":" + node.getRpc().getPort());
            try {
                RpcResponse metrics = storage.direct(DistributedCatalog.address(node), "ops.metrics", ByteString.EMPTY, Duration.ofSeconds(3));
                if (metrics.getCode().equals("OK")) item.setAll((ObjectNode) JSON.readTree(metrics.getBody().toByteArray()));
            } catch (Exception ex) { item.put("state", "UNAVAILABLE"); }
            storageNodes.add(item);
        }
        var shards = result.putArray("shards");
        for (var graph : catalog.graphs()) {
            for (int part = 0; part < graph.getPartitions(); part++) {
                ObjectNode shard = JSON.createObjectNode().put("graphId", graph.getId()).put("graph", graph.getName()).put("partId", part);
                try {
                    DataGroupSpec spec = DataGroupSpec.parseFrom(storage.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                    var members = shard.putArray("members");
                    spec.getMembersList().forEach(member -> members.add(member.getNodeId()));
                    RpcResponse status = storage.call(graph.getId(), part, "data.status", "ops-" + graph.getId() + "-" + part, ByteString.EMPTY, Duration.ofSeconds(3));
                    if (status.getCode().equals("OK")) {
                        GroupStatus group = GroupStatus.parseFrom(status.getBody());
                        shard.put("term", group.getTerm()).put("appliedIndex", group.getAppliedIndex()).put("leaderId", group.getLeaderId());
                    }
                } catch (Exception ex) { shard.put("state", "UNAVAILABLE"); }
                shards.add(shard);
            }
        }
        return result;
    }
    private JsonNode peerCall(ComputeConfig.Peer peer, String method, ObjectNode body, Duration timeout) throws Exception {
        try {
            RpcResponse response = peerClient(peer).call(rpcRequest(method, body), timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return JSON.readTree(response.getBody().toByteArray());
        } catch (Exception ex) {
            RpcClient broken = peers.remove(peer.id());
            if (broken != null) broken.close();
            throw ex;
        }
    }
    private RpcClient peerClient(ComputeConfig.Peer peer) {
        RpcClient previous = peers.get(peer.id());
        if (previous != null && !previous.isActive() && peers.remove(peer.id(), previous)) previous.close();
        return peers.computeIfAbsent(peer.id(), ignored -> {
            try { return new RpcClient(peer.rpc(), Math.max(8 * 1024 * 1024, config.resultMaxBytes() + 1024), 128); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new ClusterException("FAILED", "计算节点连接失败", ex); }
            catch (Exception ex) { throw new ClusterException("FAILED", "计算节点连接失败", ex); }
        });
    }
    private RpcRequest rpcRequest(String method, ObjectNode body) throws IOException {
        return RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId(config.clusterId()).setRemainingTimeoutMs(config.executionTimeoutMs()))
                .setMethod(method).setBody(ByteString.copyFrom(JSON.writeValueAsBytes(body))).build();
    }
    private static CompletableFuture<RpcResponse> completed(JsonNode body) {
        return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("OK").setBody(ByteString.copyFromUtf8(body.toString())).build());
    }
    private static boolean isWrite(JsonNode request) {
        String op = request.path("op").asText();
        if (op.equals("admin")) return !Set.of("showNodes", "showMembers", "stats", "storage", "computeLoad").contains(request.path("command").asText());
        if (op.equals("execute")) {
            try { return Gql.parseAll(request.path("query").asText()).stream().anyMatch(stmt -> !readOnly(stmt)); }
            catch (Exception ex) { return true; }
        }
        return !Set.of("ping", "session", "queries", "cancel", "members").contains(op);
    }
    private static String endpointText(java.net.InetSocketAddress endpoint) {
        String host = endpoint.getAddress() == null ? endpoint.getHostString() : endpoint.getAddress().getHostAddress();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + endpoint.getPort();
    }
    private static boolean readOnly(Stmt stmt) {
        if (stmt instanceof Stmt.Composite composite) return readOnly(composite.left()) && readOnly(composite.right());
        if (stmt instanceof Stmt.Next next) return readOnly(next.left()) && readOnly(next.right());
        if (stmt instanceof Stmt.AggregateFilter filter) return readOnly(filter.query());
        return stmt instanceof Stmt.Match || stmt instanceof Stmt.Select || stmt instanceof Stmt.Show || stmt instanceof Stmt.Describe
                || stmt instanceof Stmt.Use || stmt instanceof Stmt.Explain || stmt instanceof Stmt.ExplainQuery || stmt instanceof Stmt.Call;
    }
    private static ObjectNode ok() { return JSON.createObjectNode().put("code", "OK"); }
    private static ObjectNode error(Throwable failure) {
        while (failure instanceof CompletionException || failure instanceof ExecutionException) failure = failure.getCause();
        return JSON.createObjectNode().put("code", failure instanceof ClusterException ex ? ex.code()
                        : failure instanceof io.cengyan.CengyanException ex ? ex.code().name() : "FAILED")
                .put("message", failure.getMessage() == null ? "请求执行失败" : failure.getMessage());
    }
    private static ThreadFactory daemon(String name) { return task -> { Thread thread = new Thread(task, name); thread.setDaemon(true); return thread; }; }
    private static void deleteTemporary(Path directory) throws IOException {
        if (!Files.exists(directory) || Files.isSymbolicLink(directory)) return;
        try (var files = Files.walk(directory)) { for (Path path : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("用法：java -jar cengyan-compute.jar compute.properties");
        ComputeServer server = new ComputeServer(ComputeConfig.load(Path.of(args[0])));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { server.close(); } catch (Exception ignored) { } }));
        new CountDownLatch(1).await();
    }
    @Override public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;
        listener.close().awaitUninterruptibly(); clients.close().awaitUninterruptibly(); scheduler.shutdownNow(); rpc.close();
        ingress.shutdownNow(); execution.shutdownNow(); peers.values().forEach(RpcClient::close);
        control.close(); storage.close(); io.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(); boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
