package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftNode;
import io.cengyan.raft.RocksStateMachine;
import io.cengyan.rpc.RpcServer;
import io.cengyan.distributed.HostMetrics;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.TokenAuthority;
import io.cengyan.distributed.DistributedGraph;
import io.cengyan.distributed.ReplicaController;
import io.cengyan.distributed.SchemaLeases;
import io.cengyan.distributed.SchemaController;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.DivisionInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 独立 Storage 进程入口。Compute 通过业务 RPC 使用本入口提供的分片、事务、Schema 和拓扑能力。
 * 业务 RPC 只提交本节点 Ratis Division，不能悄悄跨节点转发并隐藏 NOT_LEADER。
 */
public final class StorageServer implements AutoCloseable {
    private final StorageConfig config;
    private final RaftNode node;
    private final RpcServer rpc;
    private final ThreadPoolExecutor admission;
    private final ClientId clientId = ClientId.randomId();
    private final AtomicLong calls = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final StorageGateway gateway;
    private final TransactionEngine transactions;
    private final RaftMaintenance maintenanceGate;
    /** 历史版本压缩：保留期来自 version_retention_ms，决定分片库里旧版本占用。 */
    private final io.cengyan.distributed.VersionMaintenance versions;
    private final ThreadPoolExecutor writers;
    private final ThreadPoolExecutor writeParticipants;
    private final ThreadPoolExecutor dataRequests;
    private final ThreadPoolExecutor replicaRequests;
    /** Metadata 的读写只是把请求交给 Raft，很快返回。单独一个池，不排在导入、监控这类慢请求后面。 */
    private final ThreadPoolExecutor metadataRequests;
    /** 导入分块、归并和会话请求。归并会跑很久，不能占用 Metadata 或业务读写的线程。 */
    private final ThreadPoolExecutor importRequests;
    /** 监控采集。 */
    private final ThreadPoolExecutor opsRequests;
    private final TokenAuthority authority;
    private final ReplicaController replicaController;
    private DroppedGraphCleanup dropCleanup;
    private final Object topologyController = new Object();
    private final java.util.concurrent.ConcurrentMap<RaftGroupId, Object> configurationLocks = new java.util.concurrent.ConcurrentHashMap<>();
    private long lastIdempotencyCleanup;
    private final ThreadPoolExecutor coordinators;
    /** 事务恢复单独跑，不占用协调线程。调度线程只负责按间隔派发。 */
    private final ThreadPoolExecutor recoveryWorkers;
    private final ScheduledExecutorService recovery = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "storage-transaction-recovery"); t.setDaemon(true); return t;
    });
    private final ScheduledExecutorService topologyRecovery = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "storage-topology-recovery"); t.setDaemon(true); return t;
    });
    /** 拓扑/Schema/副本恢复不能占用 RPC 协调线程。 */
    private final ExecutorService topologyWorkers = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "storage-topology-worker"); t.setDaemon(true); return t;
    });
    private final AtomicBoolean topologyRunning = new AtomicBoolean();
    /** Every Storage flushes its local database before a planned restart. */
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "storage-maintenance"); t.setDaemon(true); return t;
    });
    /** A new node retries metadata.join until it succeeds; the first attempt can fail transiently. */
    private final ScheduledExecutorService joining = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "storage-join"); t.setDaemon(true); return t;
    });
    private final HostMetrics hostMetrics = new HostMetrics();

    public StorageServer(StorageConfig config) throws IOException, InterruptedException {
        this.config = config;
        StorageGateway.setIdempotencyWindowMs(config.idempotencyWindowMs());
        // 不转换或删除旧单机数据。
        if (Files.exists(config.directory().resolve("_system")) || Files.exists(config.directory().resolve("CURRENT"))) {
            throw new IOException("拒绝使用旧单机数据库目录，请配置独立的新 Storage data_dir");
        }
        if (config.rebuildData()) throw new IOException("rebuild 数据模式已移除。请改为 data_storage_mode=raft，清空数据目录后重新建图导入");
        node = new RaftNode(config.clusterId(), config.nodeId(), config.raftAddress().getHostString(), config.raftAddress().getPort(),
                config.directory(), config.metadataGroup(), config.raftSettings(), false);
        gateway = new StorageGateway(config.clusterId(), config.metadataMembers().stream()
                .map(peer -> StorageConfig.endpoint(peer.getClientAddress())).toList());
        gateway.limit(config.rpcMaxPayload(), config.rpcMaxInflight());
        dropCleanup = new DroppedGraphCleanup(config, node, gateway, closed);
        maintenanceGate = new RaftMaintenance(config, gateway, node);
        versions = new io.cengyan.distributed.VersionMaintenance(gateway, config.versionRetentionMs(), config.readProtectionMs());
        writers = writePool(config.writeThreads(), "storage-writer");
        writeParticipants = writePool(config.writeThreads(), "storage-write-participant");
        dataRequests = writePool(config.writeThreads(), "storage-data-request");
        replicaRequests = writePool(config.writeThreads(), "storage-replica-request");
        metadataRequests = writePool(Math.max(8, config.admissionThreads()), "storage-metadata-request");
        importRequests = writePool(Math.max(4, Runtime.getRuntime().availableProcessors()), "storage-import-request");
        opsRequests = writePool(2, "storage-ops-request");
        transactions = new TransactionEngine(gateway, writeParticipants);
        authority = new TokenAuthority(gateway);
        replicaController = new ReplicaController(gateway);
        admission = new ThreadPoolExecutor(config.admissionThreads(), config.admissionThreads(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.admissionQueue()), runnable -> {
                    Thread thread = new Thread(runnable, "storage-rpc-admission");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        coordinators = new ThreadPoolExecutor(config.coordinatorThreads(), config.coordinatorThreads(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), runnable -> {
                    Thread thread = new Thread(runnable, "storage-coordinator"); thread.setDaemon(true); return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        recoveryWorkers = writePool(config.transactionRecoveryThreads(), "storage-transaction-recovery");
        try {
            rpc = new RpcServer(config.rpcAddress(), config.clusterId(), config.rpcMaxPayload(), config.rpcMaxInflight(), new RpcServer.StreamingService() {
                @Override public CompletionStage<RpcResponse> handle(RpcRequest request) { return StorageServer.this.handle(request); }
                @Override public CompletionStage<RpcServer.StreamSource> open(RpcRequest request) {
                    if (!request.getMethod().equals("scan.stream")) return CompletableFuture.failedFuture(new IllegalArgumentException("未知分片流方法"));
                    return CompletableFuture.completedFuture(new RpcServer.StreamSource() {
                        private ReadKey scan;
                        private volatile boolean stopped;
                        private volatile SchemaLease lease = request.getContext().hasSchemaLease() ? request.getContext().getSchemaLease() : null;
                        @Override public void updateLease(SchemaLease renewed) {
                            SchemaLease previous = lease;
                            if (previous == null || !previous.getLeaseId().equals(renewed.getLeaseId()) || !previous.getRequestId().equals(renewed.getRequestId())
                                    || previous.getGraphId() != renewed.getGraphId() || previous.getSchemaVersion() != renewed.getSchemaVersion()
                                    || previous.getHardDeadlineMs() != renewed.getHardDeadlineMs()) throw new IllegalArgumentException("流续租身份不一致");
                            if (renewed.getExpiresAtMs() > previous.getExpiresAtMs()) lease = renewed;
                        }
                        {
                            try { scan = ReadKey.parseFrom(request.getBody()).toBuilder().setScan(true).setLimit(256).build(); }
                            catch (Exception ex) { throw new IllegalArgumentException("扫描请求无效", ex); }
                        }
                        @Override public CompletionStage<RpcServer.Chunk> next(int maxBytes) {
                            if (stopped) return CompletableFuture.failedFuture(new IllegalStateException("流已取消"));
                            var pageBuilder = request.toBuilder().setMethod("kv.read")
                                    .setBody(scan.toBuilder().setMaxBytes(maxBytes - 64).build().toByteString());
                            if (lease != null) pageBuilder.setContext(request.getContext().toBuilder().setSchemaLease(lease));
                            var pageRequest = pageBuilder.build();
                            return StorageServer.this.handle(pageRequest).thenApply(response -> {
                                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
                                try {
                                    ScanPage page = ScanPage.parseFrom(response.getBody());
                                    scan = scan.toBuilder().setAfterKey(page.getLastKey()).setCursorId(page.getCursorId()).build();
                                    if (stopped) {
                                        close();
                                        throw new ClusterException("QUERY_CANCELED", "分片流在返回批次前已取消");
                                    }
                                    return new RpcServer.Chunk(response.getBody(), !page.getMore());
                                } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new IllegalStateException(ex); }
                            });
                        }
                        @Override public void close() {
                            stopped = true;
                            if (!scan.getCursorId().isEmpty()) {
                                StorageServer.this.handle(request.toBuilder().setMethod("kv.read")
                                        .setBody(scan.toBuilder().setCloseCursor(true).build().toByteString()).build());
                            }
                        }
                    });
                }
            });
        } catch (RuntimeException | InterruptedException ex) {
            admission.shutdownNow();
            node.close();
            gateway.close();
            recovery.shutdownNow();
            recoveryWorkers.shutdownNow();
            topologyWorkers.shutdownNow();
            coordinators.shutdownNow();
            writers.shutdownNow();
            writeParticipants.shutdownNow();
            dataRequests.shutdownNow();
            replicaRequests.shutdownNow();
            metadataRequests.shutdownNow();
            importRequests.shutdownNow();
            opsRequests.shutdownNow();
            maintenance.shutdownNow();
            joining.shutdownNow();
            if (dropCleanup != null) dropCleanup.close();
            throw ex;
        }
        recovery.scheduleWithFixedDelay(this::recoverTransactions, 2000, config.transactionRecoveryIntervalMs(), TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(() -> {
            try { maintenanceGate.tick(); }
            catch (Exception ex) { org.slf4j.LoggerFactory.getLogger(StorageServer.class).debug("维护标记等待重试：{}", ex.toString()); }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(this::expireImports, 1, 1, TimeUnit.MINUTES);
        coordinators.execute(() -> {
            try {
                Node local = localDescriptor();
                gateway.write(0, 0, StorageGateway.batch("node.descriptor/" + config.nodeId() + "/" + java.util.UUID.randomUUID())
                        .addMutations(TransactionEngine.put(TransactionEngine.text("nodes/descriptor/" + config.nodeId()), local.toByteString())).build());
            } catch (Exception ex) { org.slf4j.LoggerFactory.getLogger(StorageServer.class).warn("节点信息注册暂未成功：{}", ex.toString()); }
        });
        long topologyInterval = Math.max(2000, config.backgroundIntervalMs());
        topologyRecovery.scheduleWithFixedDelay(() -> {
            if (closed.get()) return;
            if (!topologyRunning.compareAndSet(false, true)) return;
            try { topologyWorkers.execute(this::recoverTopology); }
            catch (RejectedExecutionException ex) { topologyRunning.set(false); }
        }, topologyInterval, topologyInterval, TimeUnit.MILLISECONDS);
        if (config.metadataMembers().stream().noneMatch(peer -> peer.getId().toString().equals(config.nodeId()))) {
            ScheduledFuture<?>[] retry = new ScheduledFuture<?>[1];
            long interval = Math.max(2000, config.backgroundIntervalMs());
            retry[0] = joining.scheduleWithFixedDelay(() -> {
                if (closed.get()) { retry[0].cancel(false); return; }
                try {
                    gateway.call(0, 0, "metadata.join", "join/" + config.nodeId(), localDescriptor().toByteString(), java.time.Duration.ofMinutes(2));
                    org.slf4j.LoggerFactory.getLogger(StorageServer.class).info("已加入 Metadata 成员");
                    retry[0].cancel(false);
                } catch (Exception ex) {
                    org.slf4j.LoggerFactory.getLogger(StorageServer.class).warn("Metadata 加入失败，{} 毫秒后重试：{}", interval, ex.toString());
                }
            }, 0, interval, TimeUnit.MILLISECONDS);
        }
        dropCleanup.start();
        hostMetrics.watch(config.directory());
    }

    private void recoverTopology() {
        try {
            var metadata = node.server().getDivision(GroupIds.metadata(config.clusterId()));
            if (metadata.getInfo().isLeaderReady()) {
                long epoch = metadata.getInfo().getCurrentTerm();
                try (var guard = StorageGateway.controller(epoch, () -> metadata.getInfo().isLeaderReady() && metadata.getInfo().getCurrentTerm() == epoch)) {
                    synchronized (topologyController) {
                        // 各步互不依赖：一步失败（例如某个索引分片暂时不可达）不能让后面的副本修复跳过。
                        topologyStep("副本任务恢复", replicaController::recover);
                        topologyStep("Schema 迁移恢复", () -> new SchemaController(gateway).recover());
                        topologyStep("索引任务恢复", () -> new DistributedGraph(gateway).recoverIndexes());
                        topologyStep("副本修复", () -> replicaController.reconcile(config.recoveryTimeoutMs()));
                    }
                }
            }
        } catch (Exception ex) {
            if (!closed.get()) org.slf4j.LoggerFactory.getLogger(StorageServer.class).warn("后台拓扑任务等待恢复：{}", ex.toString());
        } finally { topologyRunning.set(false); }
    }

    /** 控制面任期变化时整轮停止；其他失败只记日志，下一轮重试。 */
    private void topologyStep(String name, Runnable step) {
        try { step.run(); }
        catch (ClusterException ex) {
            if (ex.code().equals("STALE_CONTROLLER")) throw ex;
            if (!closed.get()) org.slf4j.LoggerFactory.getLogger(StorageServer.class).warn("后台拓扑任务[{}]等待恢复：{}", name, ex.toString());
        } catch (RuntimeException ex) {
            if (!closed.get()) org.slf4j.LoggerFactory.getLogger(StorageServer.class).warn("后台拓扑任务[{}]等待恢复：{}", name, ex.toString());
        }
    }

    /** 测试暂停定时清理，改由 {@link #cleanupDroppedGraphs()} 触发。 */
    void pauseDroppedGraphCleanup() { dropCleanup.pause(); }

    void cleanupDroppedGraphs() { dropCleanup.runOnce(); }

    /** 测试立即跑一轮拓扑恢复，不必等定时任务。 */
    void recoverTopologyNow() { recoverTopology(); }

    private CompletionStage<RpcResponse> handle(RpcRequest request) {
        if (request.getMethod().equals("ping")) return CompletableFuture.completedFuture(response("OK", "", ByteString.EMPTY));
        if (request.getMethod().equals("maintenance.boot")) return CompletableFuture.completedFuture(response("OK", "", ByteString.copyFromUtf8(maintenanceGate.bootId())));
        if (request.getMethod().equals("maintenance.flush") || request.getMethod().equals("import.finish")) {
            CompletableFuture<RpcResponse> pending = new CompletableFuture<>();
            String method = request.getMethod();
            com.google.protobuf.ByteString body = request.getBody();
            (method.equals("import.finish") ? importRequests : maintenance).execute(() -> {
                try {
                    if (method.equals("maintenance.flush")) {
                        for (io.cengyan.raft.RocksStateMachine machine : node.importMachines()) machine.flushForRestart();
                    } else {
                        var beat = io.cengyan.protocol.proto.ClusterProtocol.ImportHeartbeat.parseFrom(body);
                        for (io.cengyan.raft.RocksStateMachine machine : node.importMachines()) machine.finishImport(beat.getSessionId(), beat.getAbort());
                    }
                    pending.complete(response("OK", "", com.google.protobuf.ByteString.EMPTY));
                } catch (Exception ex) {
                    pending.complete(response("FAILED", ex.getMessage() == null ? "存储维护操作失败" : ex.getMessage(), com.google.protobuf.ByteString.EMPTY));
                }
            });
            return pending;
        }
        SchemaLease lease = request.getContext().hasSchemaLease() ? request.getContext().getSchemaLease() : null;
        if (lease != null) request = request.toBuilder().setContext(request.getContext().toBuilder().clearSchemaLease()).build();
        final RpcRequest inbound = request;
        final SchemaLease checkedLease = lease;
        CompletableFuture<RpcResponse> result = new CompletableFuture<>();
        long timeout = inbound.getContext().getRemainingTimeoutMs();
        if (timeout < 1 || timeout > TimeUnit.HOURS.toMillis(24)) {
            return CompletableFuture.completedFuture(response("INVALID_ARGUMENT", "必须提供不超过 24 小时的 RPC 截止时间", ByteString.EMPTY));
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        try {
            pool(inbound).execute(() -> {
                if (result.isCancelled()) return;
                if (checkedLease != null) {
                    try {
                        if (checkedLease.getGraphId() != inbound.getContext().getGraphId()
                                || checkedLease.getSchemaVersion() != inbound.getContext().getSchemaVersion()
                                || !checkedLease.getLeaseId().equals(inbound.getContext().getLeaseId())) {
                            throw new ClusterException("SCHEMA_VERSION_MISMATCH", "分片请求的租约身份不一致");
                        }
                        new SchemaLeases(gateway).validate(checkedLease, checkedLease.getGraphId(), checkedLease.getSchemaVersion());
                    } catch (ClusterException ex) { result.complete(response(ex.code(), ex.getMessage(), ByteString.EMPTY)); return; }
                    catch (Exception ex) { result.complete(response("FAILED", "租约校验失败", ByteString.EMPTY)); return; }
                }
                if (System.nanoTime() - deadline >= 0) {
                    result.complete(response("QUEUE_TIMEOUT", "Storage 请求排队超时，尚未提交", ByteString.EMPTY));
                    return;
                }
                boolean controlRequest = inbound.getMethod().startsWith("metadata.") && !inbound.getMethod().equals("metadata.nodes")
                        || inbound.getMethod().startsWith("schema.");
                org.apache.ratis.server.RaftServer.Division metadata = null;
                long controlEpoch = 0;
                if (controlRequest) {
                    try {
                        metadata = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!metadata.getInfo().isLeaderReady()) { result.complete(notLeader(metadata.getInfo(), metadata.getRaftConf().getAllPeers())); return; }
                        controlEpoch = metadata.getInfo().getCurrentTerm();
                    } catch (IOException ex) { result.complete(response("GROUP_UNAVAILABLE", "Metadata 不可用", ByteString.EMPTY)); return; }
                }
                final var controlDivision = metadata;
                final long epoch = controlEpoch;
                try (AutoCloseable guard = controlRequest ? StorageGateway.controller(epoch,
                        () -> controlDivision.getInfo().isLeaderReady() && controlDivision.getInfo().getCurrentTerm() == epoch) : () -> { }) {
                    if (inbound.getMethod().equals("maintenance.boot")) {
                        result.complete(response("OK", "", ByteString.copyFromUtf8(maintenanceGate.bootId()))); return;
                    }
                    if (inbound.getMethod().startsWith("rebuild.") || inbound.getMethod().startsWith("replicated.")) {
                        throw new ClusterException("UNSUPPORTED", "rebuild 数据模式已移除");
                    }
                    if (inbound.getMethod().equals("node.storageSize")) {
                        long bytes;
                        try (var files = Files.walk(config.directory())) {
                            bytes = files.filter(Files::isRegularFile).mapToLong(file -> {
                                try { return Files.size(file); } catch (IOException ex) { throw new java.io.UncheckedIOException(ex); }
                            }).sum();
                        }
                        result.complete(response("OK", "", ByteString.copyFromUtf8(Long.toString(bytes)))); return;
                    }
                    if (inbound.getMethod().equals("ops.metrics")) {
                        result.complete(response("OK", "", metrics())); return;
                    }
                    if (inbound.getMethod().startsWith("metadata.maintenance.") || inbound.getMethod().equals("metadata.transferLeader")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        var json = new com.fasterxml.jackson.databind.ObjectMapper();
                        var output = json.createObjectNode().put("code", "OK");
                        switch (inbound.getMethod()) {
                            case "metadata.transferLeader" -> {
                                String away = json.readTree(inbound.getBody().toByteArray()).path("nodeId").asText();
                                boolean transferred = false;
                                if (away.equals(config.nodeId())) {
                                    var others = division.getRaftConf().getCurrentPeers().stream().filter(peer -> !peer.getId().toString().equals(away)).toList();
                                    if (!others.isEmpty()) {
                                        try (var client = node.client(RaftGroup.valueOf(GroupIds.metadata(config.clusterId()), division.getRaftConf().getAllPeers()))) {
                                            if (!client.admin().transferLeadership(others.getFirst().getId(), 10000).isSuccess())
                                                throw new ClusterException("FAILED", "Metadata Leader 转移失败");
                                        }
                                        transferred = true;
                                    }
                                }
                                output.put("transferred", transferred);
                            }
                            default -> {
                                switch (inbound.getMethod()) {
                                    case "metadata.maintenance.enter" -> output.put("id", maintenanceGate.enter());
                                    case "metadata.maintenance.exit" -> maintenanceGate.exit();
                                    case "metadata.maintenance.status" -> output.setAll(maintenanceGate.status());
                                    default -> throw new ClusterException("UNSUPPORTED", "未知维护命令");
                                }
                            }
                        }
                        result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(output)))); return;
                    }
                    if (inbound.getMethod().equals("metadata.drain") || inbound.getMethod().equals("metadata.remove")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        var body = new com.fasterxml.jackson.databind.ObjectMapper().readTree(inbound.getBody().toByteArray());
                        String target = body.path("nodeId").asText();
                        synchronized (topologyController) {
                            if (inbound.getMethod().equals("metadata.drain")) replicaController.drain(inbound.getContext().getRequestId(), target);
                            else {
                                if (gateway.get(0, 0, TransactionEngine.text("nodes/draining/" + target)) == null) throw new ClusterException("NODE_NOT_DRAINED", "存储节点尚未排空");
                                var directory = new io.cengyan.distributed.DistributedCatalog(gateway);
                                for (GraphDefinition graph : directory.graphs()) for (int part = 0; part < graph.getPartitions(); part++) {
                                    var spec = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                                    if (spec.getMembersList().stream().anyMatch(member -> member.getNodeId().equals(target))) throw new ClusterException("NODE_NOT_DRAINED", "节点仍持有分片");
                                }
                                var listeners = division.getRaftConf().getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).stream()
                                        .filter(peer -> !peer.getId().toString().equals(target)).toList();
                                var remaining = division.getRaftConf().getCurrentPeers().stream().filter(peer -> !peer.getId().toString().equals(target)).toList();
                                if (remaining.isEmpty()) throw new ClusterException("NODE_NOT_DRAINED", "不能移除最后一个存储节点");
                                try (var client = node.client(RaftGroup.valueOf(GroupIds.metadata(config.clusterId()), division.getRaftConf().getAllPeers()))) {
                                    if (target.equals(config.nodeId())) {
                                        var transferred = client.admin().transferLeadership(remaining.getFirst().getId(), 10000);
                                        if (!transferred.isSuccess()) throw new ClusterException("FAILED", "Metadata Leader 转移失败");
                                    }
                                    if (!client.admin().setConfiguration(remaining, listeners).isSuccess()) throw new ClusterException("FAILED", "存储成员移除未确认");
                                }
                                clearNodeMarkers(target);
                            }
                        }
                        result.complete(response("OK", "", ByteString.EMPTY)); return;
                    }
                    if (inbound.getMethod().equals("schema.migrate")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        GraphDefinition target = GraphDefinition.parseFrom(inbound.getBody());
                        synchronized (topologyController) { new SchemaController(gateway).alter(inbound.getContext().getRequestId(), target.getName(), target); }
                        result.complete(response("OK", "", ByteString.EMPTY)); return;
                    }
                    if (inbound.getMethod().equals("schema.acquire") || inbound.getMethod().equals("schema.renew")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        SchemaLeases leases = new SchemaLeases(gateway);
                        SchemaLease granted;
                        if (inbound.getMethod().equals("schema.renew")) granted = leases.renew(SchemaLease.parseFrom(inbound.getBody()));
                        else {
                            var body = new com.fasterxml.jackson.databind.ObjectMapper().readTree(inbound.getBody().toByteArray());
                            granted = leases.acquire(body.path("graph").asText(), inbound.getContext().getRequestId(), body.path("deadline").asLong(),
                                    division.getInfo().getCurrentTerm(), body.path("write").asBoolean());
                        }
                        result.complete(response("OK", "", granted.toByteString())); return;
                    }
                    if (inbound.getMethod().equals("metadata.join")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        Node joining = Node.parseFrom(inbound.getBody());
                        if (!joining.getClusterId().equals(config.clusterId()) || joining.getRole() != NodeRole.STORAGE
                                || !joining.getNodeId().matches("[A-Za-z0-9_-]+")) throw new ClusterException("INVALID_ARGUMENT", "加入节点身份无效");
                        synchronized (topologyController) {
                            var voters = new java.util.ArrayList<>(division.getRaftConf().getCurrentPeers());
                            var listeners = new java.util.ArrayList<>(division.getRaftConf().getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER));
                            String joiningId = joining.getNodeId();
                            boolean voter = voters.removeIf(peer -> peer.getId().toString().equals(joiningId));
                            boolean listener = listeners.removeIf(peer -> peer.getId().toString().equals(joiningId));
                            var group = RaftGroup.valueOf(GroupIds.metadata(config.clusterId()), division.getRaftConf().getAllPeers());
                            try (var client = node.client(group)) {
                                // 同名节点删掉数据后日志是空的。若成员表里还留着旧身份，先摘掉再作为新成员追快照。
                                if (voter || listener) {
                                    if (voters.isEmpty()) throw new ClusterException("FAILED", "不能移除最后一个 Metadata 成员");
                                    if (!client.admin().setConfiguration(voters, listeners).isSuccess()) throw new ClusterException("FAILED", "重新加入前移除旧 Metadata 成员失败");
                                }
                                listeners.add(raftPeer(joining, true));
                                if (!client.admin().setConfiguration(voters, listeners).isSuccess()) throw new ClusterException("FAILED", "Metadata learner 加入失败");
                                voters.add(raftPeer(joining, false));
                                listeners.removeIf(peer -> peer.getId().toString().equals(joiningId));
                                if (!client.admin().setConfiguration(voters, listeners).isSuccess()) throw new ClusterException("FAILED", "Metadata 投票成员加入失败");
                            }
                            clearNodeMarkers(joiningId);
                        }
                        result.complete(response("OK", "", ByteString.EMPTY)); return;
                    }
                    if (inbound.getMethod().equals("metadata.resize")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        var body = new com.fasterxml.jackson.databind.ObjectMapper().readTree(inbound.getBody().toByteArray());
                        synchronized (topologyController) {
                            replicaController.resize(inbound.getContext().getRequestId(), body.path("graph").asText(), body.path("replicas").asInt());
                        }
                        result.complete(response("OK", "", ByteString.EMPTY)); return;
                    }
                    if (inbound.getMethod().equals("import.heartbeat")) {
                        var beat = io.cengyan.protocol.proto.ClusterProtocol.ImportHeartbeat.parseFrom(inbound.getBody());
                        for (RocksStateMachine machine : node.importMachines()) machine.heartbeatImport(beat.getSessionId());
                        result.complete(response("OK", "", ByteString.EMPTY));
                        return;
                    }
                    if (inbound.getMethod().equals("import.stage")) {
                        var chunk = io.cengyan.protocol.proto.ClusterProtocol.ImportChunk.parseFrom(inbound.getBody());
                        var context = inbound.getContext();
                        long graphId = chunk.getGraphId() != 0 ? chunk.getGraphId() : context.getGraphId();
                        int partId = chunk.getGraphId() != 0 ? chunk.getPartId() : context.getPartId();
                        var division = node.server().getDivision(GroupIds.data(config.clusterId(), graphId, partId));
                        if (!(division.getStateMachine() instanceof RocksStateMachine machine)) {
                            result.complete(response("GROUP_UNAVAILABLE", "分片状态机不能接收导入文件", ByteString.EMPTY));
                            return;
                        }
                        machine.stageImport(chunk);
                        result.complete(response("OK", "", ByteString.EMPTY));
                        return;
                    }
                    if (inbound.getMethod().equals("import.prepare")) {
                        var prepare = io.cengyan.protocol.proto.ClusterProtocol.ImportPrepare.parseFrom(inbound.getBody());
                        var division = node.server().getDivision(GroupIds.data(config.clusterId(), prepare.getGraphId(), prepare.getPartId()));
                        if (!(division.getStateMachine() instanceof RocksStateMachine machine)) {
                            result.complete(response("GROUP_UNAVAILABLE", "分片状态机不能归并导入文件", ByteString.EMPTY)); return;
                        }
                        var prepared = machine.prepareImport(prepare.getPublish());
                        result.complete(response("OK", "", prepared.toByteString()));
                        return;
                    }
                    if (inbound.getMethod().equals("import.status")) {
                        var body = new com.fasterxml.jackson.databind.ObjectMapper().readTree(inbound.getBody().toByteArray());
                        long graphId = body.path("graphId").asLong(inbound.getContext().getGraphId());
                        int partId = body.path("part").asInt(inbound.getContext().getPartId());
                        var division = node.server().getDivision(GroupIds.data(config.clusterId(), graphId, partId));
                        if (!(division.getStateMachine() instanceof RocksStateMachine machine)) {
                            result.complete(response("GROUP_UNAVAILABLE", "分片状态机不可用", ByteString.EMPTY)); return;
                        }
                        var status = machine.importStatus(body.path("sessionId").asText());
                        var json = new com.fasterxml.jackson.databind.ObjectMapper();
                        var output = json.createObjectNode()
                                .put("code", "OK").put("nodeId", config.nodeId()).put("graphId", graphId).put("part", partId)
                                .put("runs", status.runs()).put("mergedRuns", status.mergedRuns()).put("coveredRuns", status.coveredRuns())
                                .put("partials", status.partials()).put("bytes", status.bytes()).put("partialBytes", status.partialBytes());
                        output.set("prepared", json.valueToTree(status.prepared()));
                        result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(output))));
                        return;
                    }
                    if (inbound.getMethod().equals("data.status")) {
                        var context = inbound.getContext();
                        var division = node.server().getDivision(context.getGraphId() == 0 ? GroupIds.metadata(config.clusterId())
                                : GroupIds.data(config.clusterId(), context.getGraphId(), context.getPartId()));
                        var status = GroupStatus.newBuilder().setTerm(division.getInfo().getCurrentTerm())
                                .setAppliedIndex(division.getInfo().getLastAppliedIndex());
                        if (division.getInfo().getLeaderId() != null) status.setLeaderId(division.getInfo().getLeaderId().toString());
                        division.getRaftConf().getCurrentPeers().forEach(peer -> status.addMembers(peer.getId().toString()));
                        result.complete(response("OK", "", status.build().toByteString())); return;
                    }
                    if (inbound.getMethod().equals("data.configure")) {
                        DataGroupSpec spec = DataGroupSpec.parseFrom(inbound.getBody());
                        if (!spec.getControlTaskId().isBlank()) {
                            ByteString owner = gateway.get(0, 0, TransactionEngine.text("tasks/activeReplica/" + spec.getGraphId() + "/" + spec.getPartId()));
                            if (owner == null || !owner.toStringUtf8().equals(spec.getControlTaskId())) throw new ClusterException("STALE_CONTROLLER", "副本任务已被替换或结束");
                        } else throw new ClusterException("INVALID_ARGUMENT", "分片成员变更必须关联持久化控制任务");
                        var id = GroupIds.data(config.clusterId(), spec.getGraphId(), spec.getPartId());
                        var division = node.server().getDivision(id);
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        synchronized (configurationLocks.computeIfAbsent(id, ignored -> new Object())) {
                        verifyConfigurationTask(spec);
                        var group = RaftGroup.valueOf(id, division.getRaftConf().getAllPeers());
                        try (var client = node.client(group)) {
                            var voters = new java.util.ArrayList<>(division.getRaftConf().getCurrentPeers());
                            var listeners = new java.util.ArrayList<>(division.getRaftConf().getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER));
                            for (Node desired : spec.getMembersList()) {
                                if (voters.stream().anyMatch(peer -> peer.getId().toString().equals(desired.getNodeId()))) continue;
                                verifyConfigurationTask(spec);
                                var previousVoters = java.util.List.copyOf(voters);
                                var previousListeners = java.util.List.copyOf(listeners);
                                listeners.removeIf(peer -> peer.getId().toString().equals(desired.getNodeId()));
                                voters.add(raftPeer(desired, false));
                                var promoted = client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
                                        .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET).setServersInCurrentConf(previousVoters)
                                        .setListenersInCurrentConf(previousListeners).setServersInNewConf(voters).setListenersInNewConf(listeners).build());
                                if (!promoted.isSuccess()) throw new ClusterException("FAILED", "Raft 单成员提升失败：" + promoted.getException());
                            }
                            verifyConfigurationTask(spec);
                            if (spec.getMembersList().stream().noneMatch(member -> member.getNodeId().equals(config.nodeId()))) {
                                var transfer = client.admin().transferLeadership(org.apache.ratis.protocol.RaftPeerId.valueOf(spec.getMembers(0).getNodeId()), 10000);
                                if (!transfer.isSuccess()) throw new ClusterException("FAILED", "迁移前 Leader 转移失败");
                            }
                            var changed = client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
                                    .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET).setServersInCurrentConf(voters).setListenersInCurrentConf(listeners)
                                    .setServersInNewConf(spec.getMembersList().stream().map(member -> raftPeer(member, false)).toList())
                                    .setListenersInNewConf(spec.getLearnersList().stream().map(member -> raftPeer(member, true)).toList()).build());
                            if (!changed.isSuccess()) throw new ClusterException("FAILED", "Raft 成员变更未成功确认：" + changed.getException());
                        }
                        }
                        result.complete(response("OK", "", ByteString.EMPTY)); return;
                    }
                    if (inbound.getMethod().startsWith("auth.")) {
                        var division = node.server().getDivision(GroupIds.metadata(config.clusterId()));
                        if (!division.getInfo().isLeaderReady()) { result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return; }
                        var json = new com.fasterxml.jackson.databind.ObjectMapper();
                        var body = json.readTree(inbound.getBody().toByteArray());
                        switch (inbound.getMethod()) {
                            case "auth.initialize" -> { authority.initialize(body.path("rootPassword").asText()); result.complete(response("OK", "", ByteString.EMPTY)); }
                            case "auth.login" -> {
                                String token = authority.login(body.path("user").asText(), body.path("password").asText());
                                var output = json.createObjectNode().put("token", token);
                                result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(output))));
                            }
                            case "auth.verify" -> result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(authority.verify(body.path("token").asText())))));
                            case "auth.publicKey" -> result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(java.util.Map.of("publicKey", authority.publicKey())))));
                            case "auth.user" -> {
                                var user = authority.publicUser(body.path("user").asText());
                                result.complete(response("OK", "", ByteString.copyFrom(json.writeValueAsBytes(user))));
                            }
                            case "auth.create" -> { authority.create(inbound.getContext().getRequestId(), body.path("user").asText(), body.path("password").asText(), body.path("role").asText()); result.complete(response("OK", "", ByteString.EMPTY)); }
                            case "auth.update" -> {
                                authority.update(inbound.getContext().getRequestId(), body.path("user").asText(), body.has("password") ? body.path("password").asText() : null,
                                        body.has("role") ? body.path("role").asText() : null, body.has("enabled") ? body.path("enabled").asBoolean() : null);
                                result.complete(response("OK", "", ByteString.EMPTY));
                            }
                            default -> result.complete(response("UNSUPPORTED", "未知认证方法", ByteString.EMPTY));
                        }
                        return;
                    }
                    if (inbound.getMethod().equals("metadata.nodes")) {
                        var nodes = DataGroupSpec.newBuilder();
                        for (var peer : node.server().getDivision(GroupIds.metadata(config.clusterId())).getRaftConf().getCurrentPeers()) {
                            var raft = StorageConfig.endpoint(peer.getAddress());
                            var business = StorageConfig.endpoint(peer.getClientAddress());
                            Node descriptor = Node.newBuilder().setClusterId(config.clusterId()).setNodeId(peer.getId().toString()).setRole(NodeRole.STORAGE)
                                    .setRpc(Endpoint.newBuilder().setHost(business.getHostString()).setPort(business.getPort()))
                                    .setRaft(Endpoint.newBuilder().setHost(raft.getHostString()).setPort(raft.getPort())).build();
                            // 状态机本地已应用的节点标签仅作放置建议，成员身份仍以 Raft 配置为准。
                            var local = node.server().getDivision(GroupIds.metadata(config.clusterId())).getStateMachine()
                                    .query(RocksStateMachine.message(ReadKey.newBuilder().setKey(TransactionEngine.text("nodes/descriptor/" + peer.getId())).build().toByteArray())).join();
                            var saved = StoredValue.parseFrom(local.getContent().toByteArray());
                            if (saved.getFound()) {
                                Node tagged = Node.parseFrom(saved.getValue());
                                descriptor = descriptor.toBuilder().setZone(tagged.getZone()).setRack(tagged.getRack()).setDataMode(tagged.getDataMode()).build();
                            }
                            nodes.addMembers(descriptor);
                        }
                        result.complete(response("OK", "", nodes.build().toByteString()));
                        return;
                    }
                    if (inbound.getMethod().equals("tx.execute")) {
                        TransactionPlan plan = TransactionEngine.normalize(TransactionPlan.parseFrom(inbound.getBody()));
                        // BEGIN/DECIDE/FINALIZED CAS includes a full previous and next record.
                        long requiredBytes = 2L * plan.getSerializedSize() + 64 * 1024;
                        if (requiredBytes > config.rpcMaxPayload()) {
                            result.complete(response("BATCH_TOO_LARGE", "事务状态 CAS 超过 Storage RPC 配置；planBytes="
                                    + plan.getSerializedSize() + " requiredBytes=" + requiredBytes + " rpc_max_payload_bytes="
                                    + config.rpcMaxPayload() + "；请把 rpc_max_payload_bytes 调到不小于 " + requiredBytes, ByteString.EMPTY));
                            return;
                        }
                        var division = node.server().getDivision(GroupIds.data(config.clusterId(), plan.getGraphId(), plan.getCoordinatorPart()));
                        if (!division.getInfo().isLeader()) {
                            result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers())); return;
                        }
                        TransactionRecord record = transactions.execute(plan);
                        result.complete(response(record.getState() == TransactionState.COMMITTED ? "OK" : record.getErrorCode(),
                                record.getErrorMessage(), record.toByteString()));
                        return;
                    }
                    if (inbound.getMethod().equals("data.addGroup")) {
                        addGroup(inbound);
                        result.complete(response("OK", "", ByteString.EMPTY));
                        return;
                    }
                    if (inbound.getMethod().equals("data.removeGroup")) {
                        removeGroup(inbound);
                        result.complete(response("OK", "", ByteString.EMPTY));
                        return;
                    }
                    boolean read = inbound.getMethod().equals("kv.read");
                    if (!read && !inbound.getMethod().equals("kv.write")) {
                        result.complete(response("UNSUPPORTED", "未知 Storage RPC 方法", ByteString.EMPTY));
                        return;
                    }
                    var context = inbound.getContext();
                    RaftGroupId groupId = context.getGraphId() == 0 ? GroupIds.metadata(config.clusterId())
                            : GroupIds.data(config.clusterId(), context.getGraphId(), context.getPartId());
                    var division = node.server().getDivision(groupId);
                    if (!division.getInfo().isLeaderReady()) {
                        result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers()));
                        return;
                    }
                    if (read) ReadKey.parseFrom(inbound.getBody());
                    else {
                        var command = WriteBatch.parseFrom(inbound.getBody());
                        if (inbound.getContext().getControllerEpoch() != command.getControllerEpoch()) {
                            result.complete(response("INVALID_ARGUMENT", "控制命令任期与请求上下文不一致", ByteString.EMPTY)); return;
                        }
                        if (!command.getRequestId().equals(context.getRequestId()) || command.getRequestId().isBlank()) {
                            result.complete(response("INVALID_ARGUMENT", "写入 requestId 与请求上下文不一致", ByteString.EMPTY));
                            return;
                        }
                    }
                    if (result.isCancelled() || System.nanoTime() - deadline >= 0) {
                        result.complete(response("QUEUE_TIMEOUT", "Storage 请求在提交前已超时", ByteString.EMPTY));
                        return;
                    }
                    var raftRequest = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId())
                            .setGroupId(groupId).setCallId(calls.incrementAndGet())
                            .setType(read ? RaftClientRequest.readRequestType() : RaftClientRequest.writeRequestType())
                            .setTimeoutMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))
                            .setMessage(RocksStateMachine.message(inbound.getBody().toByteArray())).build();
                    node.server().submitClientRequestAsync(raftRequest).whenComplete((reply, failure) -> {
                        if (failure != null) result.complete(response("FAILED", "Raft 未返回确认，写入状态可能未知", ByteString.EMPTY));
                        else if (reply.isSuccess()) {
                            ByteString body = ByteString.copyFrom(reply.getMessage().getContent().toByteArray());
                            if (read) result.complete(response("OK", "", body));
                            else {
                                try {
                                    var outcome = BatchResult.parseFrom(body);
                                    result.complete(response(outcome.getCode(), outcome.getMessage(), body));
                                } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                                    result.complete(response("FAILED", "Raft 状态机响应无法解析", ByteString.EMPTY));
                                }
                            }
                        }
                        else if (reply.getNotLeaderException() != null || reply.getLeaderNotReadyException() != null || reply.getLeaderSteppingDownException() != null || (read && reply.getReadIndexException() != null)) {
                            result.complete(notLeader(division.getInfo(), division.getRaftConf().getAllPeers()));
                        } else result.complete(response("FAILED", "Raft 请求未成功："
                                + (reply.getException() == null ? "未确认" : reply.getException().getClass().getSimpleName()), ByteString.EMPTY));
                    });
                } catch (ClusterException ex) {
                    result.complete(response(ex.code(), ex.getMessage(), ByteString.EMPTY));
                } catch (IllegalArgumentException | com.google.protobuf.InvalidProtocolBufferException ex) {
                    result.complete(response("INVALID_ARGUMENT", "Storage 请求参数无效：" + ex.getMessage(), ByteString.EMPTY));
                } catch (IOException ex) {
                    // 导入的暂存、归并失败原因各不相同（缺上传段、磁盘满、校验和不对），原样返回给客户端。
                    if (inbound.getMethod().startsWith("import.")) {
                        result.complete(response("FAILED", ex.getMessage() == null ? ex.toString() : ex.getMessage(), ByteString.EMPTY));
                    } else result.complete(response("GROUP_UNAVAILABLE", "分片未就绪或不属于本节点", ByteString.EMPTY));
                } catch (RuntimeException ex) {
                    result.complete(response("FAILED", "Storage 请求失败，提交状态可能未知", ByteString.EMPTY));
                } catch (Exception ex) {
                    result.complete(response("FAILED", "Storage 控制任务失败", ByteString.EMPTY));
                }
            });
        } catch (RejectedExecutionException ex) {
            result.complete(response("BUSY", "Storage 准入队列已满，尚未提交", ByteString.EMPTY));
        }
        return result;
    }

    /**
     * 每类请求用自己的线程池，慢请求只会排在同类后面。导入心跳和监控采集曾经和 Metadata 读写共用准入池，
     * 一旦它们等分片锁，认证和路由查询就跟着超时。
     */
    private ThreadPoolExecutor pool(RpcRequest request) {
        String method = request.getMethod();
        if (method.equals("tx.execute")) return writers;
        if (method.startsWith("import.")) return importRequests;
        if (method.equals("ops.metrics") || method.equals("node.storageSize")) return opsRequests;
        if (method.startsWith("kv.") || method.equals("data.status")) return request.getContext().getGraphId() > 0 ? dataRequests : metadataRequests;
        if (method.startsWith("auth.") || method.equals("data.configure") || method.startsWith("metadata.") || method.startsWith("schema.")) return coordinators;
        return admission;
    }

    private com.google.protobuf.ByteString metrics() {
        var json = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
        json.put("code", "OK").put("nodeId", config.nodeId()).put("hostname", HostMetrics.hostname()).put("role", "STORAGE");
        json.put("dataStorageMode", "raft");
        var memory = ManagementFactory.getMemoryMXBean();
        var threads = ManagementFactory.getThreadMXBean();
        json.put("heapUsedBytes", memory.getHeapMemoryUsage().getUsed());
        json.put("heapCommittedBytes", memory.getHeapMemoryUsage().getCommitted());
        json.put("heapMaxBytes", memory.getHeapMemoryUsage().getMax());
        json.put("threadCount", threads.getThreadCount());
        json.put("daemonThreadCount", threads.getDaemonThreadCount());
        json.put("writeThreads", config.writeThreads());
        json.put("coordinatorThreads", config.coordinatorThreads());
        json.put("transactionRecoveryThreads", config.transactionRecoveryThreads());
        json.put("transactionRecoveryIntervalMs", config.transactionRecoveryIntervalMs());
        json.put("writeActive", writers.getActiveCount());
        json.put("writeQueued", writers.getQueue().size());
        json.put("writeParticipantActive", writeParticipants.getActiveCount());
        json.put("writeParticipantQueued", writeParticipants.getQueue().size());
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean platform) {
            json.put("processCpuLoad", platform.getProcessCpuLoad());
            json.put("systemCpuLoad", platform.getCpuLoad());
            json.put("systemMemoryBytes", platform.getTotalMemorySize());
            json.put("systemFreeMemoryBytes", platform.getFreeMemorySize());
        }
        try {
            var disk = Files.getFileStore(config.directory());
            json.put("diskTotalBytes", disk.getTotalSpace());
            json.put("diskUsableBytes", disk.getUsableSpace());
        } catch (IOException ignored) { }
        hostMetrics.current().putDisk(json);
        try {
            var metadata = node.server().getDivision(GroupIds.metadata(config.clusterId()));
            json.put("metadataTerm", metadata.getInfo().getCurrentTerm());
            json.put("metadataAppliedIndex", metadata.getInfo().getLastAppliedIndex());
            if (metadata.getInfo().getLeaderId() != null) json.put("metadataLeaderId", metadata.getInfo().getLeaderId().toString());
        } catch (Exception ignored) { }
        appendShards(json);
        return com.google.protobuf.ByteString.copyFromUtf8(json.toString());
    }

    /** 本机 Metadata 状态机里的分片身份，加上本机 Raft 进度。不向其他 Storage 发请求。 */
    private void appendShards(com.fasterxml.jackson.databind.node.ObjectNode json) {
        var shards = json.putArray("shards");
        try {
            var metadata = node.server().getDivision(GroupIds.metadata(config.clusterId()));
            if (!(metadata.getStateMachine() instanceof RocksStateMachine rocks)) return;
            var names = new java.util.HashMap<Long, String>();
            scan(rocks, "catalog/graph/", entry -> {
                try {
                    GraphDefinition graph = GraphDefinition.parseFrom(entry.getValue());
                    if (!"DROPPED".equals(graph.getState())) names.put(graph.getId(), graph.getName());
                } catch (Exception ignored) { }
            });
            var hosted = new java.util.HashSet<RaftGroupId>();
            for (var id : node.server().getGroupIds()) hosted.add(id);
            scan(rocks, "catalog/group/", entry -> {
                try {
                    DataGroupSpec spec = DataGroupSpec.parseFrom(entry.getValue());
                    if (spec.getGraphId() <= 0) return;
                    RaftGroupId id = GroupIds.data(config.clusterId(), spec.getGraphId(), spec.getPartId());
                    if (!hosted.contains(id)) return;
                    var division = node.server().getDivision(id);
                    var info = division.getInfo();
                    var shard = shards.addObject();
                    shard.put("graphId", spec.getGraphId());
                    shard.put("graph", names.getOrDefault(spec.getGraphId(), ""));
                    shard.put("partId", spec.getPartId());
                    shard.put("term", info.getCurrentTerm());
                    shard.put("appliedIndex", info.getLastAppliedIndex());
                    if (division.getStateMachine() instanceof RocksStateMachine dataMachine) {
                        var dataUsage = dataMachine.physicalUsage();
                        var usage = shard.putObject("physicalUsageBytes");
                        dataUsage.forEach(usage::put);
                        shard.put("physicalBytes", dataUsage.values().stream().mapToLong(Long::longValue).sum());
                    }
                    if (info.getLeaderId() != null) shard.put("leaderId", info.getLeaderId().toString());
                    shard.put("leader", info.isLeader());
                    var members = shard.putArray("members");
                    for (var peer : division.getRaftConf().getCurrentPeers()) members.add(peer.getId().toString());
                } catch (Exception ignored) { }
            });
        } catch (Exception ignored) { }
    }

    private void scan(RocksStateMachine rocks, String prefix, java.util.function.Consumer<KeyValue> consumer) throws Exception {
        String cursor = "";
        ByteString after = ByteString.EMPTY;
        boolean more = false;
        try {
            for (int page = 0; page < 50; page++) {
                var key = ReadKey.newBuilder().setKey(ByteString.copyFromUtf8(prefix)).setScan(true).setLimit(200).setMaxBytes(1024 * 1024);
                if (cursor.isEmpty()) key.setScanId(java.util.UUID.randomUUID().toString());
                else key.setCursorId(cursor).setAfterKey(after);
                var reply = rocks.query(RocksStateMachine.message(key.build().toByteArray())).get(500, TimeUnit.MILLISECONDS);
                ScanPage scanned = ScanPage.parseFrom(reply.getContent().toByteArray());
                for (var entry : scanned.getEntriesList()) consumer.accept(entry);
                more = scanned.getMore();
                if (!more) return;
                cursor = scanned.getCursorId();
                after = scanned.getLastKey();
            }
        } finally {
            if (more && !cursor.isEmpty()) {
                try {
                    rocks.query(RocksStateMachine.message(ReadKey.newBuilder().setCloseCursor(true).setCursorId(cursor).build().toByteArray()))
                            .get(500, TimeUnit.MILLISECONDS);
                } catch (Exception ignored) { }
            }
        }
    }
    private Node localDescriptor() {
        return Node.newBuilder().setClusterId(config.clusterId()).setNodeId(config.nodeId()).setRole(NodeRole.STORAGE)
                .setRpc(Endpoint.newBuilder().setHost(config.rpcAddress().getHostString()).setPort(config.rpcAddress().getPort()))
                .setRaft(Endpoint.newBuilder().setHost(config.raftAddress().getHostString()).setPort(config.raftAddress().getPort()))
                .setZone(config.zone()).setRack(config.rack()).setDataMode("raft").build();
    }
    private void verifyConfigurationTask(DataGroupSpec requested) {
        ByteString bytes = gateway.get(0, 0, TransactionEngine.text("tasks/replica/" + requested.getControlTaskId()));
        if (bytes == null) throw new ClusterException("STALE_CONTROLLER", "成员变更没有持久化任务");
        try {
            ReplicaTask task = ReplicaTask.parseFrom(bytes);
            DataGroupSpec desired = requested.toBuilder().clearControlTaskId().build();
            boolean staging = !requested.getLearnersList().isEmpty();
            if (staging) {
                if (!task.getStage().equals("CREATED") || !desired.getMembersList().equals(task.getBefore().getMembersList())) {
                    throw new ClusterException("STALE_CONTROLLER", "learner 命令的任务阶段已结束，拒绝回退成员配置");
                }
            } else if (!task.getStage().equals("CATCHING_UP") || !desired.equals(task.getAfter())) {
                throw new ClusterException("STALE_CONTROLLER", "提升或移除命令与任务阶段不一致");
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "成员变更任务损坏", ex); }
    }

    /** 超过 30 分钟没有心跳的导入会话，删除各分片上的暂存文件。 */
    private void expireImports() {
        if (closed.get()) return;
        long now = System.currentTimeMillis();
        for (RocksStateMachine machine : node.importMachines()) {
            try { machine.expireImports(now); }
            catch (Exception ex) { org.slf4j.LoggerFactory.getLogger(StorageServer.class).debug("导入暂存清理等待重试：{}", ex.toString()); }
        }
    }

    private void recoverTransactions() {
        if (closed.get()) return;
        boolean purgeDue = System.currentTimeMillis() - lastIdempotencyCleanup > TimeUnit.MINUTES.toMillis(5);
        var leaders = new ArrayList<org.apache.ratis.protocol.RaftGroupId>();
        try {
            for (var groupId : node.server().getGroupIds()) {
                if (node.server().getDivision(groupId).getInfo().isLeader()) leaders.add(groupId);
            }
        } catch (Exception ex) {
            org.slf4j.LoggerFactory.getLogger(StorageServer.class).debug("事务恢复等待下次重试：{}", ex.toString());
            return;
        }
        if (leaders.isEmpty()) {
            if (purgeDue) lastIdempotencyCleanup = System.currentTimeMillis();
            return;
        }
        var done = new CountDownLatch(leaders.size());
        for (var groupId : leaders) {
            try {
                recoveryWorkers.execute(() -> {
                    try { recoverGroup(groupId, purgeDue); }
                    catch (Exception ex) { org.slf4j.LoggerFactory.getLogger(StorageServer.class).debug("分片事务恢复等待下次重试：{}", ex.toString()); }
                    finally { done.countDown(); }
                });
            } catch (RejectedExecutionException ex) { done.countDown(); }
        }
        try { done.await(); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); return; }
        if (purgeDue) lastIdempotencyCleanup = System.currentTimeMillis();
    }

    private void recoverGroup(org.apache.ratis.protocol.RaftGroupId groupId, boolean purgeDue) throws Exception {
        var read = ReadKey.newBuilder().setKey(TransactionEngine.TX_PREFIX).setScan(true).setLimit(100).setMaxBytes(7 * 1024 * 1024).build();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            if (closed.get()) return;
            var request = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId()).setGroupId(groupId)
                    .setCallId(calls.incrementAndGet()).setType(RaftClientRequest.readRequestType()).setTimeoutMs(5000)
                    .setMessage(RocksStateMachine.message(read.toBuilder().setAfterKey(after).setCursorId(cursor).build().toByteArray())).build();
            var reply = node.server().submitClientRequestAsync(request).get(5, TimeUnit.SECONDS);
            if (!reply.isSuccess()) break;
            ScanPage page = ScanPage.parseFrom(reply.getMessage().getContent().toByteArray());
            for (var row : page.getEntriesList()) {
                TransactionRecord record = TransactionRecord.parseFrom(row.getValue());
                if (!record.getFinalized() && TransactionEngine.acquire(record.getPlan().getTransactionId())) {
                    try { transactions.resume(record); }
                    finally { TransactionEngine.release(record.getPlan().getTransactionId()); }
                }
                else if (record.getPlan().getParticipantsCount() > 0) versions.collect(record, System.currentTimeMillis());
            }
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (!closed.get());
        ByteString gcAfter = ByteString.EMPTY;
        String gcCursor = "";
        do {
            if (closed.get()) return;
            var request = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId()).setGroupId(groupId)
                    .setCallId(calls.incrementAndGet()).setType(RaftClientRequest.readRequestType()).setTimeoutMs(5000)
                    .setMessage(RocksStateMachine.message(ReadKey.newBuilder().setKey(TransactionEngine.TX_GC_PREFIX).setScan(true)
                            .setLimit(100).setMaxBytes(1024 * 1024).setAfterKey(gcAfter).setCursorId(gcCursor).build().toByteArray())).build();
            var reply = node.server().submitClientRequestAsync(request).get(5, TimeUnit.SECONDS);
            if (!reply.isSuccess()) break;
            ScanPage page = ScanPage.parseFrom(reply.getMessage().getContent().toByteArray());
            for (var row : page.getEntriesList()) versions.collect(TransactionRecord.parseFrom(row.getValue()), System.currentTimeMillis());
            if (!page.getMore()) break;
            gcAfter = page.getLastKey();
            gcCursor = page.getCursorId();
        } while (!closed.get());
        ByteString deleteAfter = ByteString.EMPTY;
        String deleteCursor = "";
        do {
            if (closed.get()) return;
            var request = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId()).setGroupId(groupId)
                    .setCallId(calls.incrementAndGet()).setType(RaftClientRequest.readRequestType()).setTimeoutMs(5000)
                    .setMessage(RocksStateMachine.message(ReadKey.newBuilder().setKey(TransactionEngine.text("__delete/")).setScan(true)
                            .setLimit(100).setMaxBytes(4 * 1024 * 1024).setAfterKey(deleteAfter).setCursorId(deleteCursor).build().toByteArray())).build();
            var reply = node.server().submitClientRequestAsync(request).get(5, TimeUnit.SECONDS);
            if (!reply.isSuccess()) break;
            ScanPage page = ScanPage.parseFrom(reply.getMessage().getContent().toByteArray());
            for (var row : page.getEntriesList()) {
                DeleteTask task = DeleteTask.parseFrom(row.getValue());
                if (!task.getCompleted()) new DistributedGraph(gateway).resumeDelete(task);
            }
            if (!page.getMore()) break;
            deleteAfter = page.getLastKey();
            deleteCursor = page.getCursorId();
        } while (!closed.get());
        if (purgeDue) cleanupIdempotency(groupId);
    }
    private void cleanupIdempotency(org.apache.ratis.protocol.RaftGroupId groupId) throws Exception {
        ByteString after = ByteString.EMPTY; String cursor = "";
        final long cutoff = System.currentTimeMillis();
        do {
            ReadKey scan = ReadKey.newBuilder().setScan(true).setScanIdempotency(true).setAfterKey(after).setCursorId(cursor)
                    .setLimit(256).setMaxBytes(1024 * 1024).build();
            var request = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId()).setGroupId(groupId)
                    .setCallId(calls.incrementAndGet()).setType(RaftClientRequest.readRequestType()).setTimeoutMs(5000)
                    .setMessage(RocksStateMachine.message(scan.toByteArray())).build();
            var reply = node.server().submitClientRequestAsync(request).get(5, TimeUnit.SECONDS);
            if (!reply.isSuccess()) return;
            ScanPage page = ScanPage.parseFrom(reply.getMessage().getContent().toByteArray());
            var purge = StorageGateway.batch("maintenance.purge/" + java.util.UUID.randomUUID()).setPurgeBeforeMs(cutoff);
            for (var item : page.getEntriesList()) {
                IdempotencyRecord record = IdempotencyRecord.parseFrom(item.getValue());
                String id = item.getKey().toStringUtf8();
                // 活跃事务的子操作不得因 TTL 清掉。事务记录清理另有生命周期，当前保守保留其子操作。
                if (record.getExpiresAtMs() <= cutoff && !id.startsWith("tx.") && !id.startsWith("schema.") && !id.startsWith("delete.")) purge.addPurgeRequestIds(id);
            }
            if (purge.getPurgeRequestIdsCount() > 0) {
                var command = RaftClientRequest.newBuilder().setClientId(clientId).setServerId(node.server().getId()).setGroupId(groupId)
                        .setCallId(calls.incrementAndGet()).setType(RaftClientRequest.writeRequestType()).setTimeoutMs(5000)
                        .setMessage(RocksStateMachine.message(purge.build().toByteArray())).build();
                var removed = node.server().submitClientRequestAsync(command).get(5, TimeUnit.SECONDS);
                if (!removed.isSuccess()) return;
            }
            if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
        } while (!closed.get());
    }

    private void addGroup(RpcRequest request) throws IOException {
        var spec = DataGroupSpec.parseFrom(request.getBody());
        if (spec.getMembersCount() < 1) throw new IllegalArgumentException("Data Group 成员不能为空");
        var peers = new ArrayList<RaftPeer>();
        var ids = new java.util.HashSet<String>();
        var raftAddresses = new java.util.HashSet<java.net.InetSocketAddress>();
        var rpcAddresses = new java.util.HashSet<java.net.InetSocketAddress>();
        var allNodes = new java.util.ArrayList<>(spec.getMembersList()); allNodes.addAll(spec.getLearnersList());
        for (var member : allNodes) {
            if (!config.clusterId().equals(member.getClusterId()) || member.getRole() != NodeRole.STORAGE
                    || !member.getNodeId().matches("[A-Za-z0-9_-]+") || !ids.add(member.getNodeId())) throw new IllegalArgumentException("Data Group 成员无效");
            var raft = new java.net.InetSocketAddress(member.getRaft().getHost(), member.getRaft().getPort());
            var address = new java.net.InetSocketAddress(member.getRpc().getHost(), member.getRpc().getPort());
            if (raft.getPort() < 1 || address.getPort() < 1 || member.getRaft().getHost().isBlank()
                    || member.getRpc().getHost().isBlank() || !raftAddresses.add(raft) || !rpcAddresses.add(address)) {
                throw new IllegalArgumentException("成员地址无效或重复");
            }
            if (member.getNodeId().equals(config.nodeId())
                    && (!raft.equals(config.raftAddress()) || !address.equals(config.rpcAddress()))) {
                throw new IllegalArgumentException("本节点 Data Group 地址与实际配置不一致");
            }
            peers.add(raftPeer(member, spec.getLearnersList().contains(member)));
        }
        if (!ids.contains(config.nodeId())) throw new IllegalArgumentException("Data Group 不包含本节点");
        node.addGroup(RaftGroup.valueOf(GroupIds.data(config.clusterId(), spec.getGraphId(), spec.getPartId()), peers));
    }

    /** 删掉节点后清掉排空、描述和离线标记，同名节点才能再次加入并接收副本。 */
    private void clearNodeMarkers(String nodeId) {
        for (String prefix : java.util.List.of("nodes/draining/", "nodes/descriptor/", "health/offline/")) {
            ByteString key = TransactionEngine.text(prefix + nodeId);
            ByteString current = gateway.get(0, 0, key);
            if (current == null) continue;
            try {
                gateway.write(0, 0, StorageGateway.batch("node.forget/" + nodeId + "/" + prefix)
                        .addConditions(StorageGateway.condition(key, current))
                        .addMutations(Mutation.newBuilder().setKey(key).setDelete(true)).build());
            } catch (ClusterException ex) {
                if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
            }
        }
    }

    private void removeGroup(RpcRequest request) throws IOException {
        DataGroupSpec spec = DataGroupSpec.parseFrom(request.getBody());
        if (spec.getGraphId() <= 0 || spec.getPartId() < 0) throw new IllegalArgumentException("Data Group 身份无效");
        if (!dropCleanup.graphDropped(spec.getGraphId())) throw new ClusterException("GRAPH_STATE", "只能清理已经删除的图");
        // 只卸下 Group 并改名。清空文件由后台立刻继续，避免这次 RPC 被大目录拖超时。
        node.detachDataGroup(GroupIds.data(config.clusterId(), spec.getGraphId(), spec.getPartId()));
    }
    private static RaftPeer raftPeer(Node member, boolean learner) {
        return RaftPeer.newBuilder().setId(member.getNodeId())
                .setAddress(new java.net.InetSocketAddress(member.getRaft().getHost(), member.getRaft().getPort()))
                .setClientAddress(new java.net.InetSocketAddress(member.getRpc().getHost(), member.getRpc().getPort()))
                .setStartupRole(learner ? org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER : org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).build();
    }

    private RpcResponse notLeader(DivisionInfo info, java.util.Collection<RaftPeer> peers) {
        var response = RpcResponse.newBuilder().setCode("NOT_LEADER").setMessage("请更新分片 Leader 路由");
        var leader = info.getLeaderId();
        var hint = LeaderHint.newBuilder().setTerm(info.getCurrentTerm());
        if (leader != null) {
            hint.setNodeId(leader.toString());
            for (var peer : peers) if (peer.getId().equals(leader) && peer.getClientAddress() != null) {
                var address = StorageConfig.endpoint(peer.getClientAddress());
                hint.setRpc(Endpoint.newBuilder().setHost(address.getHostString()).setPort(address.getPort()));
            }
        }
        return response.setLeaderHint(hint).build();
    }

    private static RpcResponse response(String code, String message, ByteString body) {
        return RpcResponse.newBuilder().setCode(code).setMessage(message).setBody(body).build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("用法：java -jar cengyan-storage.jar storage.properties");
        StorageServer server = new StorageServer(StorageConfig.load(Path.of(args[0])));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.close(); } catch (IOException ex) { System.err.println("Storage 关闭失败：" + ex.getMessage()); }
        }, "storage-shutdown"));
        new CountDownLatch(1).await();
    }

    @Override public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        hostMetrics.close();
        if (dropCleanup != null) dropCleanup.close();
        rpc.close();
        maintenance.shutdownNow();
        joining.shutdownNow();
        recovery.shutdownNow();
        recoveryWorkers.shutdownNow();
        topologyRecovery.shutdownNow();
        topologyWorkers.shutdownNow();
        coordinators.shutdownNow();
        writers.shutdownNow();
        writeParticipants.shutdownNow();
        dataRequests.shutdownNow();
        replicaRequests.shutdownNow();
        metadataRequests.shutdownNow();
        importRequests.shutdownNow();
        opsRequests.shutdownNow();
        admission.shutdownNow();
        try {
            maintenance.awaitTermination(10, TimeUnit.SECONDS);
            recovery.awaitTermination(10, TimeUnit.SECONDS);
            recoveryWorkers.awaitTermination(10, TimeUnit.SECONDS);
            topologyRecovery.awaitTermination(10, TimeUnit.SECONDS);
            topologyWorkers.awaitTermination(10, TimeUnit.SECONDS);
            coordinators.awaitTermination(10, TimeUnit.SECONDS);
            writers.awaitTermination(10, TimeUnit.SECONDS);
            writeParticipants.awaitTermination(10, TimeUnit.SECONDS);
            dataRequests.awaitTermination(10, TimeUnit.SECONDS);
            replicaRequests.awaitTermination(10, TimeUnit.SECONDS);
            metadataRequests.awaitTermination(10, TimeUnit.SECONDS);
            importRequests.awaitTermination(10, TimeUnit.SECONDS);
            opsRequests.awaitTermination(10, TimeUnit.SECONDS);
            admission.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        gateway.close();
        node.close();
    }

    private static ThreadPoolExecutor writePool(int threads, String name) {
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(2048), task -> {
                    Thread thread = new Thread(task, name); thread.setDaemon(true); return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }
}

