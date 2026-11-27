package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftNode;
import io.cengyan.raft.RocksStateMachine;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

final class ReplicaBootstrap {
    private static final int CHUNK_SIZE = 512 * 1024;
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(30);

    private final StorageConfig config;
    private final StorageGateway gateway;
    private final RaftNode node;
    private final Executor executor;
    private final Path staging;
    private final Map<RaftGroupId, CompletableFuture<Void>> copies = new ConcurrentHashMap<>();
    private final Map<RaftGroupId, ManifestJob> manifests = new ConcurrentHashMap<>();
    private final Map<RaftGroupId, Integer> publishing = new ConcurrentHashMap<>();

    private static final class ManifestJob {
        final String task;
        final RocksStateMachine machine;
        final CompletableFuture<SnapshotInfo> creation;
        volatile long touched = System.nanoTime();

        ManifestJob(String task, RocksStateMachine machine, CompletableFuture<SnapshotInfo> creation) {
            this.task = task;
            this.machine = machine;
            this.creation = creation;
        }
        void close() { creation.thenAccept(machine::releaseCheckpoint); }
    }

    ReplicaBootstrap(StorageConfig config, StorageGateway gateway, RaftNode node, Executor executor) throws IOException {
        this.config = config;
        this.gateway = gateway;
        this.node = node;
        this.executor = executor;
        staging = config.directory().resolve("replica-bootstrap");
        if (Files.isDirectory(staging)) {
            try (var files = Files.list(staging)) {
                for (Path path : files.toList()) {
                    if (path.getFileName().toString().matches("[0-9a-f-]{36}-[0-9]+")) removeDownload(path);
                }
            }
        }
    }

    void beginPublish(RaftGroupId id, long graph, int part) {
        publishing.merge(id, 1, Integer::sum);
        try {
            if (gateway.get(0, 0, activeKey(graph, part)) != null) {
                throw new ClusterException("BUSY", "当前分片正在变更副本，请稍后重试导入发布");
            }
        } catch (RuntimeException ex) { endPublish(id); throw ex; }
    }

    void endPublish(RaftGroupId id) {
        publishing.computeIfPresent(id, (ignored, count) -> count == 1 ? null : count - 1);
    }

    void expireManifests() {
        long now = System.nanoTime();
        manifests.forEach((id, job) -> {
            if (job.creation.isDone() && now - job.touched > Duration.ofMinutes(5).toNanos()
                    && manifests.remove(id, job)) job.close();
        });
    }

    void begin(ReplicaBootstrapRequest request, RaftGroup group) throws IOException {
        var spec = request.getGroup();
        var task = task(spec.getGraphId(), spec.getPartId(), spec.getControlTaskId());
        boolean target = task.getAfter().getMembersList().stream().anyMatch(n -> n.getNodeId().equals(config.nodeId()))
                && task.getBefore().getMembersList().stream().noneMatch(n -> n.getNodeId().equals(config.nodeId()));
        if (!target || !spec.getMembersList().equals(task.getBefore().getMembersList())
                || !task.getBefore().getMembersList().contains(request.getSource())) {
            throw new ClusterException("STALE_CONTROLLER", "副本引导来源或目标与任务不一致");
        }
        if (ready(spec)) { copies.remove(group.getGroupId()); return; }
        var id = group.getGroupId();
        if (!task.getStage().equals("CREATED")) throw new ClusterException("STALE_CONTROLLER", "副本任务已结束引导阶段");
        var next = new CompletableFuture<Void>();
        var current = copies.putIfAbsent(id, next);
        if (current == null) {
            try {
                executor.execute(() -> {
                    try { download(request, group); next.complete(null); }
                    catch (Exception ex) { next.completeExceptionally(ex); }
                });
            } catch (RuntimeException ex) { copies.remove(id, next); throw ex; }
        } else if (current.isDone()) {
            copies.remove(id, current);
            try { current.join(); }
            catch (java.util.concurrent.CompletionException ex) {
                if (ex.getCause() instanceof ClusterException retry && retry.code().equals("BUSY")) throw retry;
                throw new ClusterException("BOOTSTRAP_FAILED", "副本 checkpoint 引导失败：" + ex.getCause().getMessage());
            }
            if (ready(spec)) return;
        }
        throw new ClusterException("BUSY", "正在安装分片 checkpoint");
    }

    boolean ready(DataGroupSpec spec) throws IOException {
        var id = id(spec.getGraphId(), spec.getPartId());
        Path receipt = receipt(id);
        if (!node.hosts(id) || !Files.isRegularFile(receipt)) return false;
        String[] fields = Files.readString(receipt).strip().split(" ");
        if (fields.length != 2 || !fields[0].equals(spec.getControlTaskId())) return false;
        var division = node.server().getDivision(id);
        return division.getStateMachine() instanceof RocksStateMachine machine && !machine.isUntrusted()
                && machine.getLastAppliedTermIndex().getIndex() >= Long.parseLong(fields[1]);
    }

    ReplicaCheckpoint manifest(ReplicaCheckpointRequest request) throws IOException {
        var task = task(request.getGraphId(), request.getPartId(), request.getTaskId());
        if (!task.getStage().equals("CREATED")
                || task.getBefore().getMembersList().stream().noneMatch(n -> n.getNodeId().equals(config.nodeId()))) {
            throw new ClusterException("STALE_CONTROLLER", " checkpoint 来源不在当前引导任务中");
        }
        var id = id(request.getGraphId(), request.getPartId());
        var division = node.server().getDivision(id);
        if (!division.getInfo().isLeaderReady()
                || !(division.getStateMachine() instanceof RocksStateMachine machine) || machine.isUntrusted()) {
            throw new ClusterException("GROUP_UNAVAILABLE", "该节点没有可信的分片 Leader");
        }
        if (publishing.containsKey(id)) throw new ClusterException("BUSY", "正在等待已经提交的导入发布");
        var job = manifests.compute(id, (ignored, previous) -> {
            if (previous != null && previous.task.equals(request.getTaskId())) return previous;
            if (previous != null) previous.close();
            var creation = CompletableFuture.supplyAsync(() -> {
                    try {
                        var group = RaftGroup.valueOf(id, division.getRaftConf().getAllPeers());
                        long fence;
                        try (var client = node.client(group)) {
                            // 空批次只推进应用位置，确认在拓扑任务创建之前接受的发布已进入快照。
                            var barrier = StorageGateway.batch("bootstrap.barrier/" + java.util.UUID.randomUUID()).build();
                            var reply = client.io().send(RocksStateMachine.message(barrier.toByteArray()));
                            if (!reply.isSuccess() || reply.getLogIndex() <= 0 || !division.getInfo().isLeaderReady()) {
                                throw new IOException("checkpoint 来源已更换或未应用导入屏障");
                            }
                            fence = reply.getLogIndex();
                        }
                        return machine.pinCheckpoint(fence);
                    } catch (IOException ex) { throw new java.util.concurrent.CompletionException(ex); }
                }, executor);
            return new ManifestJob(request.getTaskId(), machine, creation);
        });
        job.touched = System.nanoTime();
        var creation = job.creation;
        if (!creation.isDone()) throw new ClusterException("BUSY", "正在封存源 checkpoint");
        SnapshotInfo snapshot;
        try { snapshot = creation.join(); }
        catch (java.util.concurrent.CompletionException ex) {
            manifests.remove(id, job);
            throw new IOException("无法封存源 checkpoint", ex.getCause());
        }
        var result = ReplicaCheckpoint.newBuilder().setTerm(snapshot.getTerm()).setIndex(snapshot.getIndex());
        for (var file : snapshot.getFiles()) {
            result.addFiles(ReplicaCheckpointFile.newBuilder().setName(file.getPath().getFileName().toString())
                    .setSize(file.getFileSize()).setDigest(file.getFileDigest().toString()));
        }
        return result.build();
    }

    ByteString chunk(ReplicaCheckpointRequest request) throws IOException {
        task(request.getGraphId(), request.getPartId(), request.getTaskId());
        var id = id(request.getGraphId(), request.getPartId());
        var job = manifests.get(id);
        var snapshot = job == null || !job.task.equals(request.getTaskId()) || job.creation.isCompletedExceptionally()
                ? null : job.creation.getNow(null);
        if (snapshot == null || snapshot.getIndex() != request.getIndex()) {
            throw new ClusterException("BOOTSTRAP_FAILED", "源 checkpoint 已轮换");
        }
        if (job.machine.isUntrusted()) throw new ClusterException("REPLICA_UNTRUSTED", "checkpoint 来源发生状态机应用错误");
        job.touched = System.nanoTime();
        var file = snapshot.getFiles().stream()
                .filter(entry -> entry.getPath().getFileName().toString().equals(request.getFile()))
                .findFirst().orElseThrow(() -> new IOException("checkpoint 文件不在清单中"));
        long offset = request.getOffset();
        if (offset < 0 || offset >= file.getFileSize()) throw new IOException("checkpoint 文件偏移无效");
        var bytes = new byte[(int) Math.min(CHUNK_SIZE, file.getFileSize() - offset)];
        try (var input = new RandomAccessFile(file.getPath().toFile(), "r")) {
            input.seek(offset);
            input.readFully(bytes);
        } catch (IOException ex) {
            if (manifests.remove(id, job)) job.close();
            throw ex;
        }
        return ByteString.copyFrom(bytes);
    }

    private void download(ReplicaBootstrapRequest request, RaftGroup group) throws IOException {
        var spec = request.getGroup();
        var id = group.getGroupId();
        var ask = ReplicaCheckpointRequest.newBuilder().setGraphId(spec.getGraphId()).setPartId(spec.getPartId())
                .setTaskId(spec.getControlTaskId());
        var manifest = ReplicaCheckpoint.parseFrom(fetch(request.getSource(), "data.bootstrapManifest", ask.build()));
        if (manifest.getIndex() <= 0 || manifest.getTerm() <= 0 || manifest.getFilesCount() == 0) {
            throw new IOException("checkpoint 清单为空或日志位置无效");
        }
        Files.createDirectories(staging);
        var temp = Files.createTempDirectory(staging, id.getUuid() + "-");
        try {
            var checkpoint = Files.createDirectory(temp.resolve("snap-" + manifest.getTerm() + "_" + manifest.getIndex()));
            var names = new HashSet<String>();
            for (var file : manifest.getFilesList()) {
                String name = file.getName();
                if (!name.matches("[A-Za-z0-9][A-Za-z0-9._-]*") || !names.add(name)
                        || file.getSize() < 0 || !file.getDigest().matches("[0-9a-fA-F]{32}")) {
                    throw new IOException("checkpoint 文件清单无效");
                }
                var digest = md5();
                try (var channel = FileChannel.open(checkpoint.resolve(name), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                     var stream = Channels.newOutputStream(channel)) {
                    for (long offset = 0; offset < file.getSize();) {
                        if (Thread.currentThread().isInterrupted()) throw new IOException("checkpoint 下载已取消");
                        ByteString segment = fetch(request.getSource(), "data.bootstrapChunk", ask.setFile(name)
                                .setIndex(manifest.getIndex()).setOffset(offset).build());
                        if (segment.size() != Math.min(CHUNK_SIZE, file.getSize() - offset)) {
                            throw new EOFException("checkpoint 片段截断");
                        }
                        segment.writeTo(stream);
                        digest.update(segment.asReadOnlyByteBuffer());
                        offset += segment.size();
                    }
                    channel.force(true);
                }
                if (!HexFormat.of().formatHex(digest.digest()).equalsIgnoreCase(file.getDigest())) {
                    throw new IOException("checkpoint 文件校验失败：" + name);
                }
            }
            if (!names.contains("CURRENT")) throw new IOException("checkpoint 缺少 CURRENT");
            synchronized (node) {
                if (!task(spec.getGraphId(), spec.getPartId(), spec.getControlTaskId()).getStage().equals("CREATED")) {
                    throw new ClusterException("STALE_CONTROLLER", "副本任务已退出引导阶段");
                }
                // 目标此前不属于分片。旧任务的孤立副本先卸下，再安装这次校验过的快照。
                if (node.hosts(id) || node.dataDirectoryPresent(id)) node.detachDataGroup(id);
                var record = temp.resolve("receipt");
                Files.writeString(record, spec.getControlTaskId() + " " + manifest.getIndex(),
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC);
                Files.move(record, receipt(id), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                node.addGroupFromCheckpoint(group, checkpoint);
            }
        } finally {
            removeDownload(temp);
        }
    }

    private static void removeDownload(Path temp) throws IOException {
        try (var children = Files.walk(temp)) {
            for (var entry : children.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(entry);
        }
    }

    private ByteString fetch(Node source, String method, ReplicaCheckpointRequest request) {
        var answer = gateway.direct(DistributedCatalog.address(source), method, request.toByteString(), RPC_TIMEOUT);
        if (!answer.getCode().equals("OK")) throw new ClusterException(answer.getCode(), answer.getMessage());
        return answer.getBody();
    }

    private ReplicaTask task(long graph, int part, String taskId) throws IOException {
        if (taskId.isBlank()) throw new ClusterException("STALE_CONTROLLER", "分片引导需要副本任务");
        var active = gateway.get(0, 0, activeKey(graph, part));
        if (active == null || !taskId.equals(active.toStringUtf8())) throw new ClusterException("STALE_CONTROLLER", "分片引导任务已过期");
        var bytes = gateway.get(0, 0, TransactionEngine.text("tasks/replica/" + taskId));
        if (bytes == null) throw new ClusterException("STALE_CONTROLLER", "副本任务不存在");
        var task = ReplicaTask.parseFrom(bytes);
        if (task.getBefore().getGraphId() != graph || task.getBefore().getPartId() != part) {
            throw new ClusterException("STALE_CONTROLLER", "副本任务与分片不一致");
        }
        return task;
    }

    static ByteString activeKey(long graph, int part) {
        return TransactionEngine.text("tasks/activeReplica/" + graph + "/" + part);
    }

    private Path receipt(RaftGroupId id) { return staging.resolve(id.getUuid() + ".receipt"); }
    private RaftGroupId id(long graph, int part) { return GroupIds.data(config.clusterId(), graph, part); }

    private static MessageDigest md5() {
        try { return MessageDigest.getInstance("MD5"); }
        catch (NoSuchAlgorithmException ex) { throw new AssertionError(ex); }
    }
}
