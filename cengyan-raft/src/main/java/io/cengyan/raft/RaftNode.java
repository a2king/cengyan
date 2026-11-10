package io.cengyan.raft;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.TimeDuration;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** 一台机器的 Multi-Raft 宿主，不打开其它节点的数据目录。 */
public final class RaftNode implements AutoCloseable {
    private final RaftServer server;
    private final RaftProperties properties;
    private final FileChannel ownerChannel;
    private final java.nio.channels.FileLock ownerLock;
    private final ConcurrentHashMap<RaftGroupId, RocksStateMachine> machines = new ConcurrentHashMap<>();

    public RaftNode(String clusterId, String nodeId, String host, int port, Path directory,
                    RaftGroup bootstrapGroup, RaftSettings settings) throws IOException {
        this(clusterId, nodeId, host, port, directory, bootstrapGroup, settings, false);
    }
    public RaftNode(String clusterId, String nodeId, String host, int port, Path directory,
                    RaftGroup bootstrapGroup, RaftSettings settings, boolean metadataOnly) throws IOException {
        if (clusterId == null || clusterId.isBlank() || nodeId == null || nodeId.isBlank()
                || host == null || host.isBlank() || port < 1 || port > 65535) {
            throw new IllegalArgumentException("Raft 节点身份或地址无效");
        }
        if (bootstrapGroup.getPeer(RaftPeerId.valueOf(nodeId)) == null) {
            throw new IllegalArgumentException("Bootstrap 成员不包含本节点");
        }
        Path root = directory.toAbsolutePath().normalize();
        verifyIdentity(root, clusterId, nodeId, bootstrapGroup.getGroupId());
        ownerChannel = FileChannel.open(root.resolve("storage-process.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            ownerLock = ownerChannel.tryLock();
            if (ownerLock == null) throw new IOException("Storage 数据目录已被其他进程使用");
        } catch (IOException | RuntimeException ex) { ownerChannel.close(); throw ex; }
        try {
        if (metadataOnly && Files.exists(root.resolve("raft"))) {
            // Storage owns this root. Metadata is retained; former Data groups are explicitly invalidated.
            try (var groups = Files.list(root.resolve("raft"))) {
                for (Path group : groups.toList()) {
                    String name = group.getFileName().toString();
                    if (name.equalsIgnoreCase(bootstrapGroup.getGroupId().getUuid().toString())) continue;
                    try { java.util.UUID.fromString(name); }
                    catch (IllegalArgumentException ignored) { continue; }
                    deleteDataDirectory(group);
                }
            }
        }
        properties = settings.properties(root.resolve("raft"), host, port);
        Properties operations = new Properties();
        if (Files.exists(root.resolve("operations.properties"))) {
            try (var input = Files.newInputStream(root.resolve("operations.properties"))) { operations.load(input); }
        }
        long snapshotRate = Long.parseLong(operations.getProperty("background_bytes_per_second", "67108864"));
        if (snapshotRate < 1) throw new IOException("快照带宽配置必须为正数");
        server = RaftServer.newBuilder().setServerId(RaftPeerId.valueOf(nodeId))
                .setGroup(bootstrapGroup).setProperties(properties).setOption(RaftStorage.StartupOption.RECOVER)
                .setStateMachineRegistry(id -> {
                    if (metadataOnly && !id.equals(bootstrapGroup.getGroupId())) throw new IllegalStateException("重建模式禁止打开 Data Raft Group");
                    return machines.computeIfAbsent(id, ignored -> new RocksStateMachine(snapshotRate));
                }).build();
        try { server.start(); }
        catch (IOException | RuntimeException ex) {
            try { server.close(); } catch (IOException cleanup) { ex.addSuppressed(cleanup); }
            throw ex;
        }
        } catch (IOException | RuntimeException ex) { ownerLock.release(); ownerChannel.close(); throw ex; }
    }

    private static void verifyIdentity(Path directory, String cluster, String node, RaftGroupId controlGroup) throws IOException {
        Files.createDirectories(directory);
        Path identity = directory.resolve("node.properties");
        if (Files.exists(identity)) {
            Properties stored = new Properties();
            try (var input = Files.newInputStream(identity)) { stored.load(input); }
            if (!cluster.equals(stored.getProperty("cluster_id")) || !node.equals(stored.getProperty("node_id"))
                    || !controlGroup.getUuid().toString().equals(stored.getProperty("control_group"))) {
                throw new IOException("数据目录属于其它集群、节点或控制 Group，拒绝启动");
            }
            return;
        }
        Properties data = new Properties();
        data.setProperty("cluster_id", cluster);
        data.setProperty("node_id", node);
        data.setProperty("control_group", controlGroup.getUuid().toString());
        Path temp = Files.createTempFile(directory, "identity-", ".tmp");
        try {
            try (var output = Files.newOutputStream(temp)) { data.store(output, "Cengyan Raft node identity v1"); }
            try (var channel = FileChannel.open(temp, StandardOpenOption.WRITE)) { channel.force(true); }
            Files.move(temp, identity, StandardCopyOption.ATOMIC_MOVE);
        } finally { Files.deleteIfExists(temp); }
    }

    /** 管理 API 必须由 Metadata 中的持久化任务调用，不能自行决定副本归属。 */
    public synchronized void addGroup(RaftGroup group) throws IOException {
        for (var existing : server.getGroupIds()) {
            if (existing.equals(group.getGroupId())) {
                // 同一任务重试允许幂等返回，但不能用 addGroup 偷换已有 Group 成员。
                var actual = server.getDivision(existing).getRaftConf().getAllPeers();
                if (!new java.util.HashSet<>(actual).equals(new java.util.HashSet<>(group.getPeers()))) {
                    throw new IllegalArgumentException("已存在 Group 的成员不同，必须使用 Raft 成员变更协议");
                }
                return;
            }
        }
        try (var client = client(group)) {
            var reply = client.getGroupManagementApi(server.getId()).add(group, true);
            if (!reply.isSuccess()) throw new IOException("创建 Data Raft Group 失败：" + reply.getException());
        }
    }

    public RaftClient client(RaftGroup group) {
        return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(30,
                        TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))).build();
    }

    /**
     * Remove a local Data Group and its Raft storage. The caller must already have
     * persisted the Metadata deletion decision and verified lease expiry.
     */
    public synchronized void removeGroup(RaftGroupId groupId) throws IOException {
        if (!java.util.stream.StreamSupport.stream(server.getGroupIds().spliterator(), false).anyMatch(groupId::equals)) return;
        var division = server.getDivision(groupId);
        var group = RaftGroup.valueOf(groupId, division.getRaftConf().getAllPeers());
        try (var client = client(group)) {
            var reply = client.getGroupManagementApi(server.getId()).remove(groupId, true, true);
            if (!reply.isSuccess() && !String.valueOf(reply.getException()).contains("not found")) {
                throw new IOException("删除本地 Raft Group 失败：" + reply.getException());
            }
        }
    }

    public RaftServer server() { return server; }
    private static void deleteDataDirectory(Path root) throws IOException {
        if (Files.isSymbolicLink(root)) throw new IOException("拒绝删除符号链接数据目录");
        try (var files = Files.walk(root)) {
            for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(file);
        }
    }
    @Override public void close() throws IOException {
        try { server.close(); } finally { if (ownerLock.isValid()) ownerLock.release(); ownerChannel.close(); }
    }
}
