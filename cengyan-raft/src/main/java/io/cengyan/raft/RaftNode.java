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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** 一台机器的 Multi-Raft 宿主，不打开其它节点的数据目录。 */
public final class RaftNode implements AutoCloseable {
    private final String clusterId;
    private final Path storageRoot;
    private final RaftGroupId metadataGroupId;
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
        this.clusterId = clusterId;
        this.storageRoot = directory.toAbsolutePath().normalize();
        this.metadataGroupId = bootstrapGroup.getGroupId();
        if (clusterId == null || clusterId.isBlank() || nodeId == null || nodeId.isBlank()
                || host == null || host.isBlank() || port < 1 || port > 65535) {
            throw new IllegalArgumentException("Raft 节点身份或地址无效");
        }
        if (bootstrapGroup.getPeer(RaftPeerId.valueOf(nodeId)) == null) {
            throw new IllegalArgumentException("Bootstrap 成员不包含本节点");
        }
        Path root = storageRoot;
        verifyIdentity(root, clusterId, nodeId, bootstrapGroup.getGroupId());
        ownerChannel = FileChannel.open(root.resolve("storage-process.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            ownerLock = ownerChannel.tryLock();
            if (ownerLock == null) throw new IOException("Storage 数据目录已被其他进程使用");
        } catch (IOException | RuntimeException ex) { ownerChannel.close(); throw ex; }
        try {
        // 上次进程没清完的已删除 Group，先改名再启动，避免 Ratis 把它重新打开。
        relocatePending(root);
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
     * 卸下本机 Data Group，并把目录改名为 {@code raft/dropped-<uuid>}。
     * 改名只动目录项，不等待清空文件。调用方必须已经把图标记为 DROPPED，并确认 Schema 租约结束。
     * @return 等待后台删除的隔离目录；本机没有这份数据时为空
     */
    public synchronized Optional<Path> detachDataGroup(RaftGroupId groupId) throws IOException {
        if (groupId == null || groupId.equals(metadataGroupId) || groupId.equals(GroupIds.compute(clusterId))) {
            throw new IllegalArgumentException("拒绝删除 Metadata 或 Compute Group");
        }
        RocksStateMachine machine = machines.get(groupId);
        if (hosts(groupId)) {
            var division = server.getDivision(groupId);
            var group = RaftGroup.valueOf(groupId, division.getRaftConf().getAllPeers());
            try (var client = client(group)) {
                // 两个 false：关闭状态机并从 Server 摘除，不在这次调用里递归删文件。
                var reply = client.getGroupManagementApi(server.getId()).remove(groupId, false, false);
                if (!reply.isSuccess() && !String.valueOf(reply.getException()).contains("not found")) {
                    throw new IOException("卸载本地 Raft Group 失败：" + reply.getException());
                }
            }
        }
        if (!hosts(groupId)) {
            machines.remove(groupId);
            if (machine != null) {
                try { machine.close(); }
                catch (RuntimeException ignored) { /* Ratis 已经关闭过这份状态机。 */ }
            }
        }
        return quarantine(groupId);
    }

    public boolean hosts(RaftGroupId groupId) {
        return java.util.stream.StreamSupport.stream(server.getGroupIds().spliterator(), false).anyMatch(groupId::equals);
    }

    public boolean dataDirectoryPresent(RaftGroupId groupId) {
        return Files.exists(liveDirectory(groupId), LinkOption.NOFOLLOW_LINKS);
    }

    /** 已被改名、Ratis 不会在启动时重新打开的数据目录。 */
    public List<Path> quarantinedDirectories() throws IOException {
        Path raftRoot = raftRoot();
        if (!Files.isDirectory(raftRoot)) return List.of();
        List<Path> found = new ArrayList<>();
        try (var children = Files.list(raftRoot)) {
            for (Path child : children.toList()) {
                String name = child.getFileName().toString();
                if (!name.startsWith("dropped-") || Files.isSymbolicLink(child)) continue;
                Path normalized = child.toAbsolutePath().normalize();
                if (normalized.getParent().equals(raftRoot)) found.add(normalized);
            }
        }
        Path pending = storageRoot.resolve("drop-pending");
        if (Files.isDirectory(pending)) {
            try (var marks = Files.list(pending)) {
                for (Path mark : marks.toList()) {
                    if (Files.isSymbolicLink(mark)) continue;
                    String name = mark.getFileName().toString();
                    try { UUID.fromString(name); }
                    catch (IllegalArgumentException ex) { continue; }
                    Path live = raftRoot.resolve(name).normalize();
                    if (!raftRoot.equals(live.getParent()) || Files.isSymbolicLink(live) || !Files.isDirectory(live)) continue;
                    if (hosts(RaftGroupId.valueOf(UUID.fromString(name)))) continue;
                    found.add(live);
                }
            }
        }
        return found;
    }

    /** 按字节速率清空一个隔离目录。{@code stop} 为真时保留剩余文件，下次继续。 */
    public void deleteQuarantined(Path directory, long bytesPerSecond, BooleanSupplier stop) throws IOException, InterruptedException {
        if (bytesPerSecond < 1) throw new IllegalArgumentException("删除带宽必须为正数");
        Path raftRoot = raftRoot();
        Path target = directory.toAbsolutePath().normalize();
        if (target.getParent() == null || !target.getParent().equals(raftRoot)) throw new IOException("拒绝删除 Raft 根目录之外的路径");
        String name = target.getFileName().toString();
        if (!droppedName(name) && !pendingLive(name)) throw new IOException("拒绝删除非隔离目录");
        if (Files.isSymbolicLink(target)) throw new IOException("拒绝删除符号链接数据目录");
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            forgetPending(name);
            return;
        }
        deleteSlowly(target, bytesPerSecond, stop == null ? () -> false : stop, new DeletionPace());
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) forgetPending(name);
    }

    public RaftServer server() { return server; }

    private Optional<Path> quarantine(RaftGroupId groupId) throws IOException {
        Path raftRoot = raftRoot();
        Path live = liveDirectory(groupId);
        if (!live.startsWith(raftRoot) || !raftRoot.equals(live.getParent())) throw new IOException("拒绝隔离 Raft 根目录之外的路径");
        if (!Files.exists(live, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
        if (Files.isSymbolicLink(live)) throw new IOException("拒绝隔离符号链接数据目录");
        Path target = raftRoot.resolve("dropped-" + groupId.getUuid()).normalize();
        if (!raftRoot.equals(target.getParent())) throw new IOException("拒绝隔离 Raft 根目录之外的路径");
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            target = raftRoot.resolve("dropped-" + groupId.getUuid() + "-" + System.nanoTime()).normalize();
            if (!raftRoot.equals(target.getParent())) throw new IOException("拒绝隔离 Raft 根目录之外的路径");
        }
        // 同一个 raft 目录内改名只改目录项。Windows 在句柄还没放开时会拒绝改名，
        // 这时留下标记，由后台直接清空原目录；下次启动也会先把这个目录挪走。
        rememberPending(groupId.getUuid());
        try {
            Files.move(live, target);
        } catch (FileAlreadyExistsException ex) {
            target = raftRoot.resolve("dropped-" + groupId.getUuid() + "-" + System.nanoTime()).normalize();
            if (!raftRoot.equals(target.getParent())) throw new IOException("拒绝隔离 Raft 根目录之外的路径");
            try { Files.move(live, target); }
            catch (IOException again) { return Optional.of(live); }
        } catch (IOException ex) {
            if (!Files.exists(live, LinkOption.NOFOLLOW_LINKS)) {
                forgetPending(groupId.getUuid().toString());
                return Optional.empty();
            }
            return Optional.of(live);
        }
        forgetPending(groupId.getUuid().toString());
        return Optional.of(target);
    }

    private void rememberPending(UUID uuid) throws IOException {
        Path dir = storageRoot.resolve("drop-pending");
        Files.createDirectories(dir);
        Path mark = dir.resolve(uuid.toString()).normalize();
        if (!dir.toAbsolutePath().normalize().equals(mark.getParent())) throw new IOException("拒绝写出隔离标记");
        if (!Files.exists(mark, LinkOption.NOFOLLOW_LINKS)) Files.write(mark, new byte[0], StandardOpenOption.CREATE_NEW);
    }

    private void forgetPending(String directoryName) {
        String body = directoryName.startsWith("dropped-") ? directoryName.substring("dropped-".length()) : directoryName;
        if (body.length() < 36) return;
        String id = body.substring(0, 36);
        try { UUID.fromString(id); }
        catch (IllegalArgumentException ex) { return; }
        try { Files.deleteIfExists(storageRoot.resolve("drop-pending").resolve(id)); }
        catch (IOException ignored) { /* 标记留着，下次启动还会再处理这个目录。 */ }
    }

    private boolean pendingLive(String name) {
        UUID id;
        try { id = UUID.fromString(name); }
        catch (IllegalArgumentException ex) { return false; }
        Path mark = storageRoot.resolve("drop-pending").resolve(id.toString());
        return Files.exists(mark, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(mark) && !hosts(RaftGroupId.valueOf(id));
    }

    private static boolean droppedName(String name) { return name.startsWith("dropped-"); }

    private static void relocatePending(Path root) throws IOException {
        Path pending = root.resolve("drop-pending");
        if (!Files.isDirectory(pending)) return;
        Path raft = root.resolve("raft");
        try (var marks = Files.list(pending)) {
            for (Path mark : marks.toList()) {
                if (Files.isSymbolicLink(mark)) continue;
                String name = mark.getFileName().toString();
                try { UUID.fromString(name); }
                catch (IllegalArgumentException ex) { continue; }
                Path live = raft.resolve(name);
                if (!Files.exists(live, LinkOption.NOFOLLOW_LINKS)) {
                    Files.deleteIfExists(mark);
                    continue;
                }
                if (Files.isSymbolicLink(live)) throw new IOException("拒绝隔离符号链接数据目录");
                Path target = raft.resolve("dropped-" + name);
                if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) target = raft.resolve("dropped-" + name + "-" + System.nanoTime());
                try {
                    Files.move(live, target);
                    Files.deleteIfExists(mark);
                } catch (IOException ignored) { /* 后台会按文件继续清空，标记保留到目录消失。 */ }
            }
        }
    }

    private Path liveDirectory(RaftGroupId groupId) {
        return raftRoot().resolve(groupId.getUuid().toString()).normalize();
    }

    private Path raftRoot() { return storageRoot.resolve("raft").toAbsolutePath().normalize(); }

    private static void deleteSlowly(Path path, long bytesPerSecond, BooleanSupplier stop, DeletionPace pace) throws IOException, InterruptedException {
        if (stop.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new InterruptedException();
        if (Files.isSymbolicLink(path)) {
            Files.deleteIfExists(path);
            return;
        }
        if (Files.isDirectory(path)) {
            List<Path> children;
            try (var listed = Files.list(path)) { children = listed.toList(); }
            for (Path child : children) deleteSlowly(child, bytesPerSecond, stop, pace);
            if (stop.getAsBoolean()) throw new InterruptedException();
            Files.deleteIfExists(path);
            return;
        }
        long size = 0;
        try { size = Files.size(path); } catch (IOException ignored) { /* 文件在删除前消失时按 0 字节计。 */ }
        Files.deleteIfExists(path);
        pace.account(size, bytesPerSecond);
    }

    /** 把一秒钟内的删除量限制在配置带宽内。单个大文件无法拆开，超限后下一秒再继续。 */
    private static final class DeletionPace {
        private long windowStart = System.nanoTime();
        private long windowBytes;
        void account(long size, long bytesPerSecond) throws InterruptedException {
            if (size <= 0) return;
            windowBytes += size;
            long elapsed = System.nanoTime() - windowStart;
            if (windowBytes < bytesPerSecond && elapsed < 1_000_000_000L) return;
            long sleep = 1_000_000_000L - elapsed;
            if (windowBytes >= bytesPerSecond && sleep > 0) TimeUnit.NANOSECONDS.sleep(sleep);
            windowStart = System.nanoTime();
            windowBytes = 0;
        }
    }

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
