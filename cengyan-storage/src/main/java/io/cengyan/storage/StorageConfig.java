package io.cengyan.storage;

import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** 新集群独立配置，禁止把旧单机 data_dir 无意作为新状态机目录。 */
public record StorageConfig(String clusterId, String nodeId, InetSocketAddress rpcAddress,
                            InetSocketAddress raftAddress, Path directory, List<RaftPeer> metadataMembers,
                            RaftSettings raftSettings, int rpcMaxPayload, int rpcMaxInflight, int admissionThreads, int admissionQueue,
                            int writeThreads) {
    public StorageConfig(String clusterId, String nodeId, InetSocketAddress rpcAddress,
                         InetSocketAddress raftAddress, Path directory, List<RaftPeer> metadataMembers,
                         RaftSettings raftSettings, int rpcMaxPayload, int rpcMaxInflight, int admissionThreads, int admissionQueue) {
        this(clusterId, nodeId, rpcAddress, raftAddress, directory, metadataMembers, raftSettings,
                rpcMaxPayload, rpcMaxInflight, admissionThreads, admissionQueue, 10);
    }
    public StorageConfig {
        if (clusterId == null || clusterId.isBlank() || nodeId == null || !nodeId.matches("[A-Za-z0-9_-]+")
                || directory == null || raftSettings == null || rpcAddress == null || raftAddress == null
                || rpcAddress.getPort() == raftAddress.getPort() || rpcAddress.getPort() < 1 || raftAddress.getPort() < 1
                || rpcMaxPayload < 1024 || rpcMaxInflight < 1 || admissionThreads < 1 || admissionQueue < 1 || writeThreads < 1) {
            throw new IllegalArgumentException("Storage 节点配置无效");
        }
        metadataMembers = List.copyOf(metadataMembers);
        if (metadataMembers.isEmpty()) throw new IllegalArgumentException("Metadata Bootstrap 成员不能为空");
        Set<String> ids = new HashSet<>(), addresses = new HashSet<>();
        for (var member : metadataMembers) {
            endpoint(member.getAddress());
            endpoint(member.getClientAddress());
            if (!ids.add(member.getId().toString()) || !addresses.add(member.getAddress())) {
                throw new IllegalArgumentException("Metadata Bootstrap 成员重复");
            }
        }
        var local = metadataMembers.stream().filter(peer -> peer.getId().toString().equals(nodeId)).findFirst().orElse(null);
        if (local != null && (!endpoint(local.getAddress()).equals(raftAddress) || !endpoint(local.getClientAddress()).equals(rpcAddress))) {
            throw new IllegalArgumentException("本节点地址与 Bootstrap 不一致");
        }
        directory = directory.toAbsolutePath().normalize();
    }

    public RaftGroup metadataGroup() {
        var members = new ArrayList<>(metadataMembers);
        if (members.stream().noneMatch(peer -> peer.getId().toString().equals(nodeId))) {
            members.add(RaftPeer.newBuilder().setId(nodeId).setAddress(raftAddress).setClientAddress(rpcAddress)
                    .setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build());
        }
        return RaftGroup.valueOf(GroupIds.metadata(clusterId), members);
    }
    public long recoveryTimeoutMs() { return operationalNumber("replica_recovery_timeout_ms", 1800000); }
    public long idempotencyWindowMs() { return operationalNumber("idempotency_window_ms", io.cengyan.distributed.StorageGateway.DEFAULT_IDEMPOTENCY_WINDOW_MS); }
    public long backgroundIntervalMs() { return operationalNumber("background_interval_ms", 10000); }
    public long backgroundBytesPerSecond() { return operationalNumber("background_bytes_per_second", 64L * 1024 * 1024); }
    /** 已提交历史版本在分片库里的保留期；到期后压缩成基线以回收空间。 */
    public long versionRetentionMs() { return operationalNumber("version_retention_ms", io.cengyan.distributed.VersionMaintenance.DEFAULT_RETENTION_MS); }
    public String zone() { return operationalText("zone"); }
    public String rack() { return operationalText("rack"); }
    /** @return true 表示配置仍写着已删除的 rebuild 模式，启动必须失败 */
    public boolean rebuildData() { return operationalText("data_storage_mode").equals("rebuild"); }
    public long readProtectionMs() {
        return operationalNumber("version_read_protection_ms", io.cengyan.distributed.VersionMaintenance.DEFAULT_READ_PROTECTION_MS);
    }
    private String operationalText(String key) {
        Path file = directory.resolve("operations.properties");
        if (!Files.exists(file)) return "";
        Properties values = new Properties();
        try (var reader = Files.newBufferedReader(file)) { values.load(reader); return values.getProperty(key, "").trim(); }
        catch (IOException ex) { throw new IllegalArgumentException("节点故障域配置不可读取", ex); }
    }
    private long operationalNumber(String key, long fallback) {
        Path file = directory.resolve("operations.properties");
        if (!Files.exists(file)) return fallback;
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(file)) {
            properties.load(reader);
            long value = number(properties, key, fallback);
            if (value < 1) throw new IllegalArgumentException("后台任务参数必须为正数：" + key);
            return value;
        } catch (IOException ex) { throw new IllegalArgumentException("后台任务配置无法读取", ex); }
    }

    public static StorageConfig load(Path file) throws IOException {
        Properties p = new Properties();
        try (var input = Files.newBufferedReader(file, java.nio.charset.StandardCharsets.UTF_8)) { p.load(input); }
        List<RaftPeer> members = new ArrayList<>();
        for (String entry : required(p, "metadata_members").split(",")) {
            String[] fields = entry.trim().split("@", -1);
            if (fields.length != 3 || !fields[0].matches("[A-Za-z0-9_-]+")) {
                throw new IllegalArgumentException("成员格式应为 nodeId@raftHost:port@rpcHost:port");
            }
            endpoint(fields[1]);
            endpoint(fields[2]);
            members.add(RaftPeer.newBuilder().setId(fields[0]).setAddress(fields[1]).setClientAddress(fields[2]).build());
        }
        Path directory = Path.of(required(p, "data_dir"));
        if (!directory.isAbsolute()) directory = file.toAbsolutePath().getParent().resolve(directory);
        StorageConfig config = new StorageConfig(required(p, "cluster_id"), required(p, "node_id"),
                endpoint(required(p, "rpc_address")), endpoint(required(p, "raft_address")), directory, members,
                new RaftSettings(number(p, "raft_log_segment_bytes", 16L * 1024 * 1024),
                        number(p, "raft_snapshot_entries", 2_000), number(p, "raft_log_retained_entries", 1_000),
                        Math.toIntExact(number(p, "raft_snapshot_retained", 2))),
                Math.toIntExact(number(p, "rpc_max_payload_bytes", 64L * 1024 * 1024)),
                Math.toIntExact(number(p, "rpc_max_inflight", 128)),
                Math.toIntExact(number(p, "admission_threads", 2)), Math.toIntExact(number(p, "admission_queue", 256)),
                Math.toIntExact(number(p, "write_threads", 10)));
        io.cengyan.distributed.TransactionEngine.setPlanMaxBytes(Math.toIntExact(number(p, "transaction_plan_max_bytes",
                io.cengyan.distributed.TransactionEngine.DEFAULT_PLAN_MAX_BYTES)));
        Properties operations = new Properties();
        String dataMode = p.getProperty("data_storage_mode", "raft").trim();
        if (dataMode.equals("rebuild") || dataMode.equals("replicated")) throw new IOException("rebuild 数据模式已移除。请改为 data_storage_mode=raft，清空数据目录后重新建图导入");
        if (!dataMode.equals("raft")) throw new IllegalArgumentException("data_storage_mode 必须为 raft");
        operations.setProperty("data_storage_mode", dataMode);
        for (String key : List.of("replica_recovery_timeout_ms", "background_interval_ms", "background_bytes_per_second",
                "idempotency_window_ms", "version_retention_ms", "version_read_protection_ms", "zone", "rack")) {
            if (p.containsKey(key)) operations.setProperty(key, p.getProperty(key));
        }
        if (!operations.isEmpty()) {
            if (Files.exists(config.directory.resolve("_system")) || Files.exists(config.directory.resolve("CURRENT"))) throw new IOException("不能使用旧单机数据目录");
            Files.createDirectories(config.directory);
            try (var output = Files.newBufferedWriter(config.directory.resolve("operations.properties"))) { operations.store(output, "Cengyan background limits"); }
        }
        return config;
    }

    private static String required(Properties p, String key) {
        String value = p.getProperty(key, "").trim();
        if (value.isEmpty()) throw new IllegalArgumentException("缺少配置：" + key);
        return value;
    }
    private static long number(Properties p, String key, long fallback) {
        return Long.parseLong(p.getProperty(key, Long.toString(fallback)).trim());
    }
    public static InetSocketAddress endpoint(String text) {
        try {
            URI uri = URI.create("tcp://" + Objects.requireNonNull(text));
            if (uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535 || uri.getRawUserInfo() != null
                    || !uri.getRawPath().isEmpty() || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IllegalArgumentException();
            }
            String host = uri.getHost();
            if (host.startsWith("[")) host = host.substring(1, host.length() - 1);
            return new InetSocketAddress(host, uri.getPort());
        } catch (RuntimeException ex) { throw new IllegalArgumentException("节点地址应为 host:port，IPv6 使用方括号", ex); }
    }
}

