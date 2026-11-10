package io.cengyan.compute;

import org.apache.ratis.protocol.RaftPeer;
import java.net.*;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;

public record ComputeConfig(String clusterId, String nodeId, InetSocketAddress client, InetSocketAddress rpc,
                            InetSocketAddress raft, Path directory, List<Peer> members, List<InetSocketAddress> storageSeeds,
                            int slots, int queueCapacity, long queueTimeoutMs, long executionTimeoutMs,
                            long queryMemory, long nodeMemory, long queryDisk, long nodeDisk,
                            int resultMaxRows, int resultMaxBytes, String rootPassword) {
    public record Peer(String id, InetSocketAddress rpc, InetSocketAddress raft, InetSocketAddress client) {
        public Peer {
            if (id == null || !id.matches("[A-Za-z0-9_-]+") || rpc == null || raft == null || client == null
                    || rpc.getPort() < 1 || raft.getPort() < 1 || client.getPort() < 1) throw new IllegalArgumentException("计算成员身份或地址无效");
        }
        public RaftPeer raftPeer() { return RaftPeer.newBuilder().setId(id).setAddress(raft).setClientAddress(rpc).setAdminAddress(client).build(); }
    }
    public ComputeConfig {
        if (clusterId == null || clusterId.isBlank() || nodeId == null || !nodeId.matches("[A-Za-z0-9_-]+") || directory == null
                || members == null || members.isEmpty() || storageSeeds == null || storageSeeds.isEmpty() || client == null || rpc == null || raft == null
                || rootPassword == null || rootPassword.isEmpty() || client.getPort() < 1 || rpc.getPort() < 1 || raft.getPort() < 1
                || slots < 1 || queueCapacity < 1 || queueTimeoutMs < 1 || executionTimeoutMs < 1 || queryMemory < 1 || nodeMemory < queryMemory
                || queryDisk < 1 || nodeDisk < queryDisk || resultMaxRows < 1 || resultMaxBytes < 1024 || resultMaxBytes > 64 * 1024 * 1024
                || executionTimeoutMs > 1800000
                || client.getPort() == rpc.getPort() || rpc.getPort() == raft.getPort() || client.getPort() == raft.getPort()) {
            throw new IllegalArgumentException("Compute 地址、容量或预算配置无效");
        }
        members = List.copyOf(members); storageSeeds = List.copyOf(storageSeeds);
        if (members.stream().anyMatch(p -> p.id.equals(nodeId) && (!p.rpc.equals(rpc) || !p.raft.equals(raft) || !p.client.equals(client)))) {
            throw new IllegalArgumentException("Compute Bootstrap 不包含当前节点实际地址");
        }
        if (members.stream().map(Peer::id).distinct().count() != members.size()) throw new IllegalArgumentException("Compute 成员重复");
        directory = directory.toAbsolutePath().normalize();
    }
    public static ComputeConfig load(Path file) throws IOException {
        Properties p = new Properties();
        try (var reader = Files.newBufferedReader(file)) { p.load(reader); }
        List<Peer> members = new ArrayList<>();
        for (String entry : required(p, "compute_members").split(",")) {
            String[] fields = entry.trim().split("@");
            if (fields.length != 4) throw new IllegalArgumentException("Compute 成员格式：id@rpc@raft@client");
            members.add(new Peer(fields[0], address(fields[1]), address(fields[2]), address(fields[3])));
        }
        List<InetSocketAddress> seeds = Arrays.stream(required(p, "storage_seeds").split(",")).map(String::trim).map(ComputeConfig::address).toList();
        Path directory = Path.of(required(p, "data_dir"));
        if (!directory.isAbsolute()) directory = file.toAbsolutePath().getParent().resolve(directory);
        // 写入批次自带的幂等有效期；与 Storage 端使用同一个配置键。
        io.cengyan.distributed.StorageGateway.setIdempotencyWindowMs(number(p, "idempotency_window_ms", 3600000));
        return new ComputeConfig(required(p, "cluster_id"), required(p, "node_id"), address(required(p, "client_address")),
                address(required(p, "rpc_address")), address(required(p, "raft_address")), directory, members, seeds,
                (int) number(p, "query_slots", 4), (int) number(p, "queue_capacity", 128), number(p, "queue_timeout_ms", 30000),
                number(p, "max_execution_time_ms", 1800000), number(p, "query_memory_bytes", 67108864), number(p, "node_memory_bytes", 536870912),
                number(p, "query_temp_bytes", 1073741824), number(p, "node_temp_bytes", 8589934592L),
                (int) number(p, "result_max_rows", 100000), (int) number(p, "result_max_bytes", 16777216), required(p, "root_password"));
    }
    public static InetSocketAddress address(String value) {
        URI uri = URI.create("tcp://" + value);
        if (uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535 || !uri.getRawPath().isEmpty() || uri.getUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) throw new IllegalArgumentException("节点地址无效");
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }
    private static String required(Properties p, String key) {
        String value = p.getProperty(key, "").trim(); if (value.isEmpty()) throw new IllegalArgumentException("缺少配置：" + key); return value;
    }
    private static long number(Properties p, String key, long fallback) { return Long.parseLong(p.getProperty(key, Long.toString(fallback))); }
}
