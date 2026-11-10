package io.cengyan.studio.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Studio 管理的唯一集群的全部持久化状态。不可变，修改时生成新对象并整体落盘。
 *
 * @param clusterId    写入所有节点配置的 cluster_id，首次初始化生成后不再改变
 * @param rootPassword 数据库 root 密码，用于渲染 Compute 配置和以 root 调用管理命令
 */
public record ClusterState(String clusterId, String rootPassword, List<Server> servers, List<NodeSpec> nodes, ConfigVersion config) {
    public ClusterState {
        servers = List.copyOf(servers);
        nodes = List.copyOf(nodes);
    }

    public enum ServerStatus { PROVISIONING, READY, FAILED }
    public enum NodeStatus { INSTALLING, RUNNING, FAILED, REMOVING }
    public enum Role { STORAGE, COMPUTE }

    /**
     * 一台受管服务器。
     * @param hostKey   首次连接记录的主机公钥指纹（SHA256:...），之后每次连接都校验
     * @param releaseId 服务器上已同步的程序包版本
     */
    public record Server(String id, String host, int sshPort, String note, ServerStatus status, String message,
                         String hostKey, String arch, String os, String hostname, String jdkSha, String releaseId) {
        public Server with(ServerStatus nextStatus, String nextMessage) {
            return new Server(id, host, sshPort, note, nextStatus, nextMessage, hostKey, arch, os, hostname, jdkSha, releaseId);
        }
        public Server withProbe(String nextHostKey, String nextArch, String nextOs, String nextHostname) {
            return new Server(id, host, sshPort, note, status, message, nextHostKey, nextArch, nextOs, nextHostname, jdkSha, releaseId);
        }
        public Server withJdk(String sha) {
            return new Server(id, host, sshPort, note, status, message, hostKey, arch, os, hostname, sha, releaseId);
        }
        public Server withRelease(String nextRelease) {
            return new Server(id, host, sshPort, note, status, message, hostKey, arch, os, hostname, jdkSha, nextRelease);
        }
    }

    /**
     * 一个 Storage 或 Compute 节点。端口和数据目录在添加时确定，之后不能修改。
     * @param joined       节点曾经成功加入集群；之后渲染成员列表时总包含它自己
     * @param releaseId    节点 node.env 中的程序包版本
     * @param deployedHash 上次下发的有效配置摘要，与当前渲染结果不同即为"待重启"
     */
    public record NodeSpec(String id, Role role, String serverId, int raftPort, int rpcPort, int clientPort, String dataDir,
                           NodeStatus status, String message, boolean joined, String releaseId, String deployedHash) {
        public NodeSpec with(NodeStatus nextStatus, String nextMessage) {
            return new NodeSpec(id, role, serverId, raftPort, rpcPort, clientPort, dataDir, nextStatus, nextMessage, joined, releaseId, deployedHash);
        }
        public NodeSpec joinedAs(boolean value) {
            return new NodeSpec(id, role, serverId, raftPort, rpcPort, clientPort, dataDir, status, message, value, releaseId, deployedHash);
        }
        public NodeSpec deployed(String release, String hash) {
            return new NodeSpec(id, role, serverId, raftPort, rpcPort, clientPort, dataDir, status, message, joined, release, hash);
        }
        public List<Integer> ports() {
            return role == Role.STORAGE ? List.of(raftPort, rpcPort) : List.of(raftPort, rpcPort, clientPort);
        }
    }

    /**
     * 一个已保存的配置版本。值都以字符串保存，按参数目录校验。
     * @param roleDefaults  角色 → (键 → 值)，只保存与目录默认值不同的项
     * @param nodeOverrides 节点 ID → (键 → 值)
     */
    public record ConfigVersion(int version, long savedAt, String note,
                                Map<Role, Map<String, String>> roleDefaults, Map<String, Map<String, String>> nodeOverrides) {
        public ConfigVersion {
            roleDefaults = copy(roleDefaults);
            nodeOverrides = copy(nodeOverrides);
        }
        public static ConfigVersion initial() {
            return new ConfigVersion(0, 0, "初始配置", Map.of(), Map.of());
        }
        private static <K> Map<K, Map<String, String>> copy(Map<K, Map<String, String>> source) {
            Map<K, Map<String, String>> result = new LinkedHashMap<>();
            if (source != null) source.forEach((key, values) -> result.put(key, Map.copyOf(values == null ? Map.of() : values)));
            return Map.copyOf(result);
        }
    }

    public static ClusterState empty(String clusterId) {
        return new ClusterState(clusterId, "", List.of(), List.of(), ConfigVersion.initial());
    }

    public Optional<Server> server(String id) { return servers.stream().filter(s -> s.id().equals(id)).findFirst(); }
    public Optional<NodeSpec> node(String id) { return nodes.stream().filter(n -> n.id().equals(id)).findFirst(); }
    public List<NodeSpec> nodes(Role role) { return nodes.stream().filter(n -> n.role() == role).toList(); }

    public ClusterState withServer(Server server) {
        List<Server> next = new ArrayList<>(servers);
        next.removeIf(s -> s.id().equals(server.id()));
        next.add(server);
        next.sort(java.util.Comparator.comparing(Server::id));
        return new ClusterState(clusterId, rootPassword, next, nodes, config);
    }
    public ClusterState withoutServer(String id) {
        return new ClusterState(clusterId, rootPassword, servers.stream().filter(s -> !s.id().equals(id)).toList(), nodes, config);
    }
    public ClusterState withNode(NodeSpec node) {
        List<NodeSpec> next = new ArrayList<>(nodes);
        next.removeIf(n -> n.id().equals(node.id()));
        next.add(node);
        next.sort(java.util.Comparator.comparing(NodeSpec::id));
        return new ClusterState(clusterId, rootPassword, servers, next, config);
    }
    public ClusterState withoutNode(String id) {
        return new ClusterState(clusterId, rootPassword, servers, nodes.stream().filter(n -> !n.id().equals(id)).toList(), config);
    }
    public ClusterState withConfig(ConfigVersion next) {
        return new ClusterState(clusterId, rootPassword, servers, nodes, next);
    }
    public ClusterState withRootPassword(String password) {
        return new ClusterState(clusterId, password, servers, nodes, config);
    }
}
