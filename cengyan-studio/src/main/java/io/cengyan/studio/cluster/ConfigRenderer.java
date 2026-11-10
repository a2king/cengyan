package io.cengyan.studio.cluster;

import io.cengyan.studio.cluster.ClusterState.NodeSpec;
import io.cengyan.studio.cluster.ClusterState.Role;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** 把集群状态渲染成节点上的 properties（带中文注释）、node.env 和 systemd 单元。 */
public final class ConfigRenderer {
    /** @param hash 有效配置摘要，不含成员列表和 root 密码（它们变化不需要重启） */
    public record Rendered(String properties, String environment, String unit, String hash) { }

    /** 成员列表、种子地址只在节点启动时读取，变化不需要重启；root 密码只在集群首次初始化时使用。 */
    private static final Set<String> VOLATILE = Set.of("metadata_members", "compute_members", "storage_seeds", "root_password");
    private final ConfigCatalog catalog;

    public ConfigRenderer(ConfigCatalog catalog) { this.catalog = catalog; }

    public Rendered render(ClusterState state, NodeSpec node, String releaseId) {
        String host = host(state, node);
        Map<String, String> effective = catalog.effective(node.role(), state.config(), node.id());
        StringBuilder text = new StringBuilder();
        text.append("# 由层岩 Studio 生成。请在 Studio 网页上修改，手工修改会在下次下发时被覆盖。\n");
        text.append("# 节点 ").append(node.id()).append("（").append(node.role() == Role.STORAGE ? "存储" : "计算").append("），配置版本 v")
                .append(state.config().version()).append("\n\n");
        text.append("# ---- 集群拓扑（Studio 自动维护）----\n");
        line(text, "集群 ID", "cluster_id", state.clusterId());
        line(text, "节点 ID", "node_id", node.id());
        if (node.role() == Role.STORAGE) {
            line(text, "业务 RPC 地址", "rpc_address", host + ":" + node.rpcPort());
            line(text, "Raft 地址", "raft_address", host + ":" + node.raftPort());
            line(text, "数据目录", "data_dir", node.dataDir());
            line(text, "数据分片模式：重启后经维护模式比对或全量重建，Metadata 使用持久化 Raft", "data_storage_mode", "rebuild");
            line(text, "Metadata 成员，格式 nodeId@Raft地址@业务RPC地址", "metadata_members", storageMembers(state, node));
        } else {
            line(text, "客户端地址", "client_address", host + ":" + node.clientPort());
            line(text, "业务 RPC 地址", "rpc_address", host + ":" + node.rpcPort());
            line(text, "Raft 地址", "raft_address", host + ":" + node.raftPort());
            line(text, "数据目录（查询临时文件和控制面数据）", "data_dir", node.dataDir());
            line(text, "Compute 成员，格式 id@RPC地址@Raft地址@客户端地址", "compute_members", computeMembers(state, node));
            line(text, "Storage 业务 RPC 地址", "storage_seeds", storageSeeds(state));
            line(text, "数据库 root 初始密码，只在集群首次初始化时使用", "root_password", state.rootPassword());
        }
        text.append("\n# ---- 可调参数 ----\n");
        Map<String, String> overrides = state.config().nodeOverrides().getOrDefault(node.id(), Map.of());
        String jvm = "";
        for (ConfigCatalog.Entry entry : catalog.entries(node.role())) {
            String value = effective.getOrDefault(entry.key(), entry.defaultValue());
            if (entry.env()) { jvm = value; continue; }
            String note = entry.label() + "：" + entry.description() + "（默认 " + (entry.defaultValue().isEmpty() ? "空" : entry.defaultValue()) + "）"
                    + (overrides.containsKey(entry.key()) ? "【本节点单独设置】" : "");
            line(text, note, entry.key(), value);
        }
        String environment = "# 由层岩 Studio 生成\nCENGYAN_RELEASE=" + releaseId + "\nJAVA_OPTS=\"" + jvm + "\"\n";
        String unit = unit(node);
        return new Rendered(text.toString(), environment, unit, hash(text.toString(), environment, unit));
    }

    static String host(ClusterState state, NodeSpec node) {
        return state.server(node.serverId()).map(ClusterState.Server::host)
                .orElseThrow(() -> new ClusterError("NOT_FOUND", "节点 " + node.id() + " 所在服务器不存在"));
    }

    /** 已加入的节点渲染全部已加入成员；尚未加入的节点只写现有成员，启动后自动 Join。没有任何成员时自举。 */
    static String storageMembers(ClusterState state, NodeSpec node) {
        List<NodeSpec> members = members(state, node, Role.STORAGE);
        return String.join(",", members.stream().map(n -> n.id() + "@" + host(state, n) + ":" + n.raftPort() + "@" + host(state, n) + ":" + n.rpcPort()).toList());
    }

    static String computeMembers(ClusterState state, NodeSpec node) {
        List<NodeSpec> members = members(state, node, Role.COMPUTE);
        return String.join(",", members.stream().map(n -> {
            String h = host(state, n);
            return n.id() + "@" + h + ":" + n.rpcPort() + "@" + h + ":" + n.raftPort() + "@" + h + ":" + n.clientPort();
        }).toList());
    }

    static String storageSeeds(ClusterState state) {
        return String.join(",", state.nodes(Role.STORAGE).stream().filter(NodeSpec::joined).map(n -> host(state, n) + ":" + n.rpcPort()).toList());
    }

    private static List<NodeSpec> members(ClusterState state, NodeSpec node, Role role) {
        List<NodeSpec> joined = new ArrayList<>(state.nodes(role).stream().filter(n -> n.joined() && !n.id().equals(node.id())).toList());
        if (node.joined() || joined.isEmpty()) joined.add(node);
        joined.sort(Comparator.comparing(NodeSpec::id));
        return joined;
    }

    static String unit(NodeSpec node) {
        String dir = Remote.nodeDir(node.id());
        String role = node.role().name().toLowerCase(Locale.ROOT);
        String release = Remote.RELEASES + "/${CENGYAN_RELEASE}";
        return "# 由层岩 Studio 生成\n[Unit]\nDescription=Cengyan " + role + " " + node.id() + "\nAfter=network-online.target\nWants=network-online.target\n\n"
                + "[Service]\nType=simple\nUMask=0077\nEnvironmentFile=" + dir + "/node.env\nWorkingDirectory=" + dir + "\n"
                + "ExecStart=" + Remote.JDK + "/bin/java $JAVA_OPTS -Dlogback.configurationFile=" + release + "/logback.xml -Djava.io.tmpdir=" + dir + "/tmp"
                + " -jar " + release + "/cengyan-" + role + ".jar " + Remote.propertiesFile(node) + "\n"
                + "Restart=on-failure\nRestartSec=10\nTimeoutStopSec=120\nLimitNOFILE=1048576\n\n[Install]\nWantedBy=multi-user.target\n";
    }

    private static void line(StringBuilder text, String comment, String key, String value) {
        text.append("# ").append(comment.replace("\n", " ")).append("\n").append(key).append("=").append(escape(value)).append("\n");
    }

    /** properties 格式里反斜杠和行首空白需要转义；值本身已经过校验，不含换行。 */
    private static String escape(String value) {
        String escaped = value.replace("\\", "\\\\");
        return escaped.startsWith(" ") ? "\\" + escaped : escaped;
    }

    private static String hash(String properties, String environment, String unit) {
        var digest = Bundle.sha();
        for (String line : properties.split("\n")) {
            if (line.startsWith("#") || line.isBlank()) continue;
            String key = line.substring(0, Math.max(0, line.indexOf('=')));
            if (!VOLATILE.contains(key)) digest.update((line + "\n").getBytes(StandardCharsets.UTF_8));
        }
        digest.update(environment.getBytes(StandardCharsets.UTF_8));
        digest.update(unit.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest()).substring(0, 16);
    }
}
