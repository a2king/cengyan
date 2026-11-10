package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.studio.cluster.ClusterState.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Studio 管理唯一集群的入口：校验请求、提交任务、汇总状态。耗时操作都在 {@link TaskRunner} 中执行。 */
public final class ClusterManager implements AutoCloseable {
    final ClusterStore store;
    final Bundle bundle;
    final ConfigCatalog catalog;
    final ConfigRenderer renderer;
    final ClusterOps ops;
    final RemoteShell.Factory shells;
    final TaskRunner tasks;
    /** 添加服务器时的 root 密码，只在内存中保存到免密登录验证通过为止，不写盘。 */
    final Map<String, String> passwords = new ConcurrentHashMap<>();

    public static ClusterManager open(Path home) throws IOException {
        Path data = home.resolve("data");
        ClusterStore store = new ClusterStore(data);
        return new ClusterManager(store, new Bundle(home), new ClusterOps(), new SshShell.Factory(store.dataDir()));
    }

    ClusterManager(ClusterStore store, Bundle bundle, ClusterOps ops, RemoteShell.Factory shells) throws IOException {
        this.store = store;
        this.bundle = bundle;
        this.catalog = ConfigCatalog.load();
        this.renderer = new ConfigRenderer(catalog);
        this.ops = ops;
        this.shells = shells;
        ClusterPlans plans = new ClusterPlans(this);
        this.tasks = new TaskRunner(store.dataDir(), plans::plan);
    }

    public ClusterState state() { return store.get(); }
    public ConfigCatalog catalog() { return catalog; }
    public TaskRunner tasks() { return tasks; }

    RemoteShell shell(String serverId) throws IOException {
        Server server = state().server(serverId).orElseThrow(() -> new ClusterError("NOT_FOUND", "服务器不存在：" + serverId));
        return shells.open(server.host(), server.sshPort(), null, server.hostKey());
    }

    // ---- servers ----
    public TaskRunner.TaskView addServer(String host, int sshPort, String password, String note) throws IOException {
        host = host == null ? "" : host.trim();
        if (!host.matches("[A-Za-z0-9.-]{1,253}")) throw new ClusterError("INVALID_ARGUMENT", "服务器地址只能是 IP 或主机名");
        if (sshPort < 1 || sshPort > 65535) throw new ClusterError("INVALID_ARGUMENT", "SSH 端口超出范围");
        if (password == null || password.isEmpty()) throw new ClusterError("INVALID_ARGUMENT", "请输入 root 密码");
        String finalHost = host;
        if (state().servers().stream().anyMatch(s -> s.host().equalsIgnoreCase(finalHost))) throw new ClusterError("CONFLICT", "服务器已存在：" + host);
        bundle.releaseId();
        String id = "server-" + (state().servers().stream().mapToInt(s -> number(s.id())).max().orElse(0) + 1);
        Server server = new Server(id, host, sshPort, note == null ? "" : note.trim(), ServerStatus.PROVISIONING, "正在添加",
                "", "", "", "", "", "");
        store.update(s -> s.withServer(server));
        passwords.put(id, password);
        try { return tasks.submit("server.add", Map.of("server", id)); }
        catch (RuntimeException ex) { store.update(s -> s.withoutServer(id)); passwords.remove(id); throw ex; }
    }

    public TaskRunner.TaskView removeServer(String id) throws IOException {
        state().server(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "服务器不存在：" + id));
        if (state().nodes().stream().anyMatch(n -> n.serverId().equals(id))) throw new ClusterError("NOT_EMPTY", "服务器上还有节点，请先移除节点");
        return tasks.submit("server.remove", Map.of("server", id));
    }

    /** 继续或处理暂停的任务。添加服务器时可以重新提供 root 密码。 */
    public TaskRunner.TaskView act(String taskId, String action, String password, boolean exitMaintenance) {
        TaskRunner.TaskView task = tasks.get(taskId);
        if (password != null && !password.isEmpty() && task.type().equals("server.add")) passwords.put(task.params().get("server"), password);
        return tasks.act(taskId, action, exitMaintenance ? Map.of("exitMaintenance", "true") : Map.of());
    }

    // ---- nodes ----
    public static final Map<Role, int[]> BASE_PORTS = Map.of(Role.STORAGE, new int[] {19700, 19701, 0}, Role.COMPUTE, new int[] {19800, 19801, 19669});

    /** 添加节点表单的预填值：下一个节点 ID、该服务器上不冲突的端口、默认数据目录。 */
    public ObjectNode suggest(Role role, String serverId) {
        ClusterState state = state();
        String prefix = role == Role.STORAGE ? "storage-" : "compute-";
        int next = state.nodes().stream().filter(n -> n.id().startsWith(prefix)).mapToInt(n -> number(n.id())).max().orElse(0) + 1;
        String id = prefix + next;
        Set<Integer> used = usedPorts(state, serverId, null);
        int[] base = BASE_PORTS.get(role);
        int offset = 0;
        while (conflicts(base, offset, used)) offset += 10;
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("id", id).put("raftPort", base[0] + offset).put("rpcPort", base[1] + offset)
                .put("clientPort", base[2] == 0 ? 0 : base[2] + offset).put("dataDir", Remote.DATA + "/" + id);
        return result;
    }

    private static boolean conflicts(int[] base, int offset, Set<Integer> used) {
        for (int port : base) if (port != 0 && used.contains(port + offset)) return true;
        return false;
    }

    private static Set<Integer> usedPorts(ClusterState state, String serverId, String exceptNode) {
        Set<Integer> used = new HashSet<>();
        state.nodes().stream().filter(n -> n.serverId().equals(serverId) && !n.id().equals(exceptNode)).forEach(n -> used.addAll(n.ports()));
        return used;
    }

    public TaskRunner.TaskView addNode(Role role, String serverId, String id, int raftPort, int rpcPort, int clientPort, String dataDir) throws IOException {
        ClusterState state = state();
        Server server = state.server(serverId).orElseThrow(() -> new ClusterError("NOT_FOUND", "服务器不存在：" + serverId));
        if (server.status() != ServerStatus.READY) throw new ClusterError("INVALID_STATE", "服务器尚未就绪");
        if (id == null || !id.matches("[a-z][a-z0-9_-]{0,31}")) throw new ClusterError("INVALID_ARGUMENT", "节点 ID 只能包含小写字母、数字、下划线和短横线，以字母开头，最长 32 位");
        if (state.node(id).isPresent()) throw new ClusterError("CONFLICT", "节点 ID 已存在：" + id);
        if (role == Role.STORAGE) clientPort = 0;
        List<Integer> ports = role == Role.STORAGE ? List.of(raftPort, rpcPort) : List.of(raftPort, rpcPort, clientPort);
        if (ports.stream().anyMatch(p -> p < 1024 || p > 65535)) throw new ClusterError("INVALID_ARGUMENT", "端口必须在 1024 到 65535 之间");
        if (new HashSet<>(ports).size() != ports.size()) throw new ClusterError("INVALID_ARGUMENT", "同一节点的端口不能重复");
        Set<Integer> used = usedPorts(state, serverId, null);
        for (int port : ports) if (used.contains(port)) throw new ClusterError("PORT_IN_USE", "该服务器上的其他节点已使用端口 " + port);
        if (!Remote.safeDataDir(dataDir)) throw new ClusterError("INVALID_ARGUMENT", "数据目录必须是至少两级的绝对路径，只能包含字母、数字、. _ - /，且不能是系统目录");
        if (state.nodes().stream().anyMatch(n -> n.serverId().equals(serverId) && (n.dataDir().equals(dataDir) || n.dataDir().startsWith(dataDir + "/") || dataDir.startsWith(n.dataDir() + "/"))))
            throw new ClusterError("CONFLICT", "数据目录与该服务器上的其他节点重叠");
        List<NodeSpec> sameRole = state.nodes(role);
        if (!sameRole.isEmpty() && sameRole.stream().noneMatch(NodeSpec::joined))
            throw new ClusterError("INVALID_STATE", "第一个" + (role == Role.STORAGE ? "存储" : "计算") + "节点尚未成功加入，请先处理或移除它");
        if (role == Role.COMPUTE && state.nodes(Role.STORAGE).stream().noneMatch(n -> n.joined() && n.status() == NodeStatus.RUNNING))
            throw new ClusterError("INVALID_STATE", "请先添加并启动至少一个存储节点");
        if (role == Role.COMPUTE && state.rootPassword().isEmpty()) throw new ClusterError("ROOT_PASSWORD_REQUIRED", "添加第一个计算节点前请先设置数据库 root 密码");
        NodeSpec node = new NodeSpec(id, role, serverId, raftPort, rpcPort, clientPort, dataDir.replaceAll("/+$", ""),
                NodeStatus.INSTALLING, "正在安装", false, "", "");
        store.update(s -> s.withNode(node));
        try { return tasks.submit("node.add", Map.of("node", id)); }
        catch (RuntimeException ex) { store.update(s -> s.withoutNode(id)); throw ex; }
    }

    public TaskRunner.TaskView removeNode(String id, boolean deleteData) throws IOException {
        ClusterState state = state();
        NodeSpec node = state.node(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "节点不存在：" + id));
        if (node.joined()) {
            List<NodeSpec> others = state.nodes(node.role()).stream().filter(n -> n.joined() && !n.id().equals(id)).toList();
            if (node.role() == Role.COMPUTE && others.isEmpty()) throw new ClusterError("LAST_NODE", "不能移除最后一个计算节点");
            if (node.role() == Role.STORAGE && others.isEmpty()) throw new ClusterError("LAST_NODE", "不能移除最后一个存储节点");
        }
        return tasks.submit("node.remove", Map.of("node", id, "deleteData", Boolean.toString(deleteData)));
    }

    /** nodeIds 为空时重启所有待重启的节点。Storage 在前，Compute 在后。 */
    public TaskRunner.TaskView restart(List<String> nodeIds) {
        ClusterState state = state();
        List<NodeSpec> targets;
        if (nodeIds == null || nodeIds.isEmpty()) targets = state.nodes().stream().filter(n -> n.joined() && pendingRestart(state, n)).toList();
        else {
            targets = new ArrayList<>();
            for (String id : nodeIds) {
                NodeSpec node = state.node(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "节点不存在：" + id));
                if (!node.joined()) throw new ClusterError("INVALID_STATE", "节点 " + id + " 尚未加入集群");
                targets.add(node);
            }
        }
        if (targets.isEmpty()) throw new ClusterError("NOTHING_TO_DO", "没有需要重启的节点");
        return tasks.submit("restart", Map.of("nodes", order(targets)));
    }

    /** 把所有节点升级到 Studio 随附的程序包版本。 */
    public TaskRunner.TaskView upgrade() throws IOException {
        ClusterState state = state();
        String release = bundle.releaseId();
        List<NodeSpec> targets = state.nodes().stream().filter(NodeSpec::joined).toList();
        if (targets.isEmpty()) throw new ClusterError("NOTHING_TO_DO", "还没有节点");
        if (targets.stream().allMatch(n -> n.releaseId().equals(release))) throw new ClusterError("NOTHING_TO_DO", "所有节点已是当前版本");
        Set<String> previous = new TreeSet<>();
        targets.forEach(n -> previous.add(n.releaseId()));
        return tasks.submit("upgrade", Map.of("nodes", order(targets), "release", release, "previous", String.join(",", previous)));
    }

    private static String order(List<NodeSpec> nodes) {
        List<NodeSpec> sorted = new ArrayList<>(nodes);
        sorted.sort(Comparator.comparing((NodeSpec n) -> n.role() == Role.STORAGE ? 0 : 1).thenComparing(NodeSpec::id));
        return String.join(",", sorted.stream().map(NodeSpec::id).toList());
    }

    boolean pendingRestart(ClusterState state, NodeSpec node) {
        if (!node.joined() || node.deployedHash().isEmpty()) return false;
        return !renderer.render(state, node, node.releaseId()).hash().equals(node.deployedHash());
    }

    // ---- config ----
    /** 整体替换可调参数，生成新版本。与目录默认值相同的项不保存。 */
    public ConfigVersion saveConfig(Map<Role, Map<String, String>> roleDefaults, Map<String, Map<String, String>> nodeOverrides, String note) throws IOException {
        ClusterState state = state();
        Map<String, String> errors = new LinkedHashMap<>();
        Map<Role, Map<String, String>> roles = new EnumMap<>(Role.class);
        for (Role role : Role.values()) {
            Map<String, String> values = trim(roleDefaults == null ? null : roleDefaults.get(role));
            catalog.validate(role, values).forEach((k, v) -> errors.put(role.name() + "." + k, v));
            Map<String, String> kept = new LinkedHashMap<>();
            values.forEach((k, v) -> catalog.entry(role, k).filter(e -> !e.defaultValue().equals(v)).ifPresent(e -> kept.put(k, v)));
            if (!kept.isEmpty()) roles.put(role, kept);
        }
        Map<String, Map<String, String>> nodes = new LinkedHashMap<>();
        if (nodeOverrides != null) nodeOverrides.forEach((nodeId, raw) -> {
            NodeSpec node = state.node(nodeId).orElse(null);
            if (node == null) { errors.put(nodeId, "节点不存在"); return; }
            Map<String, String> values = trim(raw);
            catalog.validate(node.role(), values).forEach((k, v) -> errors.put(nodeId + "." + k, v));
            if (!values.isEmpty()) nodes.put(nodeId, values);
        });
        ConfigVersion candidate = new ConfigVersion(state.config().version() + 1, System.currentTimeMillis(), note == null ? "" : note.trim(), roles, nodes);
        if (errors.isEmpty()) {
            for (Role role : Role.values()) catalog.crossCheck(role, catalog.effective(role, candidate, null)).forEach((k, v) -> errors.put(role.name() + "." + k, v));
            for (NodeSpec node : state.nodes()) catalog.crossCheck(node.role(), catalog.effective(node.role(), candidate, node.id())).forEach((k, v) -> errors.put(node.id() + "." + k, v));
        }
        if (!errors.isEmpty()) throw new ClusterError("INVALID_CONFIG", "配置校验未通过", errors);
        return store.update(s -> s.withConfig(candidate)).config();
    }

    public ConfigVersion rollback(int version) throws IOException {
        ConfigVersion old = store.version(version);
        Map<String, Map<String, String>> overrides = new LinkedHashMap<>(old.nodeOverrides());
        overrides.keySet().removeIf(id -> state().node(id).isEmpty());
        return saveConfig(old.roleDefaults(), overrides, "回滚到 v" + version);
    }

    public record Change(String scope, String key, String before, String after) { }

    public List<Change> diff(int from, int to) throws IOException {
        ConfigVersion a = store.version(from), b = store.version(to);
        List<Change> changes = new ArrayList<>();
        for (Role role : Role.values()) compare(role.name(), a.roleDefaults().getOrDefault(role, Map.of()), b.roleDefaults().getOrDefault(role, Map.of()), changes);
        Set<String> nodes = new TreeSet<>(a.nodeOverrides().keySet());
        nodes.addAll(b.nodeOverrides().keySet());
        for (String node : nodes) compare(node, a.nodeOverrides().getOrDefault(node, Map.of()), b.nodeOverrides().getOrDefault(node, Map.of()), changes);
        return changes;
    }

    private static void compare(String scope, Map<String, String> a, Map<String, String> b, List<Change> out) {
        Set<String> keys = new TreeSet<>(a.keySet());
        keys.addAll(b.keySet());
        for (String key : keys) if (!Objects.equals(a.get(key), b.get(key))) out.add(new Change(scope, key, a.getOrDefault(key, ""), b.getOrDefault(key, "")));
    }

    public List<ConfigVersion> history() throws IOException { return store.history(); }

    /** 集群已有计算节点时，先在数据库中修改 root 密码，再更新 Studio 保存的密码和计算节点配置文件。 */
    public void setRootPassword(String password) throws IOException {
        if (password == null || password.length() < 8 || password.chars().anyMatch(c -> c < 32 || c == 127))
            throw new ClusterError("INVALID_ARGUMENT", "root 密码至少 8 位，不能包含控制字符");
        if (tasks.active() != null) throw new ClusterError("BUSY", "有任务进行中，请稍后再改密码");
        ClusterState state = state();
        Optional<NodeSpec> via = state.nodes(Role.COMPUTE).stream().filter(n -> n.joined() && n.status() == NodeStatus.RUNNING).findFirst();
        if (via.isPresent()) ops.setRootPassword(state, via.get(), password);
        ClusterState next = store.update(s -> s.withRootPassword(password));
        for (NodeSpec node : next.nodes(Role.COMPUTE)) {
            if (!node.joined()) continue;
            try (RemoteShell shell = shell(node.serverId())) {
                shell.write(Remote.propertiesFile(node), renderer.render(next, node, node.releaseId()).properties(), "600");
            } catch (IOException | RuntimeException ignored) { /* 配置文件中的密码只在集群首次初始化时使用 */ }
        }
    }

    public String logs(String nodeId, int lines) throws IOException {
        NodeSpec node = state().node(nodeId).orElseThrow(() -> new ClusterError("NOT_FOUND", "节点不存在：" + nodeId));
        try (RemoteShell shell = shell(node.serverId())) { return Remote.logs(shell, nodeId, lines); }
    }

    /** 手动退出维护模式，例如滚动重启终止后。 */
    public void exitMaintenance() { ops.exitMaintenance(state()); }

    // ---- overview ----
    /** 页面轮询用：只读本地状态，不访问集群，返回很快。 */
    public ObjectNode overview() throws IOException {
        ClusterState state = state();
        String release;
        String bundleError = "";
        try { release = bundle.releaseId(); }
        catch (ClusterError ex) { release = ""; bundleError = ex.getMessage(); }
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK");
        result.put("clusterId", state.clusterId()).put("release", release).put("version", bundle.version()).put("bundleError", bundleError)
                .put("rootPasswordSet", !state.rootPassword().isEmpty()).put("configVersion", state.config().version());
        ArrayNode servers = result.putArray("servers");
        for (Server server : state.servers()) {
            ObjectNode row = ClusterStore.JSON.valueToTree(server);
            row.put("nodes", (int) state.nodes().stream().filter(n -> n.serverId().equals(server.id())).count());
            servers.add(row);
        }
        ArrayNode nodes = result.putArray("nodes");
        int pending = 0, outdated = 0;
        for (NodeSpec node : state.nodes()) {
            ObjectNode row = ClusterStore.JSON.valueToTree(node);
            boolean restart = pendingRestart(state, node);
            boolean upgrade = node.joined() && !release.isEmpty() && !node.releaseId().isEmpty() && !node.releaseId().equals(release);
            if (restart) pending++;
            if (upgrade) outdated++;
            row.put("host", state.server(node.serverId()).map(Server::host).orElse(""));
            row.put("pendingRestart", restart).put("upgradeAvailable", upgrade)
                    .put("overrides", state.config().nodeOverrides().getOrDefault(node.id(), Map.of()).size());
            nodes.add(row);
        }
        result.put("pendingRestart", pending).put("upgradeAvailable", outdated);
        TaskRunner.TaskView active = tasks.active();
        if (active != null) result.set("activeTask", ClusterStore.JSON.valueToTree(active));
        return result;
    }

    /** 访问集群的运行状态：维护模式、各节点可达性。每项独立超时，失败时只标记不可用。 */
    public ObjectNode runtime() {
        ClusterState state = state();
        ObjectNode result = ClusterStore.JSON.createObjectNode().put("code", "OK");
        if (state.nodes(Role.STORAGE).stream().anyMatch(NodeSpec::joined)) {
            try { result.set("maintenance", ops.maintenanceStatus(state)); }
            catch (RuntimeException ex) { result.put("maintenanceError", TaskRunner.message(ex)); }
        }
        ObjectNode nodes = result.putObject("nodes");
        Optional<NodeSpec> via = state.nodes(Role.COMPUTE).stream().filter(n -> n.joined() && n.status() == NodeStatus.RUNNING).findFirst();
        Map<String, String> computeStates = new HashMap<>();
        if (via.isPresent()) {
            try {
                JsonNode rows = ops.computeAdmin(state, via.get(), ClusterStore.JSON.createObjectNode().put("op", "admin").put("command", "showNodes"), 8000).path("rows");
                for (JsonNode row : rows) computeStates.put(row.path(0).asText(), row.path(2).asText());
            } catch (IOException | RuntimeException ex) { result.put("computeError", TaskRunner.message(ex)); }
        }
        for (NodeSpec node : state.nodes()) {
            if (!node.joined()) continue;
            String status = computeStates.getOrDefault(node.id(), via.isPresent() ? "OFFLINE" : "UNKNOWN");
            nodes.put(node.id(), status);
        }
        return result;
    }

    /** 数据库请求转发的目标：第一个运行中的已加入计算节点；没有时返回 null，由调用方回退到静态配置。 */
    public java.net.InetSocketAddress databaseTarget() {
        ClusterState state = state();
        return state.nodes(Role.COMPUTE).stream().filter(n -> n.joined() && n.status() == NodeStatus.RUNNING).findFirst()
                .map(n -> java.net.InetSocketAddress.createUnresolved(ConfigRenderer.host(state, n), n.clientPort())).orElse(null);
    }

    /** 运维总览：以 root 登录受管计算节点调用 ops.overview。 */
    public JsonNode opsOverview(int timeoutMs) throws IOException {
        ClusterState state = state();
        NodeSpec via = state.nodes(Role.COMPUTE).stream().filter(n -> n.joined() && n.status() == NodeStatus.RUNNING).findFirst()
                .orElseThrow(() -> new IOException("集群还没有运行中的计算节点"));
        try { return ops.computeAdmin(state, via, ClusterStore.JSON.createObjectNode().put("op", "ops.overview"), timeoutMs); }
        catch (ClusterError ex) { throw new IOException(ex.getMessage(), ex); }
    }

    private static Map<String, String> trim(Map<String, String> values) {
        Map<String, String> result = new LinkedHashMap<>();
        if (values != null) values.forEach((k, v) -> result.put(k, v == null ? "" : v.trim()));
        return result;
    }

    private static int number(String id) {
        try { return Integer.parseInt(id.substring(id.lastIndexOf('-') + 1)); } catch (RuntimeException ex) { return 0; }
    }

    @Override public void close() { tasks.close(); }
}
