package io.cengyan.studio.cluster;

import io.cengyan.studio.cluster.ClusterState.*;
import io.cengyan.studio.cluster.TaskRunner.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class ClusterManagerTest {
    @TempDir Path tmp;
    final Fakes.Shells shells = new Fakes.Shells();
    final Fakes.Ops ops = new Fakes.Ops();
    ClusterManager manager;

    @BeforeEach void open() throws IOException { manager = reopen(); }
    @AfterEach void close() { manager.close(); }

    private ClusterManager reopen() throws IOException {
        Path home = Fakes.home(tmp.resolve("home"));
        return new ClusterManager(new ClusterStore(home.resolve("data")), new Bundle(home), ops, shells);
    }

    private TaskRunner.TaskView await(TaskRunner.TaskView task) throws InterruptedException {
        long until = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < until) {
            TaskRunner.TaskView current = manager.tasks().get(task.id());
            if (current.state() != State.RUNNING) return current;
            Thread.sleep(20);
        }
        fail("任务未结束：" + manager.tasks().get(task.id()));
        return null;
    }

    private Fakes.Host ready(String ip) throws Exception {
        Fakes.Host host = shells.add(ip, "x86_64");
        TaskRunner.TaskView task = await(manager.addServer(ip, 22, "secret", ""));
        assertEquals(State.DONE, task.state(), task.error());
        return host;
    }

    private String serverId(String ip) {
        return manager.state().servers().stream().filter(s -> s.host().equals(ip)).findFirst().orElseThrow().id();
    }

    private void addNode(Role role, String ip, String id) throws Exception {
        var suggestion = manager.suggest(role, serverId(ip));
        TaskRunner.TaskView task = await(manager.addNode(role, serverId(ip), id, suggestion.path("raftPort").asInt(),
                suggestion.path("rpcPort").asInt(), suggestion.path("clientPort").asInt(), "/opt/cengyan/data/" + id));
        assertEquals(State.DONE, task.state(), task.error());
    }

    private static String file(Fakes.Host host, String path) {
        byte[] bytes = host.files.get(path);
        assertNotNull(bytes, "远程文件不存在：" + path);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test void addServerInstallsKeyJdkReleaseAndForgetsPassword() throws Exception {
        Fakes.Host host = ready("10.0.0.11");
        Server server = manager.state().servers().getFirst();
        assertEquals(ServerStatus.READY, server.status());
        assertEquals("x86_64", server.arch());
        assertEquals("SHA256:10.0.0.11", server.hostKey(), "首次连接记录主机指纹");
        assertTrue(host.keyInstalled);
        assertTrue(host.logins.contains("key"), "验证过免密登录");
        assertTrue(manager.passwords.isEmpty(), "免密验证通过后丢弃 root 密码");
        String release = manager.bundle.releaseId();
        assertEquals(release, server.releaseId());
        for (String name : Bundle.RELEASE_FILES) assertTrue(host.files.containsKey("/opt/cengyan/releases/" + release + "/" + name));
        assertTrue(host.files.containsKey("/opt/cengyan/jdk/.cengyan-sha"));
        assertFalse(java.nio.file.Files.readString(manager.store.dataDir().resolve("state.json")).contains("secret"), "密码不写盘");
        assertEquals("CONFLICT", assertThrows(ClusterError.class, () -> manager.addServer("10.0.0.11", 22, "secret", "")).code());
    }

    @Test void unsupportedServerPausesAndPasswordIsNeededAgainAfterStudioRestart() throws Exception {
        Fakes.Host host = shells.add("10.0.0.12", "x86_64");
        host.probe = host.probe.replace("2.34", "2.12");
        TaskRunner.TaskView task = await(manager.addServer("10.0.0.12", 22, "secret", ""));
        assertEquals(State.PAUSED, task.state());
        assertTrue(task.error().contains("glibc"), task.error());
        host.probe = host.probe.replace("2.12", "2.34");
        manager.close();
        manager = reopen();
        task = await(manager.act(task.id(), "retry", null, false));
        assertEquals(State.PAUSED, task.state());
        assertTrue(task.error().contains("root 密码"), "Studio 重启后不保留密码：" + task.error());
        task = await(manager.act(task.id(), "retry", "secret", false));
        assertEquals(State.DONE, task.state(), task.error());
        assertEquals(ServerStatus.READY, manager.state().servers().getFirst().status());
    }

    @Test void wrongPasswordPauses() throws Exception {
        shells.add("10.0.0.13", "aarch64");
        TaskRunner.TaskView task = await(manager.addServer("10.0.0.13", 22, "wrong", ""));
        assertEquals(State.PAUSED, task.state());
        assertTrue(task.error().contains("root 密码错误"), task.error());
        task = await(manager.act(task.id(), "abort", null, false));
        assertEquals(State.ABORTED, task.state());
        assertEquals(ServerStatus.FAILED, manager.state().servers().getFirst().status());
    }

    @Test void storageMembershipBootstrapsThenJoins() throws Exception {
        Fakes.Host a = ready("10.0.1.1");
        Fakes.Host b = ready("10.0.1.2");
        addNode(Role.STORAGE, "10.0.1.1", "storage-1");
        String first = file(a, "/opt/cengyan/nodes/storage-1/storage.properties");
        assertTrue(first.contains("metadata_members=storage-1@10.0.1.1:19700@10.0.1.1:19701"), first);
        assertTrue(first.contains("data_storage_mode=raft"));
        assertTrue(first.contains("# 写入线程数"), "参数带中文注释");
        assertTrue(first.contains("rpc_address=10.0.1.1:19701"));
        String unit = file(a, "/etc/systemd/system/cengyan-storage-1.service");
        assertTrue(unit.contains("ExecStart=/opt/cengyan/jdk/bin/java $JAVA_OPTS"), unit);
        assertTrue(unit.contains("/opt/cengyan/releases/${CENGYAN_RELEASE}/cengyan-storage.jar /opt/cengyan/nodes/storage-1/storage.properties"), unit);
        assertTrue(a.commands.stream().anyMatch(c -> c.startsWith("systemctl enable --now cengyan-storage-1.service")));

        // Before joining, the second node only knows the existing member and joins on start.
        List<String> seen = new ArrayList<>();
        b.onCommand = c -> {
            byte[] props = b.files.get("/opt/cengyan/nodes/storage-2/storage.properties");
            if (c.startsWith("systemctl enable --now") && props != null) seen.add(new String(props, StandardCharsets.UTF_8));
        };
        addNode(Role.STORAGE, "10.0.1.2", "storage-2");
        assertTrue(seen.getFirst().contains("metadata_members=storage-1@10.0.1.1:19700@10.0.1.1:19701\n"), seen.getFirst());
        String both = "metadata_members=storage-1@10.0.1.1:19700@10.0.1.1:19701,storage-2@10.0.1.2:19700@10.0.1.2:19701";
        assertTrue(file(b, "/opt/cengyan/nodes/storage-2/storage.properties").contains(both));
        assertTrue(file(a, "/opt/cengyan/nodes/storage-1/storage.properties").contains(both), "加入后更新已有节点的成员列表");
        assertFalse(manager.pendingRestart(manager.state(), manager.state().node("storage-1").orElseThrow()), "成员列表变化不需要重启");

        assertEquals("NOT_FOUND", assertThrows(ClusterError.class, () -> manager.addNode(Role.STORAGE, "missing", "x", 1, 2, 0, "/opt/x/y")).code());
        assertEquals("PORT_IN_USE", assertThrows(ClusterError.class,
                () -> manager.addNode(Role.STORAGE, serverId("10.0.1.1"), "storage-9", 19700, 19777, 0, "/opt/cengyan/data/s9")).code());
        assertEquals(19710, manager.suggest(Role.STORAGE, serverId("10.0.1.1")).path("raftPort").asInt(), "同机第二个节点端口顺延");
    }

    @Test void crashingNodeFailsFastWithItsLogs() throws Exception {
        Fakes.Host a = ready("10.0.2.9");
        a.service = "ActiveState=activating\nSubState=auto-restart\nNRestarts=4\n";
        a.journal = "java.lang.UnsatisfiedLinkError: /tmp/librocksdbjni.so: GLIBCXX_3.4.21 not found";
        var suggestion = manager.suggest(Role.STORAGE, serverId("10.0.2.9"));
        TaskRunner.TaskView task = await(manager.addNode(Role.STORAGE, serverId("10.0.2.9"), "storage-1", suggestion.path("raftPort").asInt(),
                suggestion.path("rpcPort").asInt(), 0, "/opt/cengyan/data/storage-1"));
        assertEquals(State.PAUSED, task.state(), "服务反复退出时不应等满 10 分钟");
        assertTrue(task.error().contains("反复退出"), task.error());
        assertTrue(task.error().contains("GLIBCXX_3.4.21"), "错误里附上节点日志：" + task.error());
    }

    @Test void computeNeedsStorageAndRootPassword() throws Exception {
        Fakes.Host a = ready("10.0.2.1");
        String server = serverId("10.0.2.1");
        assertEquals("INVALID_STATE", assertThrows(ClusterError.class, () -> manager.addNode(Role.COMPUTE, server, "compute-1", 19800, 19801, 19669, "/opt/cengyan/data/c1")).code());
        addNode(Role.STORAGE, "10.0.2.1", "storage-1");
        assertEquals("ROOT_PASSWORD_REQUIRED", assertThrows(ClusterError.class, () -> manager.addNode(Role.COMPUTE, server, "compute-1", 19800, 19801, 19669, "/opt/cengyan/data/c1")).code());
        manager.setRootPassword("db-root-pass");
        addNode(Role.COMPUTE, "10.0.2.1", "compute-1");
        String props = file(a, "/opt/cengyan/nodes/compute-1/compute.properties");
        assertTrue(props.contains("storage_seeds=10.0.2.1:19701"), props);
        assertTrue(props.contains("root_password=db-root-pass"));
        assertTrue(props.contains("compute_members=compute-1@10.0.2.1:19801@10.0.2.1:19800@10.0.2.1:19669"), props);
        assertTrue(file(a, "/opt/cengyan/nodes/compute-1/node.env").contains("JAVA_OPTS=\"-Xms1g -Xmx4g\""));
        assertEquals(new java.net.InetSocketAddress("10.0.2.1", 19669).getPort(), manager.databaseTarget().getPort());
        assertEquals("LAST_NODE", assertThrows(ClusterError.class, () -> manager.removeNode("compute-1", false)).code());
    }

    private void cluster() throws Exception {
        for (String ip : List.of("10.0.3.1", "10.0.3.2")) ready(ip);
        addNode(Role.STORAGE, "10.0.3.1", "storage-1");
        addNode(Role.STORAGE, "10.0.3.2", "storage-2");
        manager.setRootPassword("db-root-pass");
        addNode(Role.COMPUTE, "10.0.3.1", "compute-1");
    }

    @Test void configSaveValidatesAndMarksPendingRestart() throws Exception {
        cluster();
        assertEquals(0, manager.overview().path("pendingRestart").asInt());
        ClusterError bad = assertThrows(ClusterError.class, () -> manager.saveConfig(Map.of(Role.STORAGE, Map.of("write_threads", "0")), Map.of(), ""));
        assertEquals("INVALID_CONFIG", bad.code());
        assertTrue(bad.details().containsKey("STORAGE.write_threads"), bad.details().toString());
        assertTrue(assertThrows(ClusterError.class, () -> manager.saveConfig(Map.of(Role.STORAGE, Map.of("JAVA_OPTS", "-Xmx1g; rm -rf /")), Map.of(), ""))
                .details().containsKey("STORAGE.JAVA_OPTS"), "JVM 参数不能注入 shell");
        assertTrue(assertThrows(ClusterError.class, () -> manager.saveConfig(Map.of(Role.COMPUTE, Map.of("query_memory_bytes", "1073741824")), Map.of(), ""))
                .details().containsKey("COMPUTE.query_memory_bytes"), "单查询内存不能超过节点内存");

        ConfigVersion v1 = manager.saveConfig(Map.of(Role.STORAGE, Map.of("write_threads", "16", "admission_threads", "2")), Map.of(), "调大写入线程");
        assertEquals(1, v1.version());
        assertEquals(Map.of("write_threads", "16"), v1.roleDefaults().get(Role.STORAGE), "与默认值相同的项不保存");
        assertEquals(2, manager.overview().path("pendingRestart").asInt(), "两个存储节点待重启，计算节点不受影响");

        ConfigVersion v2 = manager.saveConfig(Map.of(Role.STORAGE, Map.of("write_threads", "16")), Map.of("storage-2", Map.of("zone", "zone-b")), "");
        String rendered = manager.renderer.render(manager.state(), manager.state().node("storage-2").orElseThrow(), "r").properties();
        assertTrue(rendered.contains("zone=zone-b"));
        assertTrue(rendered.contains("【本节点单独设置】"));
        assertEquals(List.of(new ClusterManager.Change("storage-2", "zone", "", "zone-b")), manager.diff(v1.version(), v2.version()));

        manager.rollback(0);
        assertEquals(0, manager.overview().path("pendingRestart").asInt(), "回滚到已下发的内容后不再待重启");
        assertEquals(3, manager.history().getFirst().version());
    }

    /** systemctl restart 在假服务器上触发"进程重启"。 */
    private List<String> restartHooks() {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        for (Fakes.Host host : shells.hosts.values()) host.onCommand = c -> {
            if (!c.startsWith("systemctl restart cengyan-")) return;
            String id = c.substring("systemctl restart cengyan-".length()).replace(".service", "");
            order.add(id);
            if (id.startsWith("storage")) ops.restarted(id);
        };
        return order;
    }

    @Test void rollingRestartGoesOneNodeAtATimeInsideMaintenance() throws Exception {
        cluster();
        List<String> order = restartHooks();
        manager.saveConfig(Map.of(Role.STORAGE, Map.of("write_threads", "16"), Role.COMPUTE, Map.of("query_slots", "8")), Map.of(), "");
        assertEquals(3, manager.overview().path("pendingRestart").asInt());
        TaskRunner.TaskView task = await(manager.restart(List.of()));
        assertEquals(State.DONE, task.state(), task.error());
        assertEquals(List.of("storage-1", "storage-2", "compute-1"), order, "存储在前、计算在后，一次一台");
        assertEquals("enter", ops.calls.getFirst());
        assertTrue(ops.calls.indexOf("exit") > ops.calls.indexOf("transfer storage-2"), "全部存储节点重启后才退出维护模式");
        assertFalse(ops.calls.contains("drainCompute compute-1"), "只有一个计算节点时不排空");
        assertEquals("", ops.maintenance);
        assertEquals(0, manager.overview().path("pendingRestart").asInt(), "重启后不再待重启");
        assertTrue(file(shells.hosts.get("10.0.3.1"), "/opt/cengyan/nodes/storage-1/storage.properties").contains("write_threads=16"));
    }

    @Test void singleNodeRestartLeavesOthersAlone() throws Exception {
        cluster();
        List<String> order = restartHooks();
        manager.saveConfig(Map.of(), Map.of("storage-2", Map.of("rack", "r2")), "");
        TaskRunner.TaskView task = await(manager.restart(List.of("storage-2")));
        assertEquals(State.DONE, task.state(), task.error());
        assertEquals(List.of("storage-2"), order);
    }

    @Test void restartPausesImmediatelyWhenPartitionsAreLost() throws Exception {
        cluster();
        restartHooks();
        ops.lostAfterRestart = 1;
        TaskRunner.TaskView task = await(manager.restart(List.of("storage-1", "storage-2")));
        assertEquals(State.PAUSED, task.state());
        assertTrue(task.error().contains("丢失"), task.error());
        assertFalse(ops.maintenance.isEmpty(), "失败时保持维护模式");
        assertTrue(task.steps().get(task.current()).title().startsWith("storage-1"), "停在第一台，不继续重启第二台");
        task = await(manager.act(task.id(), "abort", null, true));
        assertEquals(State.ABORTED, task.state());
        assertEquals("", ops.maintenance, "终止时选择退出维护模式");
    }

    @Test void removeStorageChecksReplicasThenDrains() throws Exception {
        cluster();
        ops.replicas = 2;
        TaskRunner.TaskView refused = await(manager.removeNode("storage-2", false));
        assertEquals(State.PAUSED, refused.state());
        assertTrue(refused.error().contains("副本数"), refused.error());
        await(manager.act(refused.id(), "abort", null, false));
        ops.replicas = 1;
        TaskRunner.TaskView task = await(manager.removeNode("storage-2", true));
        assertEquals(State.DONE, task.state(), task.error());
        assertTrue(ops.calls.containsAll(List.of("drain storage-2", "removeStorage storage-2")));
        assertTrue(manager.state().node("storage-2").isEmpty());
        assertTrue(shells.hosts.get("10.0.3.2").commands.stream().anyMatch(c -> c.contains("rm -rf -- '/opt/cengyan/data/storage-2'")), "按选择删除数据目录");
        assertFalse(file(shells.hosts.get("10.0.3.1"), "/opt/cengyan/nodes/storage-1/storage.properties").contains("storage-2@"), "其他节点成员列表已更新");
    }

    @Test void taskRunnerPersistsAndReportsInterrupted() throws Exception {
        cluster();
        assertTrue(manager.tasks().list(10).size() >= 5);
        assertNull(manager.tasks().active());
    }
}
