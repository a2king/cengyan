package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.cengyan.studio.cluster.ClusterState.*;
import io.cengyan.studio.cluster.TaskRunner.Context;
import io.cengyan.studio.cluster.TaskRunner.Plan;
import io.cengyan.studio.cluster.TaskRunner.Step;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/** 各类任务的步骤。每一步都可以重复执行；继续或重试时从当前步骤重新开始。 */
final class ClusterPlans {
    private static final Duration JOIN_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration MIGRATE_TIMEOUT = Duration.ofHours(6);
    private final ClusterManager m;

    ClusterPlans(ClusterManager manager) { this.m = manager; }

    Plan plan(String type, Map<String, String> params) {
        return switch (type) {
            case "server.add" -> serverAdd(params.get("server"));
            case "server.remove" -> serverRemove(params.get("server"));
            case "node.add" -> nodeAdd(params.get("node"));
            case "node.remove" -> nodeRemove(params.get("node"), "true".equals(params.get("deleteData")));
            case "restart" -> restart("滚动重启", split(params.get("nodes")), "", Set.of());
            case "upgrade" -> restart("升级集群到 " + params.get("release"), split(params.get("nodes")), params.get("release"),
                    new HashSet<>(split(params.get("previous"))));
            default -> throw new ClusterError("INVALID_ARGUMENT", "未知任务类型：" + type);
        };
    }

    private static Step step(String title, TaskRunner.Action action) { return new Step(title, action); }

    private Server server(String id) {
        return m.state().server(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "服务器不存在：" + id));
    }

    private NodeSpec node(String id) {
        return m.state().node(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "节点不存在：" + id));
    }

    private void save(Server server) throws IOException { m.store.update(s -> s.withServer(server)); }
    private void save(NodeSpec node) throws IOException { m.store.update(s -> s.withNode(node)); }

    // ---- server.add ----
    private Plan serverAdd(String id) {
        String host = m.state().server(id).map(Server::host).orElse(id);
        RemoteShell[] session = new RemoteShell[1];
        TaskRunner.Action openWithPassword = c -> {
            if (session[0] != null) return;
            String password = m.passwords.get(id);
            if (password == null) throw new ClusterError("PASSWORD_REQUIRED", "需要重新输入 root 密码（Studio 不保存密码，重启后需要重新提供）");
            Server s = server(id);
            session[0] = m.shells.open(s.host(), s.sshPort(), password, s.hostKey());
        };
        List<Step> steps = List.of(
                step("密码登录并记录主机指纹", c -> {
                    openWithPassword.run(c);
                    Server s = server(id);
                    save(s.withProbe(session[0].hostKey(), s.arch(), s.os(), s.hostname()).with(ServerStatus.PROVISIONING, "已连接"));
                    c.log("主机指纹 " + session[0].hostKey());
                }),
                step("检测系统环境", c -> {
                    openWithPassword.run(c);
                    Remote.Probe probe = Remote.probe(session[0]);
                    c.log(probe.describe() + "，主机名 " + probe.hostname() + "，/opt 可用 " + probe.freeMb() + "MB");
                    Remote.require(probe, server(id).host());
                    Server s = server(id);
                    save(s.withProbe(s.hostKey(), probe.arch(), probe.os(), probe.hostname()));
                }),
                step("安装 Studio 公钥", c -> {
                    openWithPassword.run(c);
                    Remote.installKey(session[0], m.shells.publicKey(), m.state().clusterId());
                }),
                step("验证免密登录", c -> {
                    if (session[0] != null) { session[0].close(); session[0] = null; }
                    try (RemoteShell shell = m.shell(id)) { shell.run("true", Duration.ofSeconds(10)); }
                    m.passwords.remove(id);
                    c.log("免密登录成功，已丢弃 root 密码");
                }),
                step("安装 JDK 21", c -> {
                    Server s = server(id);
                    Path archive = m.bundle.jdk(s.arch());
                    String sha = Bundle.sha256(archive);
                    if (sha.equals(s.jdkSha())) c.log("已是同一版本，跳过");
                    try (RemoteShell shell = m.shell(id)) { Remote.installJdk(shell, archive, sha); }
                    save(server(id).withJdk(sha));
                }),
                step("同步程序包", c -> {
                    try (RemoteShell shell = m.shell(id)) { Remote.syncRelease(shell, m.bundle); }
                    save(server(id).withRelease(m.bundle.releaseId()));
                }),
                step("完成", c -> save(server(id).with(ServerStatus.READY, "就绪"))));
        return new Plan("添加服务器 " + host, steps, c -> {
            if (session[0] != null) session[0].close();
            m.passwords.remove(id);
            if (m.state().server(id).isPresent()) save(server(id).with(ServerStatus.FAILED, "添加已终止，可移除后重新添加"));
        });
    }

    private Plan serverRemove(String id) {
        String host = m.state().server(id).map(Server::host).orElse(id);
        return new Plan("移除服务器 " + host, List.of(
                step("清理服务器上的程序和服务", c -> { try (RemoteShell shell = m.shell(id)) { Remote.cleanServer(shell); } }),
                step("删除 Studio 公钥", c -> { try (RemoteShell shell = m.shell(id)) { Remote.removeKey(shell, m.shells.publicKey()); } }),
                step("移除服务器记录", c -> m.store.update(s -> s.withoutServer(id)))), null);
    }

    // ---- node.add ----
    private Plan nodeAdd(String id) {
        return new Plan("添加节点 " + id, List.of(
                step("检查端口和数据目录", c -> {
                    NodeSpec n = node(id);
                    try (RemoteShell shell = m.shell(n.serverId())) {
                        if (Remote.active(shell, id)) { c.log("服务已在运行，跳过检查"); return; }
                        Remote.checkFresh(shell, n);
                    }
                }),
                step("同步程序包", c -> {
                    NodeSpec n = node(id);
                    try (RemoteShell shell = m.shell(n.serverId())) { Remote.syncRelease(shell, m.bundle); }
                    save(server(n.serverId()).withRelease(m.bundle.releaseId()));
                }),
                step("写入配置和 systemd 服务", c -> {
                    NodeSpec n = node(id);
                    ConfigRenderer.Rendered rendered = m.renderer.render(m.state(), n, m.bundle.releaseId());
                    try (RemoteShell shell = m.shell(n.serverId())) { Remote.writeNode(shell, n, rendered); }
                    c.remember("hash", rendered.hash());
                    c.log(n.role() == Role.STORAGE ? "metadata_members=" + ConfigRenderer.storageMembers(m.state(), n)
                            : "compute_members=" + ConfigRenderer.computeMembers(m.state(), n));
                }),
                step("启动服务", c -> { NodeSpec n = node(id); try (RemoteShell shell = m.shell(n.serverId())) { Remote.start(shell, id); } }),
                step("等待加入集群", c -> {
                    NodeSpec n = node(id);
                    // Check through the new node itself: the first node of a role has no other member to ask.
                    ClusterState probe = m.state().withNode(n.joinedAs(true));
                    try (RemoteShell shell = m.shell(n.serverId())) {
                        int[] crashes = {0};
                        String[] previous = {""};
                        c.await("节点加入集群", JOIN_TIMEOUT, () -> {
                            crashLoop(shell, id, crashes, previous);
                            if (n.role() == Role.STORAGE) m.ops.checkStorage(probe, n);
                            else m.ops.checkCompute(probe, n);
                            return true;
                        });
                    }
                }),
                step("更新其他节点的成员列表", c -> {
                    String release = m.bundle.releaseId();
                    save(node(id).joinedAs(true).deployed(release, c.param("hash")).with(NodeStatus.RUNNING, "运行中"));
                    rewriteMembers(c);
                })),
                c -> m.state().node(id).ifPresent(n -> {
                    try { save(n.with(NodeStatus.FAILED, "添加已终止，请查看节点日志后移除")); } catch (IOException ignored) { }
                }));
    }

    /** 把最新的成员列表写入所有已加入节点的配置文件；下次启动生效，不重启。 */
    private void rewriteMembers(Context c) {
        ClusterState state = m.state();
        for (NodeSpec n : state.nodes()) {
            if (!n.joined()) continue;
            try (RemoteShell shell = m.shell(n.serverId())) {
                shell.write(Remote.propertiesFile(n), m.renderer.render(state, n, n.releaseId()).properties(), "600");
            } catch (IOException | RuntimeException ex) {
                c.log("警告：" + n.id() + " 的配置文件未更新（" + TaskRunner.message(ex) + "），下次重启前会重新下发");
            }
        }
    }

    // ---- node.remove ----
    private Plan nodeRemove(String id, boolean deleteData) {
        NodeSpec target = node(id);
        List<Step> steps = new ArrayList<>();
        if (target.joined() && target.role() == Role.STORAGE) {
            steps.add(step("检查剩余节点能否满足副本数", c -> {
                ClusterState state = m.state();
                long remaining = state.nodes(Role.STORAGE).stream().filter(n -> n.joined() && !n.id().equals(id)).count();
                int replicas = m.ops.maxReplicas(state);
                if (remaining < replicas) throw new ClusterError("INSUFFICIENT_NODES", "移除后只剩 " + remaining + " 个存储节点，少于图空间的副本数 " + replicas + "。请先调低副本数或增加节点");
                if (m.ops.maintenanceStatus(state).path("active").asBoolean()) throw new ClusterError("CLUSTER_MAINTENANCE", "集群处于维护模式，请先退出维护模式");
            }));
            steps.add(step("迁移分片", c -> {
                m.ops.drainStorage(m.state(), id);
                c.await("分片迁移完成", MIGRATE_TIMEOUT, () -> m.ops.removeStorage(m.state(), id));
            }));
        }
        if (target.joined() && target.role() == Role.COMPUTE) {
            steps.add(step("停止接收新查询", c -> m.ops.drainCompute(m.state(), otherCompute(id), id)));
            steps.add(step("等待进行中的查询结束", c -> c.await("查询结束", DRAIN_TIMEOUT, () -> idle(id))));
            steps.add(step("从计算成员中移除", c -> m.ops.removeCompute(m.state(), otherCompute(id), id, true)));
        }
        steps.add(step(deleteData ? "停止服务并删除节点和数据目录" : "停止服务并删除节点（保留数据目录）", c -> {
            NodeSpec n = node(id);
            try (RemoteShell shell = m.shell(n.serverId())) { Remote.removeNode(shell, n, deleteData); }
        }));
        steps.add(step("移除节点记录并更新成员列表", c -> {
            m.store.update(s -> s.withoutNode(id).withConfig(withoutOverrides(s.config(), id)));
            rewriteMembers(c);
        }));
        return new Plan("移除节点 " + id, steps, c -> {
            if (target.joined() && target.role() == Role.STORAGE) c.log("存储节点可能仍处于排空状态，副本不会再放置到该节点");
            if (target.joined() && target.role() == Role.COMPUTE) {
                try { m.ops.resumeCompute(m.state(), otherCompute(id), id); } catch (IOException | RuntimeException ignored) { }
            }
        });
    }

    private static ConfigVersion withoutOverrides(ConfigVersion config, String nodeId) {
        if (!config.nodeOverrides().containsKey(nodeId)) return config;
        Map<String, Map<String, String>> overrides = new LinkedHashMap<>(config.nodeOverrides());
        overrides.remove(nodeId);
        return new ConfigVersion(config.version(), config.savedAt(), config.note(), config.roleDefaults(), overrides);
    }

    /** 节点不可达也视为没有进行中的查询。 */
    private boolean idle(String id) {
        try { return m.ops.computeBusy(m.state(), otherCompute(id), id) == 0; }
        catch (IOException | RuntimeException ex) { return true; }
    }

    private static final int MAX_CRASHES = 3;

    /**
     * 进程启动后立即退出时 systemd 每 10 秒拉起一次，端口检查只会一直"等待"。
     * 发现反复退出或已失败时立即失败，并附上节点最近的日志。
     */
    private void crashLoop(RemoteShell shell, String id, int[] crashes, String[] previous) {
        Remote.ServiceState state;
        try { state = Remote.serviceState(shell, id); }
        catch (IOException ex) { throw new ClusterError("PROGRESS", "无法读取服务状态：" + ex.getMessage()); }
        // Count transitions into auto-restart; old systemd has no NRestarts.
        if (state.restarting() && !previous[0].equals("auto-restart")) crashes[0]++;
        previous[0] = state.sub();
        int restarts = Math.max(crashes[0], state.restarts());
        if (state.failed() || restarts >= MAX_CRASHES) {
            String logs;
            try { logs = Remote.logs(shell, id, 40); } catch (IOException ex) { logs = "（读取日志失败：" + ex.getMessage() + "）"; }
            throw new TaskRunner.Fail("服务 " + Remote.unit(id) + (state.failed() ? " 启动失败" : " 启动后反复退出（已重启 " + restarts + " 次）")
                    + "。节点最近日志：\n" + logs);
        }
        if (!"active".equals(state.active())) throw new ClusterError("PROGRESS", "服务状态 " + state.active() + "/" + state.sub());
    }

    /** 选一个健康的、不是 except 的计算节点发 admin 命令；没有时用 except 自己。 */
    private NodeSpec otherCompute(String except) {
        ClusterState state = m.state();
        List<NodeSpec> candidates = state.nodes(Role.COMPUTE).stream().filter(n -> n.joined() && !n.id().equals(except)).toList();
        for (NodeSpec n : candidates) if (m.ops.computeHealthy(state, n)) return n;
        return state.node(except).orElseThrow(() -> new ClusterError("NO_COMPUTE", "没有可用的计算节点"));
    }

    // ---- restart / upgrade ----
    private Plan restart(String title, List<String> ids, String release, Set<String> previous) {
        List<NodeSpec> storage = new ArrayList<>(), compute = new ArrayList<>();
        for (String id : ids) { NodeSpec n = node(id); (n.role() == Role.STORAGE ? storage : compute).add(n); }
        List<Step> steps = new ArrayList<>();
        if (!release.isEmpty()) steps.add(step("同步新程序包到所有服务器", c -> {
            for (Server s : m.state().servers()) {
                if (s.status() != ServerStatus.READY) continue;
                c.log("同步到 " + s.host());
                try (RemoteShell shell = m.shell(s.id())) { Remote.syncRelease(shell, m.bundle); }
                save(server(s.id()).withRelease(m.bundle.releaseId()));
            }
        }));
        if (!storage.isEmpty()) {
            steps.add(step("进入维护模式，暂停写入", c -> {
                c.remember("maintenance", m.ops.enterMaintenance(m.state()));
                c.remember("lostBefore", Integer.toString(m.ops.maintenanceStatus(m.state()).path("partitions").path("lost").asInt()));
            }));
            steps.add(step("等待所有存储节点封存本地分片", c -> c.await("封存完成", JOIN_TIMEOUT, () -> sealed(null))));
            for (NodeSpec n : storage) {
                String id = n.id();
                steps.add(step(id + "：下发配置", c -> push(c, id, release)));
                steps.add(step(id + "：转移 Metadata Leader", c -> {
                    if (m.ops.transferLeader(m.state(), id)) c.log("Leader 已转移到其他节点");
                }));
                steps.add(step(id + "：重启", c -> {
                    c.await("本节点封存完成", JOIN_TIMEOUT, () -> sealed(id));
                    c.remember("boot." + id, bootId(id));
                    try (RemoteShell shell = m.shell(node(id).serverId())) { Remote.restart(shell, id); }
                }));
                steps.add(step(id + "：等待恢复并完成文件比对", c -> {
                    c.await("节点恢复", JOIN_TIMEOUT, () -> storageRecovered(c, id));
                    markDeployed(c, id);
                }));
            }
            steps.add(step("退出维护模式，恢复写入", c -> m.ops.exitMaintenance(m.state())));
        }
        for (NodeSpec n : compute) {
            String id = n.id();
            steps.add(step(id + "：下发配置", c -> push(c, id, release)));
            steps.add(step(id + "：停止接收新查询", c -> {
                NodeSpec via = otherCompute(id);
                if (via.id().equals(id)) { c.log("只有一个计算节点，重启期间查询会失败"); return; }
                m.ops.drainCompute(m.state(), via, id);
                c.remember("drained." + id, "true");
                c.await("进行中的查询结束", DRAIN_TIMEOUT, () -> idle(id));
            }));
            steps.add(step(id + "：重启", c -> { try (RemoteShell shell = m.shell(node(id).serverId())) { Remote.restart(shell, id); } }));
            steps.add(step(id + "：等待恢复", c -> {
                c.await("节点恢复", JOIN_TIMEOUT, () -> { m.ops.checkCompute(m.state(), node(id)); return true; });
                if ("true".equals(c.param("drained." + id))) {
                    m.ops.resumeCompute(m.state(), node(id), id);
                    c.remember("drained." + id, "");
                }
                markDeployed(c, id);
            }));
        }
        if (!release.isEmpty()) steps.add(step("清理旧程序包", c -> {
            Set<String> keep = new HashSet<>(previous);
            keep.add(release);
            for (NodeSpec n : m.state().nodes()) keep.add(n.releaseId());
            keep.remove("");
            for (Server s : m.state().servers()) {
                if (s.status() != ServerStatus.READY) continue;
                try (RemoteShell shell = m.shell(s.id())) { Remote.pruneReleases(shell, keep); }
                catch (IOException | RuntimeException ex) { c.log("警告：" + s.host() + " 清理失败：" + TaskRunner.message(ex)); }
            }
        }));
        return new Plan(title, steps, c -> {
            for (NodeSpec n : compute) {
                if (!"true".equals(c.param("drained." + n.id()))) continue;
                try { m.ops.resumeCompute(m.state(), otherCompute(""), n.id()); } catch (IOException | RuntimeException ignored) { }
            }
            if (!storage.isEmpty()) {
                if ("true".equals(c.param("exitMaintenance"))) m.ops.exitMaintenance(m.state());
                else c.log("集群仍处于维护模式（写入暂停），确认节点正常后请在概览页退出维护模式");
            }
        });
    }

    /** 下发配置文件和 node.env，不重启。release 为空时保持节点当前版本。 */
    private void push(Context c, String id, String release) throws IOException {
        NodeSpec n = node(id);
        String target = !release.isEmpty() ? release : n.releaseId().isEmpty() ? m.bundle.releaseId() : n.releaseId();
        ConfigRenderer.Rendered rendered = m.renderer.render(m.state(), n, target);
        try (RemoteShell shell = m.shell(n.serverId())) {
            if (target.equals(m.bundle.releaseId())) Remote.syncRelease(shell, m.bundle);
            Remote.writeNode(shell, n, rendered);
        }
        c.remember("hash." + id, rendered.hash());
        c.remember("release." + id, target);
    }

    private void markDeployed(Context c, String id) throws IOException {
        save(node(id).deployed(c.param("release." + id), c.param("hash." + id)).with(NodeStatus.RUNNING, "运行中"));
    }

    // ---- gates ----
    private JsonNode statusNode(JsonNode status, String id) {
        for (JsonNode n : status.path("nodes")) if (n.path("nodeId").asText().equals(id)) return n;
        return null;
    }

    /** nodeId 为空时检查所有可达的存储节点。返回 false 或抛出说明原因的异常都表示继续等待。 */
    private boolean sealed(String nodeId) {
        JsonNode status = m.ops.maintenanceStatus(m.state());
        if (!status.path("active").asBoolean()) throw new TaskRunner.Fail("集群不在维护模式（可能已被手动退出），已停止滚动重启");
        JsonNode parts = status.path("partitions");
        if (parts.path("writing").asInt() > 0 || parts.path("frozen").asInt() > 0)
            throw new ClusterError("PROGRESS", "还有 " + parts.path("writing").asInt() + " 个分片在写入、" + parts.path("frozen").asInt() + " 个在全量同步");
        for (JsonNode n : status.path("nodes")) {
            if (nodeId != null && !n.path("nodeId").asText().equals(nodeId)) continue;
            if (!n.path("reachable").asBoolean()) continue;
            if (!n.path("sealed").asBoolean()) throw new ClusterError("PROGRESS", n.path("nodeId").asText() + " 尚未完成封存");
        }
        return true;
    }

    private String bootId(String id) {
        JsonNode n = statusNode(m.ops.maintenanceStatus(m.state()), id);
        return n == null ? "" : n.path("bootId").asText();
    }

    /** 新进程已启动、比对完成、本节点所有副本可信，且没有分片在写入或同步。新出现丢失分片时立即失败。 */
    private boolean storageRecovered(Context c, String id) {
        JsonNode status = m.ops.maintenanceStatus(m.state());
        int lost = status.path("partitions").path("lost").asInt();
        int before = c.param("lostBefore").isEmpty() ? 0 : Integer.parseInt(c.param("lostBefore"));
        if (lost > before) throw new TaskRunner.Fail("有 " + (lost - before) + " 个分片在重启后丢失（所有副本都未通过比对）。滚动重启已暂停，请检查节点日志");
        JsonNode n = statusNode(status, id);
        if (n == null || !n.path("reachable").asBoolean()) throw new ClusterError("PROGRESS", "节点尚未启动");
        if (n.path("bootId").asText().equals(c.param("boot." + id))) throw new ClusterError("PROGRESS", "节点尚未重启");
        if (!n.path("resumed").asBoolean()) throw new ClusterError("PROGRESS", "正在比对本地数据文件");
        if (n.path("untrustedReplicas").asInt() > 0) throw new ClusterError("PROGRESS", n.path("untrustedReplicas").asInt() + " 个副本比对不一致，正在全量同步");
        JsonNode parts = status.path("partitions");
        if (parts.path("writing").asInt() > 0 || parts.path("frozen").asInt() > 0) throw new ClusterError("PROGRESS", "分片同步中");
        if (status.path("metadataLeader").asText().isEmpty()) throw new ClusterError("PROGRESS", "Metadata 尚未选出 Leader");
        return true;
    }

    private static List<String> split(String text) {
        if (text == null || text.isBlank()) return List.of();
        return Arrays.stream(text.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
