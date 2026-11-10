package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 测试替身：内存中的远程文件系统和可编排的集群 RPC。 */
final class Fakes {
    private Fakes() { }

    /** 一台假服务器：记录执行过的命令和上传的文件。 */
    static final class Host {
        final String hostKey;
        String probe;
        final Map<String, byte[]> files = new ConcurrentHashMap<>();
        final List<String> commands = Collections.synchronizedList(new ArrayList<>());
        final List<String> logins = Collections.synchronizedList(new ArrayList<>());
        String rootPassword = "secret";
        boolean keyInstalled;
        /** systemctl show 的输出；默认服务正常运行。 */
        String service = "ActiveState=active\nSubState=running\nNRestarts=0\n";
        String journal = "";
        java.util.function.Consumer<String> onCommand = c -> { };

        Host(String ip, String arch) {
            hostKey = "SHA256:" + ip;
            probe = "arch=" + arch + "\nos=Rocky Linux 9.4\nosid=rocky\nlibc=ldd (GNU libc) 2.34\nsystemd=yes\nfreemb=50000\n"
                    + "hostname=node-" + ip + "\nips=" + ip + " 10.0.0.1\nuid=0\ntar=yes\n";
        }
    }

    static final class Shells implements RemoteShell.Factory {
        final Map<String, Host> hosts = new ConcurrentHashMap<>();

        Host add(String ip, String arch) { Host host = new Host(ip, arch); hosts.put(ip, host); return host; }

        @Override public String publicKey() { return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQtest"; }

        @Override public RemoteShell open(String ip, int port, String password, String expectedHostKey) {
            Host host = hosts.get(ip);
            if (host == null) throw new ClusterError("SSH_UNREACHABLE", "无法连接 " + ip);
            if (expectedHostKey != null && !expectedHostKey.isEmpty() && !expectedHostKey.equals(host.hostKey))
                throw new ClusterError("HOST_KEY_CHANGED", "指纹变化");
            if (password != null && !password.equals(host.rootPassword)) throw new ClusterError("SSH_AUTH_FAILED", "root 密码错误");
            if (password == null && !host.keyInstalled) throw new ClusterError("SSH_AUTH_FAILED", "免密登录失败");
            host.logins.add(password == null ? "key" : "password");
            return new Shell(host);
        }
    }

    private static final Pattern SHA = Pattern.compile("sha256sum '([^']+)'");
    private static final Pattern COMPLETE = Pattern.compile("cat (\\S+/\\.complete) .*= '([^']+)'");

    static final class Shell implements RemoteShell {
        private final Host host;
        Shell(Host host) { this.host = host; }
        @Override public String hostKey() { return host.hostKey; }
        @Override public Result exec(String command, Duration timeout) {
            host.commands.add(command);
            host.onCommand.accept(command);
            if (command.startsWith("echo arch=")) return new Result(0, host.probe, "");
            if (command.contains("/root/.ssh/authorized_keys") && command.contains(">>")) { host.keyInstalled = true; return new Result(0, "", ""); }
            if (command.startsWith("cat /opt/cengyan/jdk/.cengyan-sha")) {
                byte[] sha = host.files.get("/opt/cengyan/jdk/.cengyan-sha");
                return sha == null ? new Result(1, "", "") : new Result(0, new String(sha, StandardCharsets.UTF_8), "");
            }
            if (command.contains("tar -xzf")) {
                Matcher m = Pattern.compile("echo (\\w+) > ").matcher(command);
                if (m.find()) host.files.put("/opt/cengyan/jdk/.cengyan-sha", (m.group(1) + "\n").getBytes(StandardCharsets.UTF_8));
                return new Result(0, "", "");
            }
            Matcher complete = COMPLETE.matcher(command);
            if (complete.find()) {
                byte[] value = host.files.get(complete.group(1));
                boolean same = value != null && new String(value, StandardCharsets.UTF_8).strip().equals(complete.group(2));
                return new Result(same ? 0 : 1, "", "");
            }
            Matcher sha = SHA.matcher(command);
            if (sha.find()) {
                byte[] value = host.files.get(sha.group(1));
                return new Result(0, value == null ? "" : hex(value) + "\n", "");
            }
            if (command.startsWith("systemctl is-active")) return new Result(3, "", "");
            if (command.startsWith("systemctl show ")) return new Result(0, host.service, "");
            if (command.startsWith("journalctl -u ")) return new Result(0, host.journal, "");
            if (command.contains("ls -A")) return new Result(1, "", "");
            if (command.startsWith("(ss -ltnH")) return new Result(0, "22\n", "");
            return new Result(0, "", "");
        }
        @Override public void upload(Path local, String remote, String mode) throws IOException { host.files.put(remote, Files.readAllBytes(local)); }
        @Override public void write(String remote, byte[] content, String mode) { host.files.put(remote, content); }
        @Override public void close() { }
    }

    static String hex(byte[] value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
        catch (Exception ex) { throw new IllegalStateException(ex); }
    }

    /** 可编排的集群：存储节点 bootId、维护模式、封存状态都在内存中模拟。 */
    static class Ops extends ClusterOps {
        final Set<String> storageJoined = ConcurrentHashMap.newKeySet();
        final Set<String> computeHealthy = ConcurrentHashMap.newKeySet();
        final Map<String, String> boots = new ConcurrentHashMap<>();
        final Map<String, String> sealedBoot = new ConcurrentHashMap<>();
        final Map<String, Boolean> resumed = new ConcurrentHashMap<>();
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        volatile String maintenance = "";
        volatile int lost;
        volatile int lostAfterRestart = -1;
        volatile int replicas = 1;

        /** 模拟 Storage 重启：新 bootId，比对完成后重新封存。 */
        void restarted(String nodeId) {
            String boot = UUID.randomUUID().toString();
            boots.put(nodeId, boot);
            resumed.put(nodeId, true);
            if (!maintenance.isEmpty()) sealedBoot.put(nodeId, boot);
            if (lostAfterRestart >= 0) lost = lostAfterRestart;
        }

        @Override boolean storageReachable(ClusterState state, ClusterState.NodeSpec node) {
            storageJoined.add(node.id());
            boots.putIfAbsent(node.id(), UUID.randomUUID().toString());
            return true;
        }
        @Override Set<String> storageMembers(ClusterState state) { return Set.copyOf(storageJoined); }
        @Override boolean computeHealthy(ClusterState state, ClusterState.NodeSpec node) { computeHealthy.add(node.id()); return true; }
        @Override void checkCompute(ClusterState state, ClusterState.NodeSpec node) { computeHealthy.add(node.id()); }
        @Override void checkStorage(ClusterState state, ClusterState.NodeSpec node) { storageReachable(state, node); }
        @Override int maxReplicas(ClusterState state) { return replicas; }

        @Override String enterMaintenance(ClusterState state) {
            calls.add("enter");
            if (maintenance.isEmpty()) maintenance = UUID.randomUUID().toString();
            boots.forEach(sealedBoot::put);
            return maintenance;
        }
        @Override void exitMaintenance(ClusterState state) { calls.add("exit"); maintenance = ""; sealedBoot.clear(); }
        @Override boolean transferLeader(ClusterState state, String nodeId) { calls.add("transfer " + nodeId); return false; }

        @Override JsonNode maintenanceStatus(ClusterState state) {
            ObjectNode status = ClusterStore.JSON.createObjectNode().put("active", !maintenance.isEmpty()).put("id", maintenance).put("metadataLeader", "storage-1");
            status.putObject("partitions").put("total", 4).put("ready", 4 - lost).put("writing", 0).put("frozen", 0).put("lost", lost);
            var nodes = status.putArray("nodes");
            boots.forEach((id, boot) -> nodes.addObject().put("nodeId", id).put("reachable", true).put("bootId", boot)
                    .put("sealed", !maintenance.isEmpty() && boot.equals(sealedBoot.get(id)))
                    .put("resumed", !maintenance.isEmpty() && resumed.getOrDefault(id, false)).put("untrustedReplicas", 0));
            return status;
        }

        @Override void drainStorage(ClusterState state, String nodeId) { calls.add("drain " + nodeId); }
        @Override boolean removeStorage(ClusterState state, String nodeId) { calls.add("removeStorage " + nodeId); boots.remove(nodeId); return true; }
        @Override void drainCompute(ClusterState state, ClusterState.NodeSpec via, String nodeId) { calls.add("drainCompute " + nodeId); }
        @Override void resumeCompute(ClusterState state, ClusterState.NodeSpec via, String nodeId) { calls.add("resumeCompute " + nodeId); }
        @Override int computeBusy(ClusterState state, ClusterState.NodeSpec via, String nodeId) { return 0; }
        @Override void removeCompute(ClusterState state, ClusterState.NodeSpec via, String nodeId, boolean force) { calls.add("removeCompute " + nodeId); }
        @Override void setRootPassword(ClusterState state, ClusterState.NodeSpec via, String password) { calls.add("setRootPassword"); }
        @Override JsonNode computeAdmin(ClusterState state, ClusterState.NodeSpec via, ObjectNode request, int timeoutMs) {
            return ClusterStore.JSON.createObjectNode().put("code", "OK").set("rows", ClusterStore.JSON.createArrayNode());
        }
    }

    /** 带假节点程序和假 JDK 的 Studio 安装目录。 */
    static Path home(Path dir) throws IOException {
        Files.createDirectories(dir.resolve("nodes"));
        Files.createDirectories(dir.resolve("jdk"));
        for (String name : Bundle.RELEASE_FILES) Files.writeString(dir.resolve("nodes").resolve(name), "fake " + name);
        Files.writeString(dir.resolve("jdk/jdk-21-linux-x64.tar.gz"), "fake jdk x64");
        Files.writeString(dir.resolve("jdk/jdk-21-linux-aarch64.tar.gz"), "fake jdk arm");
        Files.writeString(dir.resolve("BUILD-INFO.json"), "{\"version\":\"1.0.0\"}");
        return dir;
    }
}
