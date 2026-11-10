package io.cengyan.studio.cluster;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static io.cengyan.studio.cluster.RemoteShell.quote;

/** 受管服务器上的目录约定和幂等的远程操作。所有命令以 root 执行。 */
final class Remote {
    static final String BASE = "/opt/cengyan";
    static final String JDK = BASE + "/jdk";
    static final String RELEASES = BASE + "/releases";
    static final String NODES = BASE + "/nodes";
    static final String DATA = BASE + "/data";
    static final String STAGING = BASE + "/staging";
    private static final Duration SHORT = Duration.ofSeconds(30);
    private static final Duration LONG = Duration.ofMinutes(10);

    private Remote() { }

    record Probe(String arch, String os, String hostname, List<String> ips, String libc, boolean systemd, long freeMb, boolean root, boolean tar) {
        String describe() { return os + " / " + arch + " / glibc " + libc; }
    }

    private static final String PROBE = String.join("; ",
            "echo arch=$(uname -m)",
            "( . /etc/os-release 2>/dev/null; echo os=\"$PRETTY_NAME\"; echo osid=\"$ID\" )",
            "echo libc=$(ldd --version 2>&1 | head -n1)",
            "if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then echo systemd=yes; else echo systemd=no; fi",
            "mkdir -p " + BASE + " 2>/dev/null; echo freemb=$(df -Pm " + BASE + " 2>/dev/null | awk 'NR==2{print $4}')",
            "echo hostname=$(hostname)",
            "echo ips=$(hostname -I 2>/dev/null || ip -o -4 addr show 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | tr '\\n' ' ')",
            "echo uid=$(id -u)",
            "if command -v tar >/dev/null 2>&1; then echo tar=yes; else echo tar=no; fi");

    static Probe probe(RemoteShell shell) throws IOException {
        Map<String, String> values = new HashMap<>();
        for (String line : shell.run(PROBE, SHORT).split("\n")) {
            int eq = line.indexOf('=');
            if (eq > 0) values.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
        }
        String libcLine = values.getOrDefault("libc", "");
        String libc = libcLine.toLowerCase(Locale.ROOT).contains("musl") ? "musl"
                : libcLine.replaceAll(".*?(\\d+\\.\\d+)\\s*$", "$1");
        long free;
        try { free = Long.parseLong(values.getOrDefault("freemb", "0")); } catch (NumberFormatException ex) { free = 0; }
        String os = values.getOrDefault("os", "");
        if (os.isBlank()) os = values.getOrDefault("osid", "Linux");
        return new Probe(values.getOrDefault("arch", ""), os, values.getOrDefault("hostname", ""),
                Arrays.stream(values.getOrDefault("ips", "").split("\\s+")).filter(s -> !s.isBlank()).toList(),
                libc, "yes".equals(values.get("systemd")), free, "0".equals(values.get("uid")), "yes".equals(values.get("tar")));
    }

    /** 不满足条件时抛出带原因的异常。 */
    static void require(Probe probe, String host) {
        if (!probe.root()) throw new ClusterError("UNSUPPORTED_SERVER", "登录用户不是 root");
        if (!Set.of("x86_64", "aarch64").contains(probe.arch())) throw new ClusterError("UNSUPPORTED_SERVER", "不支持的 CPU 架构：" + probe.arch() + "（只支持 x86_64、aarch64）");
        if ("musl".equals(probe.libc())) throw new ClusterError("UNSUPPORTED_SERVER", "不支持 musl 系统（如 Alpine），离线 JDK 需要 glibc");
        if (!probe.libc().matches("\\d+\\.\\d+") || compare(probe.libc(), "2.17") < 0)
            throw new ClusterError("UNSUPPORTED_SERVER", "glibc 版本过低或无法识别：" + probe.libc() + "（需要 2.17 以上）");
        if (!probe.systemd()) throw new ClusterError("UNSUPPORTED_SERVER", "服务器没有运行 systemd");
        if (!probe.tar()) throw new ClusterError("UNSUPPORTED_SERVER", "服务器缺少 tar 命令");
        if (probe.freeMb() < 1024) throw new ClusterError("UNSUPPORTED_SERVER", BASE + " 所在磁盘可用空间不足 1GB（当前 " + probe.freeMb() + "MB）");
        if (host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") && !probe.ips().contains(host))
            throw new ClusterError("UNSUPPORTED_SERVER", "服务器网卡上没有地址 " + host + "（当前：" + String.join(", ", probe.ips())
                    + "）。节点会监听这个地址，请填写服务器的内网 IP");
    }

    private static int compare(String a, String b) {
        String[] x = a.split("\\."), y = b.split("\\.");
        for (int i = 0; i < Math.max(x.length, y.length); i++) {
            int p = i < x.length ? Integer.parseInt(x[i]) : 0, q = i < y.length ? Integer.parseInt(y[i]) : 0;
            if (p != q) return Integer.compare(p, q);
        }
        return 0;
    }

    // ---- SSH key ----
    static String keyComment(String clusterId) { return "cengyan-studio-" + clusterId; }

    static void installKey(RemoteShell shell, String publicKey, String clusterId) throws IOException {
        if (!publicKey.matches("[A-Za-z0-9-]+ [A-Za-z0-9+/=]+")) throw new IllegalStateException("Studio 公钥格式异常");
        String line = publicKey + " " + keyComment(clusterId);
        shell.run("umask 077; mkdir -p /root/.ssh; touch /root/.ssh/authorized_keys; "
                + "grep -qF " + quote(publicKey) + " /root/.ssh/authorized_keys || echo " + quote(line) + " >> /root/.ssh/authorized_keys; "
                + "chmod 700 /root/.ssh; chmod 600 /root/.ssh/authorized_keys; "
                + "command -v restorecon >/dev/null 2>&1 && restorecon -R /root/.ssh || true", SHORT);
    }

    static void removeKey(RemoteShell shell, String publicKey) throws IOException {
        String body = publicKey.split(" ")[1];
        shell.run("f=/root/.ssh/authorized_keys; [ -f $f ] || exit 0; grep -vF " + quote(body) + " $f > $f.cengyan || true; "
                + "cat $f.cengyan > $f; rm -f $f.cengyan", SHORT);
    }

    // ---- JDK ----
    /** 已安装的 JDK 来自同一个压缩包时跳过。返回压缩包摘要。 */
    static String installJdk(RemoteShell shell, java.nio.file.Path archive, String sha) throws IOException {
        var current = shell.exec("cat " + JDK + "/.cengyan-sha 2>/dev/null && " + JDK + "/bin/java -version 2>&1 | head -n1", SHORT);
        if (current.ok() && current.stdout().strip().startsWith(sha)) return sha;
        String staged = STAGING + "/jdk-" + sha.substring(0, 12) + ".tar.gz";
        String unpacked = JDK + ".new";
        shell.run("mkdir -p " + STAGING, SHORT);
        shell.upload(archive, staged, "600");
        shell.run("set -e; rm -rf " + unpacked + "; mkdir -p " + unpacked
                + "; tar -xzf " + staged + " -C " + unpacked + " --strip-components=1"
                + "; " + unpacked + "/bin/java -version 2>&1 | grep -q 'version \"21' || { echo 'JDK 压缩包不是 Java 21' >&2; exit 1; }"
                + "; echo " + sha + " > " + unpacked + "/.cengyan-sha"
                + "; rm -rf " + JDK + ".old; if [ -d " + JDK + " ]; then mv " + JDK + " " + JDK + ".old; fi"
                + "; mv " + unpacked + " " + JDK + "; rm -rf " + JDK + ".old " + staged, LONG);
        return sha;
    }

    // ---- release ----
    static String releaseDir(String releaseId) { return RELEASES + "/" + releaseId; }

    /** 逐文件比对 SHA-256，一致则跳过上传；全部到位后写完成标记。 */
    static void syncRelease(RemoteShell shell, Bundle bundle) throws IOException {
        String release = bundle.releaseId();
        String dir = releaseDir(release);
        if (shell.exec("[ \"$(cat " + dir + "/.complete 2>/dev/null)\" = " + quote(release) + " ]", SHORT).ok()) return;
        for (String name : Bundle.RELEASE_FILES) {
            var local = bundle.releaseFile(name);
            String sha = Bundle.sha256(local);
            String remote = dir + "/" + name;
            var existing = shell.exec("sha256sum " + quote(remote) + " 2>/dev/null | cut -d' ' -f1", SHORT);
            if (!existing.stdout().strip().equals(sha)) shell.upload(local, remote, "644");
            if (!shell.run("sha256sum " + quote(remote) + " | cut -d' ' -f1", SHORT).strip().equals(sha))
                throw new ClusterError("REMOTE_FAILED", "上传后校验失败：" + remote);
        }
        shell.write(dir + "/.complete", release + "\n", "644");
    }

    /** 删除除 keep 以外的程序包目录。 */
    static void pruneReleases(RemoteShell shell, Set<String> keep) throws IOException {
        StringBuilder script = new StringBuilder("cd " + RELEASES + " 2>/dev/null || exit 0; for d in *; do [ -d \"$d\" ] || continue; case \"$d\" in");
        for (String id : keep) script.append(" ").append(quote(id)).append(")").append(" continue;;");
        script.append(" esac; rm -rf -- \"$d\"; done");
        shell.run(script.toString(), SHORT);
    }

    // ---- nodes ----
    static String nodeDir(String nodeId) { return NODES + "/" + nodeId; }
    static String unit(String nodeId) { return "cengyan-" + nodeId + ".service"; }
    static String unitFile(String nodeId) { return "/etc/systemd/system/" + unit(nodeId); }
    static String propertiesFile(ClusterState.NodeSpec node) { return nodeDir(node.id()) + "/" + node.role().name().toLowerCase(Locale.ROOT) + ".properties"; }

    /** 端口已被其他进程监听，或数据目录非空时拒绝安装。 */
    static void checkFresh(RemoteShell shell, ClusterState.NodeSpec node) throws IOException {
        String listening = shell.run("(ss -ltnH 2>/dev/null || ss -ltn 2>/dev/null | tail -n +2) | awk '{print $4}' | sed 's/.*://' | sort -u", SHORT);
        Set<String> used = new HashSet<>(Arrays.asList(listening.split("\\s+")));
        for (int port : node.ports()) if (used.contains(Integer.toString(port)))
            throw new ClusterError("PORT_IN_USE", "服务器上端口 " + port + " 已被占用");
        if (shell.exec("[ -d " + quote(node.dataDir()) + " ] && [ -n \"$(ls -A " + quote(node.dataDir()) + " 2>/dev/null)\" ]", SHORT).ok())
            throw new ClusterError("DATA_DIR_NOT_EMPTY", "数据目录 " + node.dataDir() + " 已存在且不为空，请换一个目录或先清空");
    }

    /** 写入配置、node.env 和 systemd 单元。不启动、不重启。 */
    static void writeNode(RemoteShell shell, ClusterState.NodeSpec node, ConfigRenderer.Rendered rendered) throws IOException {
        String dir = nodeDir(node.id());
        shell.run("mkdir -p " + quote(dir + "/tmp") + " " + quote(node.dataDir()) + "; chmod 700 " + quote(dir), SHORT);
        shell.write(propertiesFile(node), rendered.properties(), "600");
        shell.write(dir + "/node.env", rendered.environment(), "600");
        shell.write(unitFile(node.id()), rendered.unit(), "644");
        shell.run("systemctl daemon-reload", SHORT);
    }

    static void start(RemoteShell shell, String nodeId) throws IOException {
        shell.run("systemctl enable --now " + unit(nodeId), Duration.ofMinutes(2));
    }

    static void restart(RemoteShell shell, String nodeId) throws IOException {
        shell.run("systemctl restart " + unit(nodeId), Duration.ofMinutes(3));
    }

    static boolean active(RemoteShell shell, String nodeId) throws IOException {
        return shell.exec("systemctl is-active --quiet " + unit(nodeId), SHORT).ok();
    }

    /** systemd 视角的服务状态。NRestarts 在 systemd 235 以前不存在，此时为 -1。 */
    record ServiceState(String active, String sub, int restarts) {
        boolean failed() { return "failed".equals(active); }
        boolean restarting() { return "auto-restart".equals(sub); }
    }

    static ServiceState serviceState(RemoteShell shell, String nodeId) throws IOException {
        Map<String, String> values = new HashMap<>();
        for (String line : shell.exec("systemctl show " + unit(nodeId) + " -p ActiveState -p SubState -p NRestarts", SHORT).stdout().split("\n")) {
            int eq = line.indexOf('=');
            if (eq > 0) values.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
        }
        int restarts;
        try { restarts = Integer.parseInt(values.getOrDefault("NRestarts", "-1")); } catch (NumberFormatException ex) { restarts = -1; }
        return new ServiceState(values.getOrDefault("ActiveState", ""), values.getOrDefault("SubState", ""), restarts);
    }

    static String logs(RemoteShell shell, String nodeId, int lines) throws IOException {
        return shell.exec("journalctl -u " + unit(nodeId) + " -n " + Math.max(1, Math.min(lines, 2000)) + " --no-pager 2>&1", SHORT).output();
    }

    /** 停止并删除服务和节点目录；deleteData 为真时同时删除数据目录。 */
    static void removeNode(RemoteShell shell, ClusterState.NodeSpec node, boolean deleteData) throws IOException {
        String unit = unit(node.id());
        StringBuilder script = new StringBuilder("systemctl disable --now " + unit + " 2>/dev/null || true; rm -f " + unitFile(node.id())
                + "; systemctl daemon-reload; rm -rf -- " + quote(nodeDir(node.id())));
        if (deleteData) {
            if (!safeDataDir(node.dataDir())) throw new ClusterError("INVALID_ARGUMENT", "拒绝删除数据目录：" + node.dataDir());
            script.append("; rm -rf -- ").append(quote(node.dataDir()));
        }
        shell.run(script.toString(), Duration.ofMinutes(5));
    }

    /** 清理服务器上的 cengyan 程序；/opt/cengyan/data 下仍有数据时保留该目录。 */
    static void cleanServer(RemoteShell shell) throws IOException {
        shell.run("for u in /etc/systemd/system/cengyan-*.service; do [ -e \"$u\" ] || continue; systemctl disable --now \"$(basename \"$u\")\" 2>/dev/null || true; rm -f \"$u\"; done; "
                + "systemctl daemon-reload; rm -rf -- " + JDK + " " + JDK + ".new " + JDK + ".old " + RELEASES + " " + NODES + " " + STAGING + "; "
                + "rmdir " + DATA + " 2>/dev/null; rmdir " + BASE + " 2>/dev/null; true", Duration.ofMinutes(5));
    }

    /** 绝对路径、至少两级、不含 .. 且不是系统目录。 */
    static boolean safeDataDir(String path) {
        if (path == null || !path.startsWith("/") || path.contains("..") || path.contains("//") || !path.matches("[A-Za-z0-9_./-]+")) return false;
        String normalized = path.replaceAll("/+$", "");
        if (normalized.split("/").length < 3) return false;
        for (String reserved : List.of("/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc", "/root", "/run", "/sbin", "/sys", "/usr", "/var/lib/rpm",
                JDK, RELEASES, NODES, STAGING))
            if (normalized.equals(reserved) || normalized.startsWith(reserved + "/")) return false;
        return true;
    }
}
