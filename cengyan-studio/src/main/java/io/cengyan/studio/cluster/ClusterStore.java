package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Studio 自身状态的落盘：{@code data/state.json} 和配置历史 {@code data/config-history/v<N>.json}。
 * 所有修改经 {@link #update} 串行执行，先写临时文件再原子改名，文件只有 Studio 进程可读。
 */
public final class ClusterStore {
    static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .enable(SerializationFeature.INDENT_OUTPUT);
    /**
     * 一行一个 JSON 对象的协议（Compute 客户端端口、Studio 转发的查询）。绝不能缩进：
     * 对方按行读取，多行请求只会收到第一个 "{"。
     */
    public static final ObjectMapper LINE = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Path dataDir;
    private final Path stateFile;
    private final Path historyDir;
    private ClusterState state;

    public ClusterStore(Path dataDir) throws IOException {
        this.dataDir = dataDir.toAbsolutePath().normalize();
        Files.createDirectories(this.dataDir);
        restrict(this.dataDir, true);
        stateFile = this.dataDir.resolve("state.json");
        historyDir = this.dataDir.resolve("config-history");
        Files.createDirectories(historyDir);
        if (Files.exists(stateFile)) state = JSON.readValue(stateFile.toFile(), ClusterState.class);
        else {
            state = ClusterState.empty("cengyan-" + UUID.randomUUID().toString().substring(0, 8));
            write(stateFile, state);
        }
    }

    public Path dataDir() { return dataDir; }

    public synchronized ClusterState get() { return state; }

    /** 在锁内计算新状态并落盘；函数抛出的异常原样传出，状态不变。 */
    public synchronized ClusterState update(UnaryOperator<ClusterState> change) throws IOException {
        ClusterState next = change.apply(state);
        if (next == state) return state;
        if (next.config().version() != state.config().version()) {
            write(historyDir.resolve("v" + next.config().version() + ".json"), next.config());
        }
        write(stateFile, next);
        state = next;
        return next;
    }

    public synchronized List<ClusterState.ConfigVersion> history() throws IOException {
        List<ClusterState.ConfigVersion> result = new ArrayList<>();
        try (var files = Files.list(historyDir)) {
            for (Path file : files.filter(f -> f.getFileName().toString().matches("v\\d+\\.json")).toList()) {
                result.add(JSON.readValue(file.toFile(), ClusterState.ConfigVersion.class));
            }
        }
        result.sort((a, b) -> Integer.compare(b.version(), a.version()));
        return result;
    }

    public synchronized ClusterState.ConfigVersion version(int version) throws IOException {
        if (version == state.config().version()) return state.config();
        if (version == 0) return ClusterState.ConfigVersion.initial();
        Path file = historyDir.resolve("v" + version + ".json");
        if (!Files.exists(file)) throw new IllegalArgumentException("配置版本不存在：" + version);
        return JSON.readValue(file.toFile(), ClusterState.ConfigVersion.class);
    }

    static void write(Path target, Object value) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(temporary, JSON.writeValueAsBytes(value));
        restrict(temporary, false);
        // Windows may briefly deny replacing a file another process (indexer, antivirus) has open.
        for (int attempt = 0; ; attempt++) {
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (AccessDeniedException ex) {
                if (attempt >= 10) throw ex;
                try { Thread.sleep(20L * (attempt + 1)); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw ex; }
            }
        }
    }

    /** POSIX 系统上把权限收紧到 700/600；Windows 开发机上忽略。 */
    static void restrict(Path path, boolean directory) {
        try { Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(directory ? "rwx------" : "rw-------")); }
        catch (UnsupportedOperationException | IOException ignored) { }
    }
}
