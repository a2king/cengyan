package io.cengyan.studio.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Studio 发布包里随附的节点程序和离线 JDK：
 * {@code nodes/cengyan-storage.jar}、{@code nodes/cengyan-compute.jar}、{@code nodes/logback.xml}、
 * {@code jdk/jdk-21-linux-x64.tar.gz}、{@code jdk/jdk-21-linux-aarch64.tar.gz}。
 */
public final class Bundle {
    public static final List<String> RELEASE_FILES = List.of("cengyan-storage.jar", "cengyan-compute.jar", "logback.xml");
    private static final Map<String, String> JDKS = Map.of("x86_64", "jdk-21-linux-x64.tar.gz", "aarch64", "jdk-21-linux-aarch64.tar.gz");
    private final Path home;
    private volatile String releaseId;

    public Bundle(Path home) { this.home = home.toAbsolutePath().normalize(); }

    public Path home() { return home; }

    public String version() {
        Path info = home.resolve("BUILD-INFO.json");
        try {
            if (Files.exists(info)) return ClusterStore.JSON.readTree(info.toFile()).path("version").asText("dev");
        } catch (IOException ignored) { }
        return "dev";
    }

    /** 版本号加 jar 内容摘要：同一版本号重新构建后也能识别为新程序包。 */
    public String releaseId() throws IOException {
        String cached = releaseId;
        if (cached != null) return cached;
        MessageDigest digest = sha();
        for (String name : RELEASE_FILES) digest.update(HexFormat.of().parseHex(sha256(releaseFile(name))));
        cached = version() + "-" + HexFormat.of().formatHex(digest.digest()).substring(0, 10);
        releaseId = cached;
        return cached;
    }

    public Path releaseFile(String name) throws IOException {
        Path file = home.resolve("nodes").resolve(name);
        if (!Files.isRegularFile(file)) throw new ClusterError("BUNDLE_MISSING", "Studio 发布包缺少节点程序：" + file + "。请使用完整发布包启动 Studio");
        return file;
    }

    public Path jdk(String arch) throws IOException {
        String name = JDKS.get(arch);
        if (name == null) throw new ClusterError("UNSUPPORTED_ARCH", "不支持的 CPU 架构：" + arch);
        Path file = home.resolve("jdk").resolve(name);
        if (!Files.isRegularFile(file)) throw new ClusterError("BUNDLE_MISSING", "Studio 发布包缺少离线 JDK：" + file);
        return file;
    }

    public static String sha256(Path file) throws IOException {
        MessageDigest digest = sha();
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[1 << 16];
            for (int read; (read = input.read(buffer)) > 0; ) digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static MessageDigest sha() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (Exception ex) { throw new IllegalStateException(ex); }
    }
}
