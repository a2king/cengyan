package io.cengyan.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** 把一次性能测试追加到 docs/perf-report.md。 */
public final class PerfReport {
    private static final String HEADER = """
            # 性能测试记录

            | 时间 | 场景 | 机器 | 数据量 | 热缓存 | 块缓存 | 堆 | p50 | p99 |
            |---|---|---|---|---|---|---|---|---|
            """;

    private PerfReport() {
    }

    public static long percentile(long[] nanos, double ratio) {
        long[] copy = Arrays.copyOf(nanos, nanos.length);
        Arrays.sort(copy);
        int index = (int) Math.ceil(ratio * copy.length) - 1;
        if (index < 0) {
            index = 0;
        }
        if (index >= copy.length) {
            index = copy.length - 1;
        }
        return copy[index];
    }

    public static void append(
            String scenario,
            String volume,
            boolean hotCache,
            long blockCacheBytes,
            long p50Nanos,
            long p99Nanos) {
        String property = System.getProperty("cengyan.report", "docs/perf-report.md");
        Path path = Path.of(property);
        String row = "| " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                + " | " + sanitize(scenario)
                + " | " + sanitize(machine())
                + " | " + sanitize(volume)
                + " | " + (hotCache ? "是" : "否")
                + " | " + megabytes(blockCacheBytes)
                + " | " + megabytes(Runtime.getRuntime().maxMemory())
                + " | " + millis(p50Nanos)
                + " | " + millis(p99Nanos)
                + " |\n";
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
                 FileChannel channel = file.getChannel();
                 FileLock ignored = channel.lock()) {
                if (file.length() == 0) {
                    file.write(HEADER.getBytes(StandardCharsets.UTF_8));
                }
                file.seek(file.length());
                file.write(row.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new IllegalStateException("无法写入性能报告：" + ex.getMessage(), ex);
        }
    }

    private static volatile String cachedMachine;

    public static String machine() {
        String cached = cachedMachine;
        if (cached != null) {
            return cached;
        }
        synchronized (PerfReport.class) {
            if (cachedMachine == null) {
                cachedMachine = detectMachine();
            }
            return cachedMachine;
        }
    }

    private static String detectMachine() {
        try {
            String model = regValue("HKLM\\HARDWARE\\DESCRIPTION\\System\\BIOS", "SystemProductName");
            String cpu = regValue("HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", "ProcessorNameString");
            if (!model.isBlank() && !cpu.isBlank()) {
                return model + " / " + cpu;
            }
        } catch (Exception ignored) {
            // 读不到注册表时退回操作系统信息
        }
        String cpu = System.getenv("PROCESSOR_IDENTIFIER");
        if (cpu == null || cpu.isBlank()) {
            return System.getProperty("os.name");
        }
        return System.getProperty("os.name") + " / " + cpu;
    }

    private static String regValue(String key, String name) throws Exception {
        Process process = new ProcessBuilder("reg", "query", key, "/v", name)
                .redirectErrorStream(true)
                .start();
        byte[] bytes = process.getInputStream().readAllBytes();
        boolean finished = process.waitFor(3, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IllegalStateException("读取注册表超时");
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("读取注册表失败");
        }
        String text = new String(bytes, Charset.defaultCharset());
        for (String line : text.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.regionMatches(true, 0, name, 0, name.length())) {
                continue;
            }
            String[] parts = trimmed.split("\\s{2,}");
            if (parts.length >= 3) {
                return parts[parts.length - 1].trim();
            }
        }
        throw new IllegalStateException("注册表里没有 " + name);
    }

    private static String millis(long nanos) {
        return String.format("%.3f ms", nanos / 1_000_000.0);
    }

    private static String megabytes(long bytes) {
        return (bytes / (1024 * 1024)) + " MB";
    }

    private static String sanitize(String text) {
        return text.replace('|', '/').replace('\n', ' ');
    }
}
