package io.cengyan.server;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** 启动时读完的配置。运行中不热更新。 */
public record ServerConfig(
        int port,
        Path dataDir,
        int queryThreads,
        long maxExecutionTimeMs,
        int maxEdgesPerVertex,
        int maxFrontierPerHop,
        int maxHops,
        int maxPaths,
        long queryMemoryBytes,
        long blockCacheBytes,
        String duckdbMemory,
        Path duckdbTempDirectory,
        double minEdgeRatio,
        double minAccumRatio,
        String rootPassword) {

    public ServerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port 超出范围");
        }
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir 不能为空");
        }
        if (queryThreads <= 0) {
            throw new IllegalArgumentException("queryThreads 必须为正");
        }
        if (maxExecutionTimeMs <= 0) {
            throw new IllegalArgumentException("maxExecutionTimeMs 必须为正");
        }
        if (maxEdgesPerVertex <= 0 || maxFrontierPerHop <= 0 || maxHops <= 0 || maxPaths <= 0) {
            throw new IllegalArgumentException("查询上限必须为正");
        }
        if (queryMemoryBytes <= 0 || blockCacheBytes <= 0) {
            throw new IllegalArgumentException("内存配置必须为正");
        }
        if (duckdbMemory == null || duckdbMemory.isBlank()) {
            throw new IllegalArgumentException("duckdbMemory 不能为空");
        }
        if (duckdbTempDirectory == null) {
            throw new IllegalArgumentException("duckdbTempDirectory 不能为空");
        }
        if (rootPassword == null) {
            throw new IllegalArgumentException("rootPassword 不能为空");
        }
    }

    public static ServerConfig defaults() {
        return from(new Properties());
    }

    public static ServerConfig load(Path file) {
        Properties properties = new Properties();
        if (file != null && Files.exists(file)) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                properties.load(reader);
            } catch (IOException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "无法读取配置：" + ex.getMessage(), ex);
            }
        }
        return from(properties);
    }

    /** 测试用：随机端口、独立数据目录、小块缓存。 */
    public ServerConfig forTest(Path dataDir) {
        return new ServerConfig(
                0,
                dataDir,
                2,
                maxExecutionTimeMs,
                maxEdgesPerVertex,
                maxFrontierPerHop,
                maxHops,
                maxPaths,
                queryMemoryBytes,
                8L * 1024 * 1024,
                duckdbMemory,
                duckdbTempDirectory,
                minEdgeRatio,
                minAccumRatio,
                rootPassword);
    }

    private static ServerConfig from(Properties properties) {
        int threads = integer(properties, "query_threads", Runtime.getRuntime().availableProcessors());
        return new ServerConfig(
                integer(properties, "port", 19669),
                Path.of(properties.getProperty("data_dir", "data")),
                Math.max(threads, 1),
                Long.parseLong(properties.getProperty("max_execution_time_ms", "1000").trim()),
                integer(properties, "max_edges_per_vertex", 64),
                integer(properties, "max_frontier_per_hop", 64),
                integer(properties, "max_hops", 10),
                integer(properties, "max_paths", 100),
                longValue(properties, "query_memory_bytes", 67108864L),
                longValue(properties, "block_cache_bytes", 268435456L),
                properties.getProperty("duckdb_memory", "1GB").trim(),
                Path.of(properties.getProperty("duckdb_temp_directory", "tmp/duck")),
                doubleValue(properties, "min_edge_ratio", 0.01),
                doubleValue(properties, "min_accum_ratio", 0.001),
                properties.getProperty("root_password", "cengyan"));
    }

    private static int integer(Properties properties, String key, int fallback) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "配置项 " + key + " 不是整数");
        }
    }

    private static long longValue(Properties properties, String key, long fallback) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "配置项 " + key + " 不是整数");
        }
    }

    private static double doubleValue(Properties properties, String key, double fallback) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "配置项 " + key + " 不是数字");
        }
    }
}
