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
        String host,
        int port,
        int httpPort,
        boolean httpTlsEnabled,
        Path httpTlsKeystore,
        String httpTlsPassword,
        Path dataDir,
        int queryThreads,
        int interactiveReserved,
        int heavyQueryThreads,
        long queryQueueMs,
        long maxExecutionTimeMs,
        long heavyMaxExecutionTimeMs,
        int maxEdgesPerVertex,
        int maxFrontierPerHop,
        int maxHops,
        int maxPaths,
        long queryMemoryBytes,
        long blockCacheBytes,
        int rocksdbBackgroundJobs,
        long rocksdbWriteBufferBytes,
        int cacheCompactIntervalMinutes,
        String duckdbMemory,
        Path duckdbTempDirectory,
        String rootPassword) {

    public ServerConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host 不能为空");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port 超出范围");
        }
        if (httpPort < 0 || httpPort > 65535) {
            throw new IllegalArgumentException("httpPort 超出范围");
        }
        if (httpTlsKeystore == null || httpTlsPassword == null) {
            throw new IllegalArgumentException("HTTP TLS 配置不能为空");
        }
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir 不能为空");
        }
        if (queryThreads <= 0) {
            throw new IllegalArgumentException("queryThreads 必须为正");
        }
        if (interactiveReserved < 0 || interactiveReserved >= queryThreads) {
            throw new IllegalArgumentException("interactiveReserved 必须小于 queryThreads");
        }
        if (heavyQueryThreads <= 0) {
            throw new IllegalArgumentException("heavyQueryThreads 必须为正");
        }
        if (queryQueueMs < 0) {
            throw new IllegalArgumentException("queryQueueMs 不能为负");
        }
        if (maxExecutionTimeMs < 0) {
            throw new IllegalArgumentException("maxExecutionTimeMs 不能为负，0 表示不限");
        }
        if (heavyMaxExecutionTimeMs < 0) {
            throw new IllegalArgumentException("heavyMaxExecutionTimeMs 不能为负，0 表示不限");
        }
        if (maxEdgesPerVertex < 0 || maxFrontierPerHop < 0 || maxHops < 0 || maxPaths < 0) {
            throw new IllegalArgumentException("查询预算不能为负，0 表示不限");
        }
        if (queryMemoryBytes <= 0 || blockCacheBytes <= 0
                || rocksdbBackgroundJobs <= 0 || rocksdbWriteBufferBytes <= 0) {
            throw new IllegalArgumentException("内存配置必须为正");
        }
        if (cacheCompactIntervalMinutes < 0) {
            throw new IllegalArgumentException("cacheCompactIntervalMinutes 不能为负，0 表示关闭");
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
                host,
                0,
                0,
                false,
                Path.of("config/server.p12"),
                "",
                dataDir,
                2,
                0,
                1,
                queryQueueMs,
                maxExecutionTimeMs,
                heavyMaxExecutionTimeMs,
                maxEdgesPerVertex,
                maxFrontierPerHop,
                maxHops,
                maxPaths,
                queryMemoryBytes,
                8L * 1024 * 1024,
                rocksdbBackgroundJobs,
                rocksdbWriteBufferBytes,
                cacheCompactIntervalMinutes,
                duckdbMemory,
                duckdbTempDirectory,
                rootPassword);
    }

    private static ServerConfig from(Properties properties) {
        int threads = Math.max(integer(properties, "query_threads", Runtime.getRuntime().availableProcessors()), 1);
        return new ServerConfig(
                text(properties, "host", "0.0.0.0"),
                integer(properties, "port", 19669),
                integer(properties, "http_port", 19671),
                Boolean.parseBoolean(properties.getProperty("http_tls_enabled", "false").trim()),
                Path.of(properties.getProperty("http_tls_keystore", "config/server.p12")),
                properties.getProperty("http_tls_password", ""),
                Path.of(properties.getProperty("data_dir", "data")),
                threads,
                integer(properties, "interactive_reserved", Math.min(2, threads - 1)),
                integer(properties, "heavy_query_threads", 2),
                longValue(properties, "query_queue_ms", 30_000L),
                Long.parseLong(properties.getProperty("max_execution_time_ms", "0").trim()),
                longValue(properties, "heavy_max_execution_time_ms", 0L),
                integer(properties, "max_edges_per_vertex", 0),
                integer(properties, "max_frontier_per_hop", 0),
                integer(properties, "max_hops", 0),
                integer(properties, "max_paths", 0),
                longValue(properties, "query_memory_bytes", 67108864L),
                longValue(properties, "block_cache_bytes", 268435456L),
                integer(properties, "rocksdb_background_jobs", 6),
                longValue(properties, "rocksdb_write_buffer_bytes", 134217728L),
                integer(properties, "cache_compact_interval_minutes", 30),
                properties.getProperty("duckdb_memory", "1GB").trim(),
                Path.of(properties.getProperty("duckdb_temp_directory", "tmp/duck")),
                properties.getProperty("root_password", "cengyan"));
    }

    private static String text(Properties properties, String key, String fallback) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String value = raw.trim();
        if (value.length() >= 2
                && ((value.startsWith("'") && value.endsWith("'"))
                || (value.startsWith("\"") && value.endsWith("\"")))) {
            value = value.substring(1, value.length() - 1).trim();
        }
        return value.isEmpty() ? fallback : value;
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

}
