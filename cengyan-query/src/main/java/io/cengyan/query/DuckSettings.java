package io.cengyan.query;

import io.cengyan.CengyanException;

import java.nio.file.Path;
import java.util.function.Consumer;

/** DuckDB 的内存上限和临时目录。查询结束前可以看一眼临时文件。 */
final class DuckSettings {
    private final String memoryLimit;
    private final Path tempDirectory;
    private final String maxTempDirectorySize;
    private final int threads;
    private final Consumer<Path> beforeClose;

    DuckSettings(String memoryLimit, Path tempDirectory) {
        this(memoryLimit, tempDirectory, "100GB", 1, null);
    }

    DuckSettings(String memoryLimit, Path tempDirectory, Consumer<Path> beforeClose) {
        this(memoryLimit, tempDirectory, "100GB", 1, beforeClose);
    }

    DuckSettings(String memoryLimit, Path tempDirectory, String maxTempDirectorySize, int threads) {
        this(memoryLimit, tempDirectory, maxTempDirectorySize, threads, null);
    }

    DuckSettings(
            String memoryLimit,
            Path tempDirectory,
            String maxTempDirectorySize,
            int threads,
            Consumer<Path> beforeClose) {
        if (memoryLimit == null || memoryLimit.isBlank()) {
            throw new IllegalArgumentException("DuckDB 内存上限不能为空");
        }
        if (tempDirectory == null) {
            throw new IllegalArgumentException("DuckDB 临时目录不能为空");
        }
        if (maxTempDirectorySize == null || maxTempDirectorySize.isBlank()) {
            throw new IllegalArgumentException("DuckDB 临时目录上限不能为空");
        }
        if (threads <= 0) {
            throw new IllegalArgumentException("DuckDB 线程数必须为正");
        }
        this.memoryLimit = memoryLimit.trim();
        this.tempDirectory = tempDirectory;
        this.maxTempDirectorySize = maxTempDirectorySize.trim();
        this.threads = threads;
        this.beforeClose = beforeClose;
    }

    String memoryLimit() {
        return memoryLimit;
    }

    Path tempDirectory() {
        return tempDirectory;
    }

    DuckSettings forCurrentQuery() {
        long queryBytes = QueryMemory.current().limit();
        if (queryBytes <= 0) {
            return this;
        }
        long configuredBytes = memoryBytes(memoryLimit);
        long effective = Math.min(queryBytes, configuredBytes);
        return new DuckSettings(formatBytes(effective), tempDirectory, maxTempDirectorySize, threads, beforeClose);
    }

    Consumer<Path> beforeClose() {
        return beforeClose;
    }

    String maxTempDirectorySize() {
        return maxTempDirectorySize;
    }

    int threads() {
        return threads;
    }

    private static long memoryBytes(String value) {
        String raw = value.trim().toUpperCase(java.util.Locale.ROOT);
        long multiplier = 1L;
        if (raw.endsWith("KB")) {
            multiplier = 1024L;
            raw = raw.substring(0, raw.length() - 2);
        } else if (raw.endsWith("MB")) {
            multiplier = 1024L * 1024L;
            raw = raw.substring(0, raw.length() - 2);
        } else if (raw.endsWith("GB")) {
            multiplier = 1024L * 1024L * 1024L;
            raw = raw.substring(0, raw.length() - 2);
        } else if (raw.endsWith("TB")) {
            multiplier = 1024L * 1024L * 1024L * 1024L;
            raw = raw.substring(0, raw.length() - 2);
        }
        try {
            return Math.multiplyExact(Long.parseLong(raw), multiplier);
        } catch (NumberFormatException | ArithmeticException ex) {
            throw new CengyanException(io.cengyan.ErrorCode.INTERNAL, "DuckDB 内存上限格式不正确");
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes % (1024L * 1024L * 1024L) == 0) {
            return (bytes / (1024L * 1024L * 1024L)) + "GB";
        }
        if (bytes % (1024L * 1024L) == 0) {
            return (bytes / (1024L * 1024L)) + "MB";
        }
        if (bytes % 1024L == 0) {
            return (bytes / 1024L) + "KB";
        }
        return bytes + "B";
    }
}
