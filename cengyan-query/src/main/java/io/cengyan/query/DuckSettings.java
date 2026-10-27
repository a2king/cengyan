package io.cengyan.query;

import java.nio.file.Path;
import java.util.function.Consumer;

/** DuckDB 的内存上限和临时目录。查询结束前可以看一眼临时文件。 */
final class DuckSettings {
    private final String memoryLimit;
    private final Path tempDirectory;
    private final Consumer<Path> beforeClose;

    DuckSettings(String memoryLimit, Path tempDirectory) {
        this(memoryLimit, tempDirectory, null);
    }

    DuckSettings(String memoryLimit, Path tempDirectory, Consumer<Path> beforeClose) {
        if (memoryLimit == null || memoryLimit.isBlank()) {
            throw new IllegalArgumentException("DuckDB 内存上限不能为空");
        }
        if (tempDirectory == null) {
            throw new IllegalArgumentException("DuckDB 临时目录不能为空");
        }
        this.memoryLimit = memoryLimit.trim();
        this.tempDirectory = tempDirectory;
        this.beforeClose = beforeClose;
    }

    String memoryLimit() {
        return memoryLimit;
    }

    Path tempDirectory() {
        return tempDirectory;
    }

    Consumer<Path> beforeClose() {
        return beforeClose;
    }
}
