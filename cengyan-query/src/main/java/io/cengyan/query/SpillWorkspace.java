package io.cengyan.query;

import io.cengyan.distributed.ClusterException;

import java.io.*;
import java.nio.file.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/** 临时文件的字节数在写入前计入同一查询的磁盘预算。 */
final class SpillWorkspace implements AutoCloseable {
    private static final int BUFFER = 4096;
    private final Path directory;
    private final long limit;
    private long used;
    private int sequence;
    private final Map<Path, Long> allocations = new HashMap<>();

    SpillWorkspace(Path parent, long limit) throws IOException {
        Files.createDirectories(parent);
        directory = Files.createTempDirectory(parent, "spill-");
        this.limit = limit;
    }

    Path file() {
        Path path = directory.resolve(Integer.toString(sequence++));
        allocations.put(path, 0L);
        return path;
    }

    private void reserve(Path path, int bytes) {
        if (bytes > limit - used) throw new ClusterException("QUERY_TEMP_LIMIT_EXCEEDED", "查询临时盘预算为 " + limit + " 字节");
        used += bytes;
        allocations.compute(path, (key, size) -> Math.addExact(size, bytes));
    }

    DataOutputStream output(Path path) throws IOException {
        var file = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
        return new DataOutputStream(new Buffer(file, path));
    }

    private final class Buffer extends BufferedOutputStream {
        private final Path path;

        private Buffer(OutputStream file, Path path) { super(file, BUFFER); this.path = path; }

        @Override public synchronized void write(int value) throws IOException {
            reserve(path, 1);
            super.write(value);
        }

        @Override public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
            reserve(path, length);
            super.write(bytes, offset, length);
        }
    }

    DataInputStream input(Path path) throws IOException {
        return new DataInputStream(new BufferedInputStream(Files.newInputStream(path), BUFFER));
    }

    void delete(Path path) throws IOException {
        Files.deleteIfExists(path);
        used -= allocations.remove(path);
    }

    @Override public void close() throws IOException {
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
