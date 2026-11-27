package io.cengyan.query;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** 按字节限制初始排序段，按固定路数进行多轮归并。 */
final class SpillSorter<T> {
    private static final int FAN_IN = 8;
    interface Codec<T> {
        T read(DataInputStream in) throws IOException;
        void write(DataOutputStream out, T value) throws IOException;
    }

    @FunctionalInterface
    interface Visitor<T> {
        void accept(T row) throws IOException;
    }

    private record Head<T>(DataInputStream stream, T value) {}

    private final SpillWorkspace workspace;
    private final Codec<T> codec;
    private final Comparator<T> order;
    private final long bufferBytes;
    private final Runnable check;
    private final List<T> buffer = new ArrayList<>();
    private List<Path> runs = new ArrayList<>();
    private long bytes;

    SpillSorter(SpillWorkspace workspace, Codec<T> codec, Comparator<T> order, long bufferBytes, Runnable check) {
        this.workspace = workspace;
        this.codec = codec;
        this.order = order;
        this.bufferBytes = bufferBytes;
        this.check = check;
    }

    void add(T value, long size) throws IOException {
        if (size > bufferBytes) throw new io.cengyan.distributed.ClusterException(
                "QUERY_MEMORY_EXCEEDED", "单个排序项超过查询排序缓冲预算");
        if (size > bufferBytes - bytes || buffer.size() >= 8192) flush();
        buffer.add(value);
        bytes += size;
    }

    void visit(Visitor<T> visitor) throws IOException {
        if (runs.isEmpty()) {
            buffer.sort(order);
            for (T value : buffer) { check.run(); visitor.accept(value); }
            buffer.clear();
            return;
        }
        flush();
        while (runs.size() > FAN_IN) {
            var next = new ArrayList<Path>();
            for (int offset = 0; offset < runs.size(); offset += FAN_IN) {
                var batch = runs.subList(offset, Math.min(offset + FAN_IN, runs.size()));
                Path target = workspace.file();
                try (var out = workspace.output(target)) {
                    merge(batch, item -> codec.write(out, item));
                }
                for (Path path : batch) workspace.delete(path);
                next.add(target);
            }
            runs = next;
        }
        merge(runs, visitor);
        for (Path path : runs) workspace.delete(path);
        runs.clear();
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) return;
        check.run();
        buffer.sort(order);
        Path path = workspace.file();
        try (var out = workspace.output(path)) {
            for (T value : buffer) codec.write(out, value);
        }
        runs.add(path);
        buffer.clear();
        bytes = 0;
    }

    private void merge(List<Path> paths, Visitor<T> visitor) throws IOException {
        var streams = new ArrayList<DataInputStream>();
        var queue = new PriorityQueue<Head<T>>((a, b) -> order.compare(a.value(), b.value()));
        try {
            for (Path path : paths) {
                var stream = workspace.input(path);
                streams.add(stream);
                advance(stream, queue);
            }
            while (!queue.isEmpty()) {
                check.run();
                var head = queue.remove();
                visitor.accept(head.value());
                advance(head.stream(), queue);
            }
        } finally {
            for (var stream : streams) stream.close();
        }
    }

    private void advance(DataInputStream stream, PriorityQueue<Head<T>> queue) throws IOException {
        // 文件是本查询生成的完整段。首字节之外的 EOF 属于文件损坏。
        if (stream.available() > 0) queue.add(new Head<>(stream, codec.read(stream)));
    }
}
