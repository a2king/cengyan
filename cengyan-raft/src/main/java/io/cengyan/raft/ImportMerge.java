package io.cengyan.raft;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/** 有序 SST 的 k 路归并，以及按大小、按键类切分的输出。不读写分片库。 */
final class ImportMerge {
    /** 一次归并最多同时打开的输入数。超过时先分组归并成中间文件。 */
    static final int FAN_IN = 64;
    private static final long READAHEAD = 4L * 1024 * 1024;

    private ImportMerge() { }

    interface Sink { void put(byte[] key, byte[] value) throws IOException; }

    record Outcome(long rows, long duplicates) { }

    /**
     * 每个输入必须严格递增。同一个键出现在多个输入里时只输出一条，保留字节序最小的值：
     * 结果只取决于输入的内容，与输入的先后顺序、各副本在后台怎样分组归并都无关。
     */
    static Outcome merge(List<Path> inputs, Sink sink) throws IOException {
        List<Cursor> open = new ArrayList<>();
        PriorityQueue<Cursor> heap = new PriorityQueue<>((left, right) -> Arrays.compareUnsigned(left.key, right.key));
        long rows = 0, duplicates = 0;
        try (Options options = ImportSst.runOptions();
             ReadOptions read = new ReadOptions().setReadaheadSize(READAHEAD).setFillCache(false)) {
            for (Path input : inputs) {
                Cursor cursor = new Cursor(input, options, read);
                open.add(cursor);
                if (cursor.advance()) heap.add(cursor);
            }
            while (!heap.isEmpty()) {
                Cursor first = heap.poll();
                byte[] key = first.key, value = first.value;
                while (!heap.isEmpty() && Arrays.equals(heap.peek().key, key)) {
                    Cursor other = heap.poll();
                    if (Arrays.compareUnsigned(other.value, value) < 0) value = other.value;
                    duplicates++;
                    if (other.advance()) heap.add(other);
                }
                sink.put(key, value);
                rows++;
                if (first.advance()) heap.add(first);
            }
        } finally {
            for (Cursor cursor : open) cursor.close();
        }
        return new Outcome(rows, duplicates);
    }

    /** 把一组文件归并成一个上传段格式的文件，用于后台合并上传段和分组归并。 */
    static Outcome mergeInto(List<Path> inputs, Path output) throws IOException {
        try (Output out = new Output(output.getParent(), output.getFileName().toString(), Long.MAX_VALUE, false, false)) {
            Outcome outcome = merge(inputs, out::put);
            List<Path> files = out.finish();
            if (files.size() != 1) throw new IOException("归并结果为空");
            Files.move(files.getFirst(), output, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return outcome;
        }
    }

    private static final class Cursor implements AutoCloseable {
        private final Path path;
        private final SstFileReader reader;
        private final SstFileReaderIterator iterator;
        private boolean started;
        byte[] key, value;

        Cursor(Path path, Options options, ReadOptions read) throws IOException {
            this.path = path;
            reader = new SstFileReader(options);
            try {
                reader.open(path.toString());
                iterator = reader.newIterator(read);
                iterator.seekToFirst();
            } catch (RocksDBException ex) {
                reader.close();
                throw new IOException("无法读取导入文件 " + path.getFileName(), ex);
            }
        }

        boolean advance() throws IOException {
            if (started) iterator.next();
            started = true;
            if (!iterator.isValid()) {
                try { iterator.status(); }
                catch (RocksDBException ex) { throw new IOException("读取导入文件失败 " + path.getFileName(), ex); }
                return false;
            }
            byte[] next = iterator.key();
            if (key != null && Arrays.compareUnsigned(key, next) >= 0) throw new ImportException("INVALID_ARGUMENT", "导入文件键序无效：" + path.getFileName());
            key = next;
            value = iterator.value();
            return true;
        }

        @Override public void close() {
            iterator.close();
            reader.close();
        }
    }

    /**
     * 写出一组互不重叠的 SST。超过目标大小就换下一个文件；{@code splitKinds} 时键类（物理键第二个字节）
     * 变化也换文件，一个文件只覆盖一类键的连续区间，摄入时不会跨过其他类型的数据。
     */
    static final class Output implements AutoCloseable {
        private static final int SIZE_CHECK = 4096;
        private final Path directory;
        private final String prefix;
        private final long targetBytes;
        private final boolean splitKinds;
        private final boolean publish;
        private final List<Path> files = new ArrayList<>();
        private EnvOptions env;
        private Options options;
        private SstFileWriter writer;
        private byte kind;
        private int sinceCheck;
        private long bytes;

        Output(Path directory, String prefix, long targetBytes, boolean splitKinds, boolean publish) {
            this.directory = directory;
            this.prefix = prefix;
            this.targetBytes = targetBytes;
            this.splitKinds = splitKinds;
            this.publish = publish;
        }

        void put(byte[] key, byte[] value) throws IOException {
            try {
                byte nextKind = key.length > 1 ? key[1] : 0;
                if (writer != null) {
                    boolean cut = splitKinds && nextKind != kind;
                    if (!cut && ++sinceCheck >= SIZE_CHECK) {
                        sinceCheck = 0;
                        cut = writer.fileSize() >= targetBytes;
                    }
                    if (cut) finishFile();
                }
                if (writer == null) openFile(nextKind);
                writer.put(key, value);
            } catch (RocksDBException ex) {
                throw new IOException("写导入文件失败", ex);
            }
        }

        /** @return 已写完的文件，按键序排列 */
        List<Path> finish() throws IOException {
            try { finishFile(); }
            catch (RocksDBException ex) { throw new IOException("写导入文件失败", ex); }
            return List.copyOf(files);
        }

        long bytes() { return bytes; }

        private void openFile(byte nextKind) throws IOException, RocksDBException {
            Files.createDirectories(directory);
            env = new EnvOptions().setBytesPerSync(1024 * 1024);
            options = publish ? ImportSst.publishOptions() : ImportSst.runOptions();
            writer = new SstFileWriter(env, options);
            Path file = directory.resolve(String.format("%s-%05d.sst", prefix, files.size() + 1));
            Files.deleteIfExists(file);
            writer.open(file.toString());
            files.add(file);
            kind = nextKind;
            sinceCheck = 0;
        }

        private void finishFile() throws IOException, RocksDBException {
            if (writer == null) return;
            try {
                writer.finish();
                bytes += Files.size(files.getLast());
            } finally { release(); }
        }

        private void release() {
            if (writer != null) writer.close();
            if (options != null) options.close();
            if (env != null) env.close();
            writer = null;
            options = null;
            env = null;
        }

        /** 中途失败时丢掉没写完的文件。 */
        @Override public void close() {
            if (writer == null) return;
            release();
            try { Files.deleteIfExists(files.removeLast()); }
            catch (IOException ignored) { /* 会话结束时整个目录都会删除。 */ }
        }
    }

    /** 由导入内容决定的失败：每个副本得到同样的结论，可以作为发布结果返回。 */
    static final class ImportException extends IOException {
        final String code;
        ImportException(String code, String message) { super(message); this.code = code; }
    }
}
