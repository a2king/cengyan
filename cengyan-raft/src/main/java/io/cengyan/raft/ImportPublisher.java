package io.cengyan.raft;

import io.cengyan.protocol.EntityKeys;
import io.cengyan.protocol.proto.ClusterProtocol.BatchResult;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.ImportConflict;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPublish;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在 Raft apply 里决定一次导入发布怎样落进分片库。只读分片库，写由调用方在写锁内完成：
 * 一次 {@code ingestExternalFile} 原子地摄入全部文件，再用一个 WriteBatch 写计数、发布标记和应用位置。
 *
 * <p>类型的键区间为空时（新类型导入），文件不需要比对任何已有数据，直接摄入；RocksDB 把互不重叠的文件
 * 放到能放下的最深一层，之后不再被压实重写。点类型只额外把新点 ID 和点目录顺序比对一遍。
 *
 * <p>崩溃恢复：分片库关掉了 WAL，写进 WriteBatch 的东西在刷盘前崩溃会丢，摄入的文件不会丢。所以
 * 摄入的文件里带一个发布标记，记下这次发布新增了多少主键。重放时看到标记就跳过比对和摄入，只把计数补上。
 */
final class ImportPublisher {
    private static final byte MARKER_NAMESPACE = 4;
    private static final byte INGESTED = 0, DONE = 1;

    private ImportPublisher() { }

    /**
     * @param inserted 新增主键数，计入实体计数
     * @param ingest 要一次摄入的文件；为空时不摄入
     * @param done 这次发布早已完成，只返回结果
     */
    record Plan(BatchResult result, long inserted, List<Path> ingest, byte[] markerKey, boolean done) {
        boolean ok() { return result.getCode().equals("OK"); }
        /** WriteBatch 里写的标记：发布已经完成，计数已经算过。 */
        byte[] doneMarker() { return marker(DONE, inserted, result); }
    }

    /**
     * 发布前的检查：按本副本当前的数据比对一遍，提前发现会让发布失败的冲突（--unq error、点 ID 属于别的类型、
     * 边端点不同）。不是最终结论：真正的比对在 apply 里按日志顺序再做一次。
     */
    static ImportStaging.Prepared precheck(RocksDB db, ImportPublish publish, ImportStaging.Prepared prepared) throws IOException, RocksDBException {
        if (!prepared.ok()) return prepared;
        try { classify(db, publish, prepared); return prepared; }
        catch (ImportMerge.ImportException ex) { return ImportStaging.Prepared.failed(ex.code, ex.getMessage(), prepared.digest()); }
    }

    static Plan plan(RocksDB db, ImportStaging staging, ImportPublish publish) throws IOException, RocksDBException {
        byte[] markerKey = markerKey(publish);
        byte[] stored = db.get(markerKey);
        if (stored != null) {
            long inserted = ByteBuffer.wrap(stored, 1, 8).getLong();
            BatchResult result = BatchResult.parseFrom(Arrays.copyOfRange(stored, 9, stored.length));
            return new Plan(result, inserted, List.of(), markerKey, stored[0] == DONE);
        }
        BatchResult invalid = validate(publish);
        if (invalid != null) return new Plan(invalid, 0, List.of(), markerKey, false);
        ReentrantLock lock = staging.mergeLock(publish.getSessionId());
        lock.lock();
        try {
            ImportStaging.Prepared prepared = staging.prepare(publish);
            if (!prepared.ok()) return new Plan(failure(prepared.code(), prepared.message()), 0, List.of(), markerKey, false);
            Conflicts conflicts;
            try { conflicts = classify(db, publish, prepared); }
            catch (ImportMerge.ImportException ex) { return new Plan(failure(ex.code, ex.getMessage()), 0, List.of(), markerKey, false); }
            long inserted = prepared.primaries() - conflicts.skipped - conflicts.replaced;
            BatchResult result = BatchResult.newBuilder().setCode("OK").setAffected(inserted + conflicts.replaced)
                    .setSkipped(conflicts.skipped).setReplaced(conflicts.replaced).build();
            Path work = prepared.data().isEmpty() ? null : prepared.data().getFirst().getParent().resolve("publish");
            if (work != null) ImportStaging.deleteTree(work);
            List<Path> ingest = new ArrayList<>();
            ingest.addAll(conflicts.skipData.isEmpty() ? prepared.data() : filter(prepared.data(), conflicts.skipData, work, "data", true));
            ingest.addAll(conflicts.skipDirectory.isEmpty() ? prepared.directory()
                    : filter(prepared.directory(), conflicts.skipDirectory, work, "dir", false));
            if (!ingest.isEmpty()) {
                try (ImportMerge.Output sentinel = new ImportMerge.Output(work, "marker", Long.MAX_VALUE, false, true)) {
                    sentinel.put(markerKey, marker(INGESTED, inserted, result));
                    ingest.addAll(sentinel.finish());
                }
            }
            return new Plan(result, inserted, List.copyOf(ingest), markerKey, false);
        } finally { lock.unlock(); }
    }

    static byte[] markerKey(ImportPublish publish) {
        MessageDigest digest = ImportSst.digest();
        digest.update(("publish\0" + publish.getSessionId() + "\0" + (publish.getNode() ? "n" : "e") + publish.getTypeId()
                + "\0" + publish.getGeneration()).getBytes(StandardCharsets.UTF_8));
        byte[] hash = digest.digest();
        byte[] key = new byte[hash.length + 1];
        key[0] = MARKER_NAMESPACE;
        System.arraycopy(hash, 0, key, 1, hash.length);
        return key;
    }

    private static byte[] marker(byte state, long inserted, BatchResult result) {
        byte[] encoded = result.toByteArray();
        return ByteBuffer.allocate(9 + encoded.length).put(state).putLong(inserted).put(encoded).array();
    }

    private static BatchResult validate(ImportPublish publish) {
        try { ImportStaging.validate(publish.getSessionId()); }
        catch (IOException ex) { return failure("INVALID_ARGUMENT", ex.getMessage()); }
        if (publish.getTypeId() <= 0 || publish.getTypeName().isBlank()) return failure("INVALID_ARGUMENT", "导入发布缺少类型");
        if (publish.getConflict() == ImportConflict.IMPORT_CONFLICT_UNSPECIFIED || publish.getConflict() == ImportConflict.UNRECOGNIZED) {
            return failure("INVALID_ARGUMENT", "导入冲突策略无效");
        }
        if (publish.getRunsCount() == 0) return failure("INVALID_ARGUMENT", "发布清单为空");
        return null;
    }

    private static final class Conflicts {
        /** 不能写入的物理键，按键序。 */
        final List<byte[]> skipData = new ArrayList<>();
        /** 不写进点目录的键：被跳过的点，和覆盖已有点时原来的目录项。 */
        final List<byte[]> skipDirectory = new ArrayList<>();
        long skipped, replaced;
    }

    /**
     * 点：新 ID 已经登记给别的点类型（或正在删除）时，无论冲突策略都不能写；登记在同一个类型下时按冲突策略处理。
     * 边：边的 ID 只在边类型内唯一，只比对这个边类型已有的边记录。邻接键按点和边 ID 确定，重写同一个键是幂等的，不比对。
     */
    private static Conflicts classify(RocksDB db, ImportPublish publish, ImportStaging.Prepared prepared) throws IOException, RocksDBException {
        Conflicts conflicts = new Conflicts();
        long generation = publish.getGeneration();
        ImportConflict mode = publish.getConflict();
        if (publish.getNode()) {
            if (empty(db, ImportSst.physical(EntityKeys.kindPrefix(EntityKeys.DIRECTORY, generation)))) return conflicts;
            try (Seeker seeker = new Seeker(db)) {
                for (Path file : prepared.directory()) forEach(file, (key, value) -> {
                    Entity entry = visible(seeker.find(key));
                    if (entry == null) return;
                    boolean same = entry.getType().equals(publish.getTypeName()) && entry.getState().equals("ACTIVE");
                    if (mode == ImportConflict.IMPORT_ERROR) {
                        throw new ImportMerge.ImportException(same ? "KEY_EXISTS" : "ID_CONFLICT",
                                same ? "导入主键已存在" : "点 ID 已经属于点类型 " + entry.getType() + "，点 ID 在全图唯一");
                    }
                    conflicts.skipDirectory.add(key);
                    if (same && mode == ImportConflict.IMPORT_REPLACE) {
                        conflicts.replaced++;
                        return;
                    }
                    byte[] id = EntityKeys.directoryId(Arrays.copyOfRange(key, 1, key.length));
                    conflicts.skipData.add(ImportSst.physical(EntityKeys.entity(true, generation, publish.getTypeId(), id)));
                    conflicts.skipped++;
                });
            }
            return conflicts;
        }
        if (empty(db, ImportSst.physical(EntityKeys.typePrefix(false, generation, publish.getTypeId())))) return conflicts;
        try (Seeker seeker = new Seeker(db)) {
            for (Path file : prepared.data()) forEach(file, (key, value) -> {
                if (key[1] != EntityKeys.EDGE) return;
                Entity existing = visible(seeker.find(key));
                if (existing == null) return;
                if (mode == ImportConflict.IMPORT_ERROR) throw new ImportMerge.ImportException("KEY_EXISTS", "导入主键已存在");
                // 邻接键在端点所在的分片，这里改不到。端点变了的边不论跳过还是覆盖，都会在别的分片留下指错的邻接。
                Entity incoming = visible(value);
                if (incoming == null || !incoming.getSource().equals(existing.getSource()) || !incoming.getTarget().equals(existing.getTarget())) {
                    throw new ImportMerge.ImportException("ID_CONFLICT", "边 ID 已存在且端点不同，批量导入不能改变已有边的端点");
                }
                if (mode == ImportConflict.IMPORT_REPLACE) { conflicts.replaced++; return; }
                conflicts.skipData.add(key);
                conflicts.skipped++;
            });
        }
        return conflicts;
    }

    /** 去掉跳过的键，重写成新的一组文件。只在新导入和已有数据冲突时发生。 */
    private static List<Path> filter(List<Path> files, List<byte[]> skip, Path work, String prefix, boolean splitKinds) throws IOException {
        int[] next = {0};
        try (ImportMerge.Output output = new ImportMerge.Output(work, prefix, ImportStaging.PUBLISH_FILE_BYTES, splitKinds, true)) {
            for (Path file : files) forEach(file, (key, value) -> {
                while (next[0] < skip.size() && Arrays.compareUnsigned(skip.get(next[0]), key) < 0) next[0]++;
                if (next[0] < skip.size() && Arrays.equals(skip.get(next[0]), key)) return;
                output.put(key, value);
            });
            return output.finish();
        }
    }

    private static boolean empty(RocksDB db, byte[] prefix) throws RocksDBException {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            if (!iterator.isValid()) { iterator.status(); return true; }
            byte[] key = iterator.key();
            return key.length < prefix.length || Arrays.mismatch(key, 0, prefix.length, prefix, 0, prefix.length) >= 0;
        }
    }

    private static void forEach(Path file, ImportMerge.Sink sink) throws IOException {
        try (Options options = ImportSst.publishOptions(); SstFileReader reader = new SstFileReader(options);
             ReadOptions read = new ReadOptions().setReadaheadSize(4L * 1024 * 1024).setFillCache(false)) {
            reader.open(file.toString());
            try (SstFileReaderIterator iterator = reader.newIterator(read)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) sink.put(iterator.key(), iterator.value());
                iterator.status();
            }
        } catch (RocksDBException ex) {
            throw new IOException("读取待发布文件失败 " + file.getFileName(), ex);
        }
    }

    /** 已提交的最新版本；被删除或没有已提交版本时为 null。 */
    private static Entity visible(byte[] encoded) throws IOException {
        if (encoded == null) return null;
        VersionedCell cell = VersionedCell.parseFrom(encoded);
        for (int i = cell.getVersionsCount() - 1; i >= 0; i--) {
            var version = cell.getVersions(i);
            if (!version.getBaseline() && version.getCommittedAtMs() == 0) continue;
            return version.getDeleted() ? null : Entity.parseFrom(version.getValue());
        }
        return null;
    }

    private static BatchResult failure(String code, String message) {
        return BatchResult.newBuilder().setCode(code).setMessage(message).build();
    }

    /**
     * 按键序依次查找。相邻的键往往挨得很近，先往后走几步，走不到再 seek；
     * 两边数据量相当时接近一次顺序扫描，新键稀疏时退化为逐个 seek。
     */
    private static final class Seeker implements AutoCloseable {
        private final RocksIterator iterator;
        private byte[] current;
        private boolean started;

        Seeker(RocksDB db) { iterator = db.newIterator(); }

        byte[] find(byte[] key) throws IOException {
            try {
                if (!started) {
                    started = true;
                    seek(key);
                } else if (current != null && Arrays.compareUnsigned(current, key) < 0) {
                    for (int step = 0; step < 8 && current != null && Arrays.compareUnsigned(current, key) < 0; step++) {
                        iterator.next();
                        load();
                    }
                    if (current != null && Arrays.compareUnsigned(current, key) < 0) seek(key);
                }
                return current != null && Arrays.equals(current, key) ? iterator.value() : null;
            } catch (RocksDBException ex) { throw new IOException("比对已有数据失败", ex); }
        }

        private void seek(byte[] key) throws RocksDBException {
            iterator.seek(key);
            load();
        }

        private void load() throws RocksDBException {
            if (iterator.isValid()) current = iterator.key();
            else { iterator.status(); current = null; }
        }

        @Override public void close() { iterator.close(); }
    }
}
