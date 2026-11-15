package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.BatchResult;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.ImportChunk;
import io.cengyan.protocol.proto.ClusterProtocol.ImportConflict;
import io.cengyan.protocol.proto.ClusterProtocol.ImportDirective;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 把已经落到本副本的 SST 摄入 RocksDB。
 * 计数增减先写入暂存文件再摄入，这样进程内重试不会把已写入的键再算一遍。
 */
final class ImportApplier {
    private ImportApplier() { }

    static void stage(Path staging, ImportChunk chunk, long now) throws IOException {
        byte[] checksum = checksum(chunk.getChecksum());
        String hex = hex(checksum);
        ImportSessions.bind(staging, chunk.getSessionId(), hex, now);
        Path ready = staging.resolve(hex + ".sst");
        if (Files.exists(ready)) {
            if (!Arrays.equals(ImportSst.sha256(ready), checksum)) throw new IOException("同一校验和对应了不同的导入文件");
            return;
        }
        Path partial = staging.resolve(hex + ".partial");
        long size = Files.exists(partial) ? Files.size(partial) : 0;
        if (chunk.getOffset() != size) throw new IOException("导入分块偏移不连续");
        try (FileChannel channel = FileChannel.open(partial, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.position(chunk.getOffset());
            channel.write(chunk.getData().asReadOnlyByteBuffer());
            channel.force(true);
        }
        if (chunk.getLast()) {
            if (!Arrays.equals(ImportSst.sha256(partial), checksum)) {
                Files.deleteIfExists(partial);
                throw new IOException("导入文件校验和不匹配");
            }
            Files.move(partial, ready, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static long stagedBytes(Path staging, byte[] checksum) throws IOException {
        Path source = staging.resolve(hex(checksum) + ".sst");
        return Files.isRegularFile(source) ? Files.size(source) : 0;
    }

    static String sessionOwner(Path staging, byte[] checksum) throws IOException {
        Path owner = staging.resolve(hex(checksum) + ".session");
        if (!Files.isRegularFile(owner)) return null;
        String session = Files.readString(owner).trim();
        return session.isEmpty() ? null : session;
    }

    static BatchResult ingest(RocksDB db, org.rocksdb.WriteBatch batch, Path staging, ImportDirective directive, boolean appendOnly, boolean[] newlyApplied) throws IOException, RocksDBException {
        byte[] checksum = checksum(directive.getChecksum());
        byte[] markerKey = marker(checksum);
        String hex = hex(checksum);
        if (directive.getUndo()) return undo(db, batch, markerKey, checksum);
        byte[] done = db.get(markerKey);
        if (done != null) return BatchResult.parseFrom(done);
        Path source = staging.resolve(hex + ".sst");
        if (!Files.exists(source)) throw new IOException("导入文件尚未到达本副本");
        if (!Arrays.equals(ImportSst.sha256(source), checksum)) return failure("CHECKSUM_MISMATCH", "导入文件校验和不匹配");
        Path pendingFile = staging.resolve(hex + ".pending");
        Path accepted = staging.resolve(hex + ".apply.sst");
        Path inserted = staging.resolve(hex + ".inserted");
        Planned planned = Files.exists(pendingFile) ? Planned.read(pendingFile, inserted) : plan(db, directive, source, accepted, inserted, pendingFile, appendOnly);
        if (planned.failure != null) return planned.failure;
        if (planned.keys > 0) {
            Path ingestFile = planned.direct ? source : accepted;
            Path link = linkedCopy(ingestFile, staging.resolve(hex + ".link"));
            try (IngestExternalFileOptions options = new IngestExternalFileOptions().setMoveFiles(true)) {
                db.ingestExternalFile(List.of(link.toString()), options);
            }
        }
        for (byte[] obsolete : planned.obsoleteAdjacency) batch.delete(obsolete);
        applyDeltas(db, batch, planned.deltas);
        var result = BatchResult.newBuilder().setCode("OK").setAffected(planned.inserted).build();
        batch.put(markerKey, result.toByteArray());
        batch.put(undoKey(checksum), encodeKeys(planned.insertedKeys));
        if (newlyApplied != null && newlyApplied.length > 0) newlyApplied[0] = true;
        return result;
    }

    /** 提交已经落盘后删掉暂存。硬链接摄入后，删掉源文件不会再复制一份数据。 */
    static void release(Path staging, byte[] checksum) throws IOException {
        if (staging == null || checksum == null) return;
        String hex = hex(checksum);
        for (String suffix : new String[] {".partial", ".sst", ".apply.sst", ".inserted", ".pending", ".session", ".link"}) {
            Files.deleteIfExists(staging.resolve(hex + suffix));
        }
    }

    /** 优先硬链接，跨盘时才复制。moveFiles 会把这个链接移进库，原文件仍留到提交成功。 */
    private static Path linkedCopy(Path source, Path link) throws IOException {
        Files.deleteIfExists(link);
        try { Files.createLink(link, source); }
        catch (UnsupportedOperationException | IOException ex) { Files.copy(source, link); }
        return link;
    }

    private static BatchResult undo(RocksDB db, org.rocksdb.WriteBatch batch, byte[] markerKey, byte[] checksum) throws IOException, RocksDBException {
        if (db.get(markerKey) == null) return failure("IMPORT_NOT_APPLIED", "导入文件尚未摄入，不能回滚");
        byte[] stored = db.get(undoKey(checksum));
        if (stored == null) return failure("IMPORT_NOT_APPLIED", "导入文件没有可回滚的新键");
        Map<ByteString, Long> deltas = new LinkedHashMap<>();
        long affected = 0;
        for (byte[] key : decodeKeys(stored)) {
            byte[] current = db.get(key);
            if (current == null) continue;
            tally(deltas, key, current, -1);
            batch.delete(key);
            affected++;
        }
        applyDeltas(db, batch, deltas);
        batch.delete(markerKey);
        batch.delete(undoKey(checksum));
        return BatchResult.newBuilder().setCode("OK").setAffected(affected).build();
    }

    private static Planned plan(RocksDB db, ImportDirective directive, Path source, Path accepted, Path inserted, Path pendingFile, boolean appendOnly) throws IOException, RocksDBException {
        if (directive.getConflict() == ImportConflict.IMPORT_CONFLICT_UNSPECIFIED) {
            return Planned.failed(failure("INVALID_ARGUMENT", "导入冲突策略无效"));
        }
        Scan scan = scan(db, directive, source, inserted, appendOnly);
        if (scan.failure != null) return Planned.failed(scan.failure);
        if (scan.rewrite && scan.accepted > 0) rewrite(db, directive, source, accepted);
        else Files.deleteIfExists(accepted);
        boolean direct = !scan.rewrite && scan.accepted > 0;
        var planned = new Planned(null, scan.deltas, scan.obsolete, scan.inserted, scan.accepted, direct, scan.newKeys);
        planned.write(pendingFile);
        return planned;
    }

    /** @param appendOnly 会话开始时本分片还没有这个代次的点或边。整批视为新键，不再逐键点查。 */
    private static Scan scan(RocksDB db, ImportDirective directive, Path source, Path inserted, boolean appendOnly) throws IOException, RocksDBException {
        Map<ByteString, Long> deltas = new LinkedHashMap<>();
        List<byte[]> obsolete = new ArrayList<>();
        List<byte[]> newKeys = new ArrayList<>();
        long insertedKeys = 0;
        long acceptedKeys = 0;
        boolean rewrite = false;
        try (Options readerOptions = ImportSst.writerOptions();
             SstFileReader reader = new SstFileReader(readerOptions);
             ReadOptions read = new ReadOptions();
             DataOutputStream insertedOut = new DataOutputStream(Files.newOutputStream(inserted))) {
            reader.open(source.toString());
            try (SstFileReaderIterator iterator = reader.newIterator(read)) {
                byte[] previous = null;
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();
                    byte[] value = iterator.value();
                    if (previous != null && ImportSst.compare(previous, key) >= 0) return Scan.failed(failure("INVALID_ARGUMENT", "导入文件键序无效"));
                    previous = key;
                    byte[] logical = logical(key);
                    if (logical.length < 9 || logical[0] < 16 || logical[0] > 19) return Scan.failed(failure("INVALID_ARGUMENT", "导入键不是点、边或邻接"));
                    if (ByteBuffer.wrap(logical, 1, 8).getLong() != directive.getGeneration()) return Scan.failed(failure("INVALID_ARGUMENT", "导入键代次与图不一致"));
                    boolean primary = logical[0] == 16 || logical[0] == 17;
                    byte[] current = appendOnly ? null : db.get(key);
                    if (current != null && primary && directive.getConflict() == ImportConflict.IMPORT_ERROR) {
                        return Scan.failed(failure("KEY_EXISTS", "导入主键已存在"));
                    }
                    if (current != null && directive.getConflict() == ImportConflict.IMPORT_IGNORE) { rewrite = true; continue; }
                    if (current != null) {
                        rewrite = true;
                        if (logical[0] == 17 && directive.getConflict() == ImportConflict.IMPORT_REPLACE) obsolete.addAll(oldAdjacency(current, value, logical));
                    }
                    if (current == null) {
                        insertedOut.writeInt(key.length);
                        insertedOut.write(key);
                        newKeys.add(key);
                        if (primary) insertedKeys++;
                    }
                    if (primary) {
                        if (current != null) tally(deltas, key, current, -1);
                        tally(deltas, key, value, 1);
                    }
                    acceptedKeys++;
                }
            }
        }
        return new Scan(null, deltas, obsolete, newKeys, insertedKeys, acceptedKeys, rewrite);
    }

    /** 只有发生跳过或覆盖时才重写 SST。全部是新键时直接摄入原始文件。 */
    private static void rewrite(RocksDB db, ImportDirective directive, Path source, Path accepted) throws IOException, RocksDBException {
        try (Options readerOptions = ImportSst.writerOptions();
             SstFileReader reader = new SstFileReader(readerOptions);
             ReadOptions read = new ReadOptions();
             EnvOptions env = new EnvOptions();
             Options writerOptions = ImportSst.writerOptions();
             SstFileWriter writer = new SstFileWriter(env, writerOptions)) {
            reader.open(source.toString());
            writer.open(accepted.toString());
            try (SstFileReaderIterator iterator = reader.newIterator(read)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();
                    byte[] current = db.get(key);
                    if (current != null && directive.getConflict() == ImportConflict.IMPORT_IGNORE) continue;
                    writer.put(key, iterator.value());
                }
            }
            writer.finish();
        }
    }

    /** 覆盖边并且端点变化时，返回需要删除的旧出边、入边邻接。 */
    private static List<byte[]> oldAdjacency(byte[] current, byte[] incoming, byte[] logical) throws IOException {
        Entity old = visible(current);
        Entity newer = visible(incoming);
        if (old == null || newer == null || (old.getSource().equals(newer.getSource()) && old.getTarget().equals(newer.getTarget()))) return List.of();
        long generation = ByteBuffer.wrap(logical, 1, 8).getLong();
        byte[] edge = Arrays.copyOfRange(logical, 9, logical.length);
        return List.of(
                ImportSst.physical(ImportSst.adjacency(true, generation, ImportSst.canonical(old.getSource()), edge)),
                ImportSst.physical(ImportSst.adjacency(false, generation, ImportSst.canonical(old.getTarget()), edge)));
    }

    private static void applyDeltas(RocksDB db, org.rocksdb.WriteBatch batch, Map<ByteString, Long> deltas) throws IOException, RocksDBException {
        for (var entry : deltas.entrySet()) {
            if (entry.getValue() == 0) continue;
            byte[] old = db.get(entry.getKey().toByteArray());
            long next = Math.addExact(old == null ? 0 : ByteBuffer.wrap(old).getLong(), entry.getValue());
            if (next < 0) throw new IOException("实体计数不能为负");
            batch.put(entry.getKey().toByteArray(), ByteBuffer.allocate(8).putLong(next).array());
        }
    }

    private static void tally(Map<ByteString, Long> deltas, byte[] physical, byte[] encoded, long sign) throws IOException {
        if (physical.length < 11 || physical[0] != 2 || (physical[1] != 16 && physical[1] != 17)) return;
        Entity entity = visible(encoded);
        if (entity == null || entity.getType().isBlank()) return;
        long generation = ByteBuffer.wrap(physical, 2, 8).getLong();
        add(deltas, countKey(physical[1], generation, entity.getType()), sign);
        add(deltas, countKey(physical[1], generation, ""), sign);
    }

    private static void add(Map<ByteString, Long> deltas, byte[] key, long sign) {
        deltas.merge(ByteString.copyFrom(key), sign, Math::addExact);
    }

    private static byte[] countKey(byte kind, long generation, String type) {
        byte[] name = type.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(10 + name.length).put((byte) 3).put(kind).putLong(generation).put(name).array();
    }

    private static Entity visible(byte[] encoded) throws IOException {
        if (encoded == null) return null;
        VersionedCell cell = VersionedCell.parseFrom(encoded);
        for (int i = cell.getVersionsCount() - 1; i >= 0; i--) {
            var version = cell.getVersions(i);
            if (!version.getBaseline() && version.getCommittedAtMs() == 0) continue;
            if (version.getDeleted()) return null;
            return Entity.parseFrom(version.getValue());
        }
        return null;
    }

    private static byte[] logical(byte[] physical) throws IOException {
        if (physical.length < 2 || physical[0] != 2) throw new IOException("导入键缺少分片命名空间");
        return Arrays.copyOfRange(physical, 1, physical.length);
    }

    private static byte[] checksum(ByteString value) throws IOException {
        if (value.size() != 32) throw new IOException("导入校验和必须是 SHA-256");
        return value.toByteArray();
    }

    private static byte[] marker(byte[] checksum) {
        return prefixed(checksum, (byte) 4);
    }

    private static byte[] undoKey(byte[] checksum) {
        return prefixed(checksum, (byte) 5);
    }

    private static byte[] prefixed(byte[] checksum, byte kind) {
        byte[] key = new byte[checksum.length + 1];
        key[0] = kind;
        System.arraycopy(checksum, 0, key, 1, checksum.length);
        return key;
    }

    private static byte[] encodeKeys(List<byte[]> keys) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(buffer)) {
            output.writeInt(keys.size());
            for (byte[] key : keys) {
                output.writeInt(key.length);
                output.write(key);
            }
        }
        return buffer.toByteArray();
    }

    private static List<byte[]> decodeKeys(byte[] encoded) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            int count = input.readInt();
            if (count < 0) throw new IOException("导入回滚记录损坏");
            List<byte[]> keys = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int length = input.readInt();
                byte[] key = input.readNBytes(length);
                if (key.length != length) throw new IOException("导入回滚记录损坏");
                keys.add(key);
            }
            return keys;
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder text = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            text.append(Character.forDigit((value >> 4) & 0xf, 16)).append(Character.forDigit(value & 0xf, 16));
        }
        return text.toString();
    }

    private static BatchResult failure(String code, String text) {
        return BatchResult.newBuilder().setCode(code).setMessage(text).build();
    }

    private record Scan(BatchResult failure, Map<ByteString, Long> deltas, List<byte[]> obsolete, List<byte[]> newKeys,
                        long inserted, long accepted, boolean rewrite) {
        static Scan failed(BatchResult failure) { return new Scan(failure, Map.of(), List.of(), List.of(), 0, 0, false); }
    }

    private record Planned(BatchResult failure, Map<ByteString, Long> deltas, List<byte[]> obsoleteAdjacency, long inserted, long keys,
                           boolean direct, List<byte[]> insertedKeys) {
        static Planned failed(BatchResult failure) { return new Planned(failure, Map.of(), List.of(), 0, 0, false, List.of()); }

        static Planned read(Path file, Path insertedFile) throws IOException {
            try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
                long inserted = input.readLong();
                long keys = input.readLong();
                int deltaCount = input.readInt();
                Map<ByteString, Long> deltas = new LinkedHashMap<>();
                for (int i = 0; i < deltaCount; i++) {
                    byte[] key = input.readNBytes(input.readInt());
                    deltas.put(ByteString.copyFrom(key), input.readLong());
                }
                int obsoleteCount = input.readInt();
                List<byte[]> obsolete = new ArrayList<>();
                for (int i = 0; i < obsoleteCount; i++) obsolete.add(input.readNBytes(input.readInt()));
                boolean direct = input.readBoolean();
                return new Planned(null, deltas, obsolete, inserted, keys, direct, readInserted(insertedFile));
            }
        }

        void write(Path file) throws IOException {
            try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(file))) {
                output.writeLong(inserted);
                output.writeLong(keys);
                output.writeInt(deltas.size());
                for (var entry : deltas.entrySet()) {
                    output.writeInt(entry.getKey().size());
                    output.write(entry.getKey().toByteArray());
                    output.writeLong(entry.getValue());
                }
                output.writeInt(obsoleteAdjacency.size());
                for (byte[] key : obsoleteAdjacency) {
                    output.writeInt(key.length);
                    output.write(key);
                }
                output.writeBoolean(direct);
            }
        }

        private static List<byte[]> readInserted(Path file) throws IOException {
            if (!Files.exists(file)) return List.of();
            List<byte[]> keys = new ArrayList<>();
            try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
                while (true) {
                    int length;
                    try { length = input.readInt(); }
                    catch (EOFException end) { break; }
                    byte[] key = input.readNBytes(length);
                    if (key.length != length) throw new IOException("导入回滚记录损坏");
                    keys.add(key);
                }
            }
            return keys;
        }
    }
}
