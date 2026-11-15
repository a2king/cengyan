package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.proto.ClusterProtocol.CellVersion;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.rocksdb.CompressionType;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 导入 SST 的物理键和基线单元格。键序与分片 RocksDB 的默认字节序一致。 */
public final class ImportSst {
    private ImportSst() { }

    public record Row(byte[] logicalKey, byte[] entity) { }

    public static byte[] physical(byte[] logical) {
        byte[] key = new byte[logical.length + 1];
        key[0] = 2;
        System.arraycopy(logical, 0, key, 1, logical.length);
        return key;
    }

    public static byte[] cell(byte[] entity) {
        return VersionedCell.newBuilder().addVersions(CellVersion.newBuilder()
                .setBaseline(true).setCommittedAtMs(1).setValue(ByteString.copyFrom(entity))).build().toByteArray();
    }

    public static byte[] adjacency(boolean outgoing, long generation, byte[] vertex, byte[] edge) {
        return ByteBuffer.allocate(13 + vertex.length + edge.length)
                .put(outgoing ? (byte) 18 : (byte) 19).putLong(generation).putInt(vertex.length).put(vertex).put(edge).array();
    }

    public static byte[] canonical(Value value) throws IOException {
        return switch (value.getKindCase()) {
            case INT_VALUE -> new EntityId.Int64(value.getIntValue()).canonicalBytes();
            case STRING_VALUE -> new EntityId.Text(value.getStringValue()).canonicalBytes();
            default -> throw new IOException("边端点主键类型无效");
        };
    }

    public static int compare(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int diff = (left[i] & 0xff) - (right[i] & 0xff);
            if (diff != 0) return diff;
        }
        return left.length - right.length;
    }

    /** 按物理键排序后写入。重复的物理键会被拒绝。 */
    public static void write(Path file, List<Row> rows) throws IOException {
        List<Row> ordered = new ArrayList<>(rows);
        ordered.sort((left, right) -> compare(physical(left.logicalKey()), physical(right.logicalKey())));
        Files.createDirectories(file.getParent() == null ? Path.of(".") : file.getParent());
        try (EnvOptions env = new EnvOptions(); Options options = writerOptions(); SstFileWriter writer = new SstFileWriter(env, options)) {
            writer.open(file.toString());
            byte[] previous = null;
            for (Row row : ordered) {
                byte[] key = physical(row.logicalKey());
                if (previous != null && compare(previous, key) >= 0) throw new IOException("导入文件存在重复主键");
                writer.put(key, cell(row.entity()));
                previous = key;
            }
            writer.finish();
        } catch (org.rocksdb.RocksDBException ex) {
            throw new IOException(ex);
        }
    }

    /** 导入 SST 与分片库使用同一种压缩，摄入后不必再整表重写。 */
    public static Options writerOptions() {
        return new Options().setCompressionType(CompressionType.LZ4_COMPRESSION);
    }

    public static byte[] sha256(Path file) throws IOException {
        try { return MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)); }
        catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }

    public static boolean same(byte[] left, byte[] right) { return Arrays.equals(left, right); }
}
