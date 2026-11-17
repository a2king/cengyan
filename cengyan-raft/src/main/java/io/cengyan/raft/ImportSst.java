package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.EntityKeys;
import io.cengyan.protocol.proto.ClusterProtocol.CellVersion;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.rocksdb.CompressionType;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 导入上传段的格式：一个有序 SST，键是分片库里的物理键，值是基线单元格。
 * 键的逻辑布局见 {@link EntityKeys}；物理键在逻辑键前面加分片命名空间 2。
 */
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

    /** 出邻接或入邻接的逻辑键。 */
    public static byte[] adjacency(boolean outgoing, long generation, int edgeTypeId, byte[] vertex, byte[] edge) {
        return EntityKeys.adjacency(outgoing, generation, edgeTypeId, vertex, edge);
    }

    public static byte[] canonical(Value value) throws IOException {
        return switch (value.getKindCase()) {
            case INT_VALUE -> new EntityId.Int64(value.getIntValue()).canonicalBytes();
            case STRING_VALUE -> new EntityId.Text(value.getStringValue()).canonicalBytes();
            default -> throw new IOException("实体主键类型无效");
        };
    }

    public static int compare(byte[] left, byte[] right) { return Arrays.compareUnsigned(left, right); }

    /** 按物理键排序后写成一个上传段。重复的物理键会被拒绝。 */
    public static void write(Path file, List<Row> rows) throws IOException {
        List<byte[][]> ordered = new ArrayList<>(rows.size());
        for (Row row : rows) ordered.add(new byte[][] {physical(row.logicalKey()), row.entity()});
        ordered.sort((left, right) -> compare(left[0], right[0]));
        Files.createDirectories(file.toAbsolutePath().getParent());
        try (EnvOptions env = new EnvOptions(); Options options = runOptions(); SstFileWriter writer = new SstFileWriter(env, options)) {
            writer.open(file.toString());
            byte[] previous = null;
            for (byte[][] row : ordered) {
                if (previous != null && compare(previous, row[0]) >= 0) throw new IOException("导入文件存在重复主键");
                writer.put(row[0], cell(row[1]));
                previous = row[0];
            }
            writer.finish();
        } catch (org.rocksdb.RocksDBException ex) {
            throw new IOException(ex);
        }
    }

    /** 上传段和中间归并结果只在归并时顺序读一遍，不需要过滤器。 */
    public static Options runOptions() {
        return new Options().setCompressionType(CompressionType.LZ4_COMPRESSION);
    }

    /**
     * 最终摄入分片库的文件。表格式与分片库相同（分区索引、分区布隆过滤器），
     * 压缩取分片库最底层的 ZSTD：这些文件摄入后直接落在最底层，不再被压实重写。
     */
    static Options publishOptions() {
        return new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(RocksStateMachine.tableConfig());
    }

    /** 流式计算，不把整个文件读进堆。 */
    public static byte[] sha256(Path file) throws IOException {
        MessageDigest digest = digest();
        byte[] buffer = new byte[1 << 20];
        try (InputStream input = Files.newInputStream(file)) {
            int read;
            while ((read = input.read(buffer)) >= 0) digest.update(buffer, 0, read);
        }
        return digest.digest();
    }

    static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }

    public static String hex(byte[] bytes) { return java.util.HexFormat.of().formatHex(bytes); }

    public static boolean same(byte[] left, byte[] right) { return Arrays.equals(left, right); }
}
