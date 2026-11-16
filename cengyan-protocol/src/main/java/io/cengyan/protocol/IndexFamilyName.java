package io.cengyan.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 索引的 RocksDB Column Family 名称。逻辑索引键是 {@code [32][generation:8][len:4][indexId][...]}，
 * 名称只由 indexId 决定，与代次无关：同一个索引在新旧代次的键落在同一个 Column Family。
 */
public final class IndexFamilyName {
    public static final byte INDEX_NAMESPACE = 32;
    private static final int HEADER = 1 + 8 + 4;
    private IndexFamilyName() { }

    public static String of(String indexIdentity) {
        if (indexIdentity == null || indexIdentity.isBlank()) throw new IllegalArgumentException("索引物理身份不能为空");
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(indexIdentity.getBytes(StandardCharsets.UTF_8));
            return "idx-" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }

    /** @return 键里的 indexId；不是完整索引键（或前缀）时返回 null */
    public static String identity(byte[] key) {
        if (key == null || key.length < HEADER || key[0] != INDEX_NAMESPACE) return null;
        int length = ByteBuffer.wrap(key, 9, 4).getInt();
        if (length < 1 || length > key.length - HEADER) return null;
        return new String(key, HEADER, length, StandardCharsets.UTF_8);
    }

    public static String fromIndexKey(byte[] key) {
        String identity = identity(key);
        return identity == null ? null : of(identity);
    }

    /** 恰好是一个索引的完整前缀 {@code [32][generation][len][indexId]}，后面没有属性值。 */
    public static boolean isIndexPrefix(byte[] prefix) {
        return identity(prefix) != null && prefix.length == HEADER + ByteBuffer.wrap(prefix, 9, 4).getInt();
    }
}
