package io.cengyan.store;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 键的唯一编码处。其它类不自己拼字节。多字节整数用大端，RocksDB 才能按前缀有序。
 */
public final class KeyCodec {
    public static final int EDGE_KEY_LENGTH = 42;
    public static final int EDGE_PREFIX_LENGTH = 17;
    public static final int VERTEX_KEY_LENGTH = 16;
    public static final int VERTEX_PREFIX_LENGTH = 12;
    public static final int PROP_KEY_LENGTH = 31;
    public static final byte PROP_MARK = 0x01;

    private KeyCodec() {
    }

    public static byte[] edgeKey(
            int graphId, int partId, long src, int edgeTypeId, int direction, long rank, long dst) {
        checkUint16(graphId, "graphId");
        checkUint16(partId, "partId");
        checkUint32(edgeTypeId, "edgeTypeId");
        if (direction != Direction.OUT.code() && direction != Direction.IN.code()) {
            throw new IllegalArgumentException("direction 只能是 0 或 1");
        }
        byte[] key = new byte[EDGE_KEY_LENGTH];
        putUint16(key, 0, graphId);
        putUint16(key, 2, partId);
        putInt64(key, 4, src);
        putUint32(key, 12, edgeTypeId);
        key[16] = (byte) direction;
        putInt64(key, 17, rank);
        putInt64(key, 25, dst);
        return key;
    }

    public static byte[] edgePrefix(int graphId, int partId, long src, int edgeTypeId, int direction) {
        byte[] key = edgeKey(graphId, partId, src, edgeTypeId, direction, 0L, 0L);
        return Arrays.copyOf(key, EDGE_PREFIX_LENGTH);
    }

    public static byte[] vertexKey(int graphId, int partId, long vid, int tagId) {
        checkUint16(graphId, "graphId");
        checkUint16(partId, "partId");
        checkUint32(tagId, "tagId");
        byte[] key = new byte[VERTEX_KEY_LENGTH];
        putUint16(key, 0, graphId);
        putUint16(key, 2, partId);
        putInt64(key, 4, vid);
        putUint32(key, 12, tagId);
        return key;
    }

    /** 一个点的全部关联边共用的 12 字节前缀。 */
    public static byte[] vertexPrefix(int graphId, int partId, long vid) {
        byte[] key = vertexKey(graphId, partId, vid, 0);
        return Arrays.copyOf(key, VERTEX_PREFIX_LENGTH);
    }

    public static byte[] propKey(int graphId, int edgeTypeId, long rank, long outSrc, long outDst) {
        checkUint16(graphId, "graphId");
        checkUint32(edgeTypeId, "edgeTypeId");
        byte[] key = new byte[PROP_KEY_LENGTH];
        key[0] = PROP_MARK;
        putUint16(key, 1, graphId);
        putUint32(key, 3, edgeTypeId);
        putInt64(key, 7, rank);
        putInt64(key, 15, outSrc);
        putInt64(key, 23, outDst);
        return key;
    }

    /**
     * 前缀的开区间上界。全部是 0xFF 时返回 null，表示扫到列族末尾。
     */
    public static byte[] upperBound(byte[] prefix) {
        if (prefix == null || prefix.length == 0) {
            throw new IllegalArgumentException("前缀不能为空");
        }
        byte[] bound = Arrays.copyOf(prefix, prefix.length);
        for (int i = bound.length - 1; i >= 0; i--) {
            int value = bound[i] & 0xFF;
            if (value != 0xFF) {
                bound[i] = (byte) (value + 1);
                return bound;
            }
            bound[i] = 0;
        }
        return null;
    }

    public static DecodedEdge decodeEdge(byte[] key) {
        if (key == null || key.length != EDGE_KEY_LENGTH) {
            throw new IllegalArgumentException("边键长度必须是 42");
        }
        return new DecodedEdge(
                getUint16(key, 0),
                getUint16(key, 2),
                getInt64(key, 4),
                getUint32(key, 12),
                key[16] & 0xFF,
                getInt64(key, 17),
                getInt64(key, 25));
    }

    /** 调用方拿到的始终是逻辑端点，入边会把键里的两端换回来。 */
    public static EdgeRef toLogical(byte[] key) {
        DecodedEdge decoded = decodeEdge(key);
        if (decoded.direction() == Direction.OUT.code()) {
            return new EdgeRef(decoded.edgeTypeId(), decoded.rank(), decoded.src(), decoded.dst());
        }
        return new EdgeRef(decoded.edgeTypeId(), decoded.rank(), decoded.dst(), decoded.src());
    }

    public static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public static int compare(byte[] left, byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }

    /** 阶段 9 之前恒为 0。阶段 9 起改为按 vid 取模，不能只改这个函数。 */
    public static int partOf(long vid) {
        return 0;
    }

    public static byte[] nextGraphIdKey() {
        return new byte[] {0x01};
    }

    public static byte[] nameKey(String name) {
        byte[] utf8 = nameUtf8(name);
        byte[] key = new byte[1 + utf8.length];
        key[0] = 0x10;
        System.arraycopy(utf8, 0, key, 1, utf8.length);
        return key;
    }

    public static byte[] namePrefix() {
        return new byte[] {0x10};
    }

    public static byte[] graphRecordKey(int graphId) {
        return markedGraphKey(0x11, graphId);
    }

    public static byte[] graphRecordPrefix() {
        return new byte[] {0x11};
    }

    public static byte[] aliasRecordKey(int graphId, String alias) {
        byte[] utf8 = nameUtf8(alias);
        byte[] key = new byte[3 + utf8.length];
        key[0] = 0x12;
        putUint16(key, 1, graphId);
        System.arraycopy(utf8, 0, key, 3, utf8.length);
        return key;
    }

    public static byte[] aliasPrefix(int graphId) {
        return markedGraphKey(0x12, graphId);
    }

    public static byte[] nextNodeTypeKey(int graphId) {
        return markedGraphKey(0x21, graphId);
    }

    public static byte[] nextRelTypeKey(int graphId) {
        return markedGraphKey(0x22, graphId);
    }

    public static byte[] typeKey(int graphId, int kind, int typeId) {
        checkUint16(graphId, "graphId");
        checkUint32(typeId, "typeId");
        if (kind != 1 && kind != 2) {
            throw new IllegalArgumentException("类型种类无效");
        }
        byte[] key = new byte[8];
        key[0] = 0x20;
        putUint16(key, 1, graphId);
        key[3] = (byte) kind;
        putUint32(key, 4, typeId);
        return key;
    }

    public static byte[] typePrefix(int graphId) {
        return markedGraphKey(0x20, graphId);
    }

    public static byte[] indexRecordPrefix(int graphId) {
        return markedGraphKey(0x30, graphId);
    }

    /** 全部类型记录的前缀。节点类型和边类型都从 0x20 开始。 */
    public static byte[] allTypePrefix() {
        return new byte[] {0x20};
    }

    /** 索引列族里某个 indexId 的前缀。索引键以 indexId 开头。 */
    public static byte[] indexDataPrefix(int indexId) {
        checkUint32(indexId, "indexId");
        byte[] key = new byte[4];
        putUint32(key, 0, indexId);
        return key;
    }

    public static int graphIdFromMarked(byte[] key) {
        if (key == null || key.length < 3) {
            throw new IllegalArgumentException("键里没有 graphId");
        }
        return getUint16(key, 1);
    }

    public static TypeKey decodeTypeKey(byte[] key) {
        if (key == null || key.length != 8 || (key[0] & 0xFF) != 0x20) {
            throw new IllegalArgumentException("不是类型键");
        }
        int kind = key[3] & 0xFF;
        if (kind != 1 && kind != 2) {
            throw new IllegalArgumentException("类型种类无效");
        }
        return new TypeKey(getUint16(key, 1), kind, getUint32(key, 4));
    }

    public static int indexIdFromRecord(byte[] key) {
        if (key == null || key.length != 7 || (key[0] & 0xFF) != 0x30) {
            throw new IllegalArgumentException("不是索引记录键");
        }
        return getUint32(key, 3);
    }

    public record TypeKey(int graphId, int kind, int typeId) {
    }

    public static byte[] graphIdPrefix(int graphId) {
        checkUint16(graphId, "graphId");
        byte[] key = new byte[2];
        putUint16(key, 0, graphId);
        return key;
    }

    public static byte[] propGraphPrefix(int graphId) {
        checkUint16(graphId, "graphId");
        byte[] key = new byte[3];
        key[0] = PROP_MARK;
        putUint16(key, 1, graphId);
        return key;
    }

    public static String nameFromKey(byte[] key, int offset) {
        return new String(key, offset, key.length - offset, StandardCharsets.UTF_8);
    }

    private static byte[] markedGraphKey(int mark, int graphId) {
        checkUint16(graphId, "graphId");
        byte[] key = new byte[3];
        key[0] = (byte) mark;
        putUint16(key, 1, graphId);
        return key;
    }

    private static byte[] nameUtf8(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("名字不能为空");
        }
        byte[] utf8 = name.getBytes(StandardCharsets.UTF_8);
        if (utf8.length > 128) {
            throw new IllegalArgumentException("名字最长 128 字节");
        }
        for (byte one : utf8) {
            if (one == 0) {
                throw new IllegalArgumentException("名字不能包含空字节");
            }
        }
        return utf8;
    }

    private static void checkUint16(int value, String name) {
        if (value < 0 || value > 0xFFFF) {
            throw new IllegalArgumentException(name + " 超出 uint16");
        }
    }

    private static void checkUint32(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " 不能为负");
        }
    }

    private static void putUint16(byte[] key, int offset, int value) {
        key[offset] = (byte) (value >>> 8);
        key[offset + 1] = (byte) value;
    }

    private static void putUint32(byte[] key, int offset, int value) {
        key[offset] = (byte) (value >>> 24);
        key[offset + 1] = (byte) (value >>> 16);
        key[offset + 2] = (byte) (value >>> 8);
        key[offset + 3] = (byte) value;
    }

    private static void putInt64(byte[] key, int offset, long value) {
        key[offset] = (byte) (value >>> 56);
        key[offset + 1] = (byte) (value >>> 48);
        key[offset + 2] = (byte) (value >>> 40);
        key[offset + 3] = (byte) (value >>> 32);
        key[offset + 4] = (byte) (value >>> 24);
        key[offset + 5] = (byte) (value >>> 16);
        key[offset + 6] = (byte) (value >>> 8);
        key[offset + 7] = (byte) value;
    }

    private static int getUint16(byte[] key, int offset) {
        return ((key[offset] & 0xFF) << 8) | (key[offset + 1] & 0xFF);
    }

    private static int getUint32(byte[] key, int offset) {
        return ((key[offset] & 0xFF) << 24)
                | ((key[offset + 1] & 0xFF) << 16)
                | ((key[offset + 2] & 0xFF) << 8)
                | (key[offset + 3] & 0xFF);
    }

    private static long getInt64(byte[] key, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (key[offset + i] & 0xFFL);
        }
        return value;
    }

    public record DecodedEdge(
            int graphId, int partId, long src, int edgeTypeId, int direction, long rank, long dst) {
    }
}
