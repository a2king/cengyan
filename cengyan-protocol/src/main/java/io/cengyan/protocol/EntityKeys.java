package io.cengyan.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 点、边、邻接和点目录的逻辑键布局。Compute、Storage 状态机和导入客户端共用这一份，不各自拼字节。
 *
 * <pre>
 * 点数据  [16][代次 8][点类型 4][点 ID]
 * 边      [17][代次 8][边类型 4][边 ID]
 * 出邻接  [18][代次 8][边类型 4][起点长度 4][起点 ID][边 ID]
 * 入邻接  [19][代次 8][边类型 4][终点长度 4][终点 ID][边 ID]
 * 点目录  [20][代次 8][点 ID]
 * </pre>
 *
 * 类型编号排在 ID 前面，一个类型的全部记录落在一段连续的键区间里：按类型扫描是前缀扫描，
 * 批量导入一个新类型时它的文件与其他类型的文件互不重叠。点的 ID 在全图唯一，由点目录保证：
 * 目录的键里没有类型，同一个 ID 只能登记一次。边的 ID 只在边类型内唯一，不需要目录。
 */
public final class EntityKeys {
    public static final byte NODE = 16, EDGE = 17, OUT = 18, IN = 19, DIRECTORY = 20;
    /** 点数据、边、邻接键里类型编号之后的偏移。 */
    private static final int TYPED_HEADER = 13;
    private static final int DIRECTORY_HEADER = 9;

    private EntityKeys() { }

    public static byte[] entity(boolean node, long generation, int typeId, byte[] id) {
        return typed(node ? NODE : EDGE, generation, typeId, id.length).put(id).array();
    }

    /** 一个点类型或边类型全部记录的键前缀。 */
    public static byte[] typePrefix(boolean node, long generation, int typeId) {
        return typed(node ? NODE : EDGE, generation, typeId, 0).array();
    }

    /** 某一类键（点数据、边、邻接、点目录）在一个代次里的全部记录。 */
    public static byte[] kindPrefix(byte kind, long generation) {
        return ByteBuffer.allocate(DIRECTORY_HEADER).put(kind).putLong(generation).array();
    }

    public static byte[] directory(long generation, byte[] id) {
        return ByteBuffer.allocate(DIRECTORY_HEADER + id.length).put(DIRECTORY).putLong(generation).put(id).array();
    }

    public static byte[] adjacency(boolean outgoing, long generation, int typeId, byte[] vertex, byte[] edge) {
        return typed(outgoing ? OUT : IN, generation, typeId, 4 + vertex.length + edge.length)
                .putInt(vertex.length).put(vertex).put(edge).array();
    }

    /** 一个点在一个边类型下的全部出边或入边。 */
    public static byte[] adjacencyPrefix(boolean outgoing, long generation, int typeId, byte[] vertex) {
        return typed(outgoing ? OUT : IN, generation, typeId, 4 + vertex.length).putInt(vertex.length).put(vertex).array();
    }

    /** 一个边类型的全部出邻接或入邻接。 */
    public static byte[] adjacencyTypePrefix(boolean outgoing, long generation, int typeId) {
        return typed(outgoing ? OUT : IN, generation, typeId, 0).array();
    }

    public static byte kind(byte[] logical) { return logical.length == 0 ? 0 : logical[0]; }

    /** 点数据或边记录的键，带完整的 ID。 */
    public static boolean primary(byte[] logical) {
        return logical.length > TYPED_HEADER && (logical[0] == NODE || logical[0] == EDGE);
    }

    public static boolean adjacency(byte[] logical) {
        return logical.length > TYPED_HEADER + 4 && (logical[0] == OUT || logical[0] == IN);
    }

    public static boolean directory(byte[] logical) {
        return logical.length > DIRECTORY_HEADER && logical[0] == DIRECTORY;
    }

    public static long generation(byte[] logical) { return ByteBuffer.wrap(logical, 1, 8).getLong(); }

    /** 点数据、边、邻接键里的类型编号。点目录的键没有类型。 */
    public static int typeId(byte[] logical) { return ByteBuffer.wrap(logical, 9, 4).getInt(); }

    /** 点数据或边记录键里的 ID 字节。 */
    public static byte[] primaryId(byte[] logical) { return Arrays.copyOfRange(logical, TYPED_HEADER, logical.length); }

    public static byte[] directoryId(byte[] logical) { return Arrays.copyOfRange(logical, DIRECTORY_HEADER, logical.length); }

    private static ByteBuffer typed(byte kind, long generation, int typeId, int tail) {
        if (typeId <= 0) throw new IllegalArgumentException("类型编号必须为正数");
        return ByteBuffer.allocate(TYPED_HEADER + tail).put(kind).putLong(generation).putInt(typeId);
    }
}
