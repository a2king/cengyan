package com.cengyan.core.internal;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * 与服务端 {@code io.cengyan.protocol.PartitionRouter} / {@code EntityId} 完全一致的分片路由：
 * xxHash64 v1（seed=0）作用于主键的规范字节，再按无符号取模。
 * 规范字节：INT64 为 {@code 0x01} + 8 字节大端；STRING 为 {@code 0x02} + UTF-8。
 * 一致性由 src/test/resources/contract/partition-vectors.json（用服务端实现生成）校验。
 */
public final class PartitionRouter {
    private static final long P1 = 0x9E3779B185EBCA87L;
    private static final long P2 = 0xC2B2AE3D27D4EB4FL;
    private static final long P3 = 0x165667B19E3779F9L;
    private static final long P4 = 0x85EBCA77C2B2AE63L;
    private static final long P5 = 0x27D4EB2F165667C5L;
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private PartitionRouter() {
    }

    /** 主键所在分片。id 与 idType 不符时抛 {@link IllegalArgumentException}。 */
    public static int partOf(Object id, String idType, int partitions) {
        if (partitions < 1 || partitions > 65535) throw new IllegalArgumentException("分片数必须在 1 和 65535 之间：" + partitions);
        return (int) Long.remainderUnsigned(hash(canonical(id, idType)), partitions);
    }

    public static byte[] canonical(Object id, String idType) {
        if ("STRING".equalsIgnoreCase(idType)) {
            if (!(id instanceof String)) throw new IllegalArgumentException("字符串 ID 必须是 String，实际是 " + typeName(id));
            // String.getBytes 会把孤立代理字符替换成 '?'，不同 ID 会落到同一编码上；服务端拒绝这种 ID。
            byte[] utf8 = Json.checkText((String) id).getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(1 + utf8.length).put((byte) 2).put(utf8).array();
        }
        if ("INT64".equalsIgnoreCase(idType)) {
            long value;
            if (id instanceof Long || id instanceof Integer || id instanceof Short || id instanceof Byte) {
                value = ((Number) id).longValue();
            } else if (id instanceof BigInteger) {
                BigInteger number = (BigInteger) id;
                if (number.compareTo(LONG_MIN) < 0 || number.compareTo(LONG_MAX) > 0) {
                    throw new IllegalArgumentException("整数 ID 超出 INT64 范围：" + number);
                }
                value = number.longValue();
            } else {
                throw new IllegalArgumentException("整数 ID 必须是 Long/Integer，实际是 " + typeName(id));
            }
            return ByteBuffer.allocate(9).put((byte) 1).putLong(value).array();
        }
        throw new IllegalArgumentException("未知 ID 类型：" + idType);
    }

    private static String typeName(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }

    /** xxHash64，seed=0。 */
    public static long hash(byte[] bytes) {
        ByteBuffer in = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long h;
        if (bytes.length >= 32) {
            long v1 = P1 + P2, v2 = P2, v3 = 0, v4 = -P1;
            while (in.remaining() >= 32) {
                v1 = round(v1, in.getLong());
                v2 = round(v2, in.getLong());
                v3 = round(v3, in.getLong());
                v4 = round(v4, in.getLong());
            }
            h = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h = merge(merge(merge(merge(h, v1), v2), v3), v4);
        } else {
            h = P5;
        }
        h += bytes.length;
        while (in.remaining() >= 8) {
            h = Long.rotateLeft(h ^ round(0, in.getLong()), 27) * P1 + P4;
        }
        if (in.remaining() >= 4) {
            h = Long.rotateLeft(h ^ ((in.getInt() & 0xFFFFFFFFL) * P1), 23) * P2 + P3;
        }
        while (in.hasRemaining()) {
            h = Long.rotateLeft(h ^ ((in.get() & 0xFFL) * P5), 11) * P1;
        }
        h ^= h >>> 33;
        h *= P2;
        h ^= h >>> 29;
        h *= P3;
        return h ^ (h >>> 32);
    }

    private static long round(long acc, long input) {
        return Long.rotateLeft(acc + input * P2, 31) * P1;
    }

    private static long merge(long acc, long value) {
        return (acc ^ round(0, value)) * P1 + P4;
    }
}
