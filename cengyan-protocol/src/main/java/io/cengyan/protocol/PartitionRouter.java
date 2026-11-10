package io.cengyan.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/** xxHash64 v1，seed=0。ID 类型标记和大端数值编码属于持久化协议。 */
public final class PartitionRouter {
    private static final long P1 = 0x9E3779B185EBCA87L;
    private static final long P2 = 0xC2B2AE3D27D4EB4FL;
    private static final long P3 = 0x165667B19E3779F9L;
    private static final long P4 = 0x85EBCA77C2B2AE63L;
    private static final long P5 = 0x27D4EB2F165667C5L;

    private PartitionRouter() { }

    public static int partOf(EntityId id, int partitions) {
        return partOf(Objects.requireNonNull(id).canonicalBytes(), partitions);
    }

    public static int partOf(byte[] canonicalKey, int partitions) {
        if (partitions < 1 || partitions > 65535) {
            throw new IllegalArgumentException("分片数必须在 1 和 65535 之间");
        }
        return (int) Long.remainderUnsigned(hash(canonicalKey), partitions);
    }

    public static long hash(byte[] bytes) {
        Objects.requireNonNull(bytes);
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
            h = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h = merge(merge(merge(merge(h, v1), v2), v3), v4);
        } else {
            h = P5;
        }
        h += bytes.length;
        while (in.remaining() >= 8) {
            h = Long.rotateLeft(h ^ round(0, in.getLong()), 27) * P1 + P4;
        }
        if (in.remaining() >= 4) {
            h = Long.rotateLeft(h ^ (Integer.toUnsignedLong(in.getInt()) * P1), 23) * P2 + P3;
        }
        while (in.hasRemaining()) {
            h = Long.rotateLeft(h ^ (Byte.toUnsignedLong(in.get()) * P5), 11) * P1;
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
