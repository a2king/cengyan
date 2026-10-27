package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 索引键里的属性顺序，以及目录里的索引记录。
 * 整数和浮点按数值有序；字符串只保证等值前缀，不做范围。
 */
public final class IndexCodec {
    private IndexCodec() {
    }

    public static byte[] encode(ValueType type, Object value) {
        if (type == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        if (value == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "空值不进索引");
        }
        return switch (type) {
            case BOOL -> {
                if (!(value instanceof Boolean bit)) {
                    throw mismatch(type, value);
                }
                yield new byte[] {(byte) (bit ? 1 : 0)};
            }
            case INT64, DATETIME -> longBytes(integer(type, value) ^ Long.MIN_VALUE);
            case DOUBLE -> longBytes(ordered(real(value)));
            case STRING -> {
                if (!(value instanceof String text)) {
                    throw mismatch(type, value);
                }
                byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
                if (utf8.length > PropCodec.MAX_STRING_BYTES) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串最长 64KiB");
                }
                byte[] encoded = new byte[4 + utf8.length];
                encoded[0] = (byte) (utf8.length >>> 24);
                encoded[1] = (byte) (utf8.length >>> 16);
                encoded[2] = (byte) (utf8.length >>> 8);
                encoded[3] = (byte) utf8.length;
                System.arraycopy(utf8, 0, encoded, 4, utf8.length);
                yield encoded;
            }
        };
    }

    /** 定长列的编码宽度。字符串长度不固定，返回 -1。 */
    public static int encodedWidth(ValueType type) {
        return switch (type) {
            case BOOL -> 1;
            case INT64, DOUBLE, DATETIME -> 8;
            case STRING -> -1;
        };
    }

    public static boolean rangeable(ValueType type) {
        return type == ValueType.INT64 || type == ValueType.DOUBLE || type == ValueType.DATETIME;
    }

    public static byte[] record(
            String name, int kind, int typeId, int[] propertyIndexes, boolean unique, boolean ready) {
        if (name == null || name.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "索引名不能为空");
        }
        if (kind != Catalog.NODE_KIND && kind != Catalog.REL_KIND) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引种类无效");
        }
        if (propertyIndexes == null || propertyIndexes.length == 0) {
            throw new CengyanException(ErrorCode.SYNTAX, "索引至少要有一列");
        }
        byte[] utf8 = name.getBytes(StandardCharsets.UTF_8);
        if (utf8.length > 128) {
            throw new CengyanException(ErrorCode.SYNTAX, "名字最长 128 字节");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 + utf8.length + 1 + 4 + 2 + propertyIndexes.length * 2 + 2);
        buffer.putShort((short) utf8.length);
        buffer.put(utf8);
        buffer.put((byte) kind);
        buffer.putInt(typeId);
        buffer.putShort((short) propertyIndexes.length);
        for (int index : propertyIndexes) {
            if (index < 0 || index > 0xFFFF) {
                throw new CengyanException(ErrorCode.INTERNAL, "属性下标超出范围");
            }
            buffer.putShort((short) index);
        }
        buffer.put((byte) (unique ? 1 : 0));
        buffer.put((byte) (ready ? 1 : 0));
        return buffer.array();
    }

    public static Decoded decode(byte[] raw) {
        if (raw == null || raw.length < 2) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引记录不完整");
        }
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        int nameLength = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() < nameLength + 1 + 4 + 2 + 2) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引记录不完整");
        }
        byte[] utf8 = new byte[nameLength];
        buffer.get(utf8);
        String name = new String(utf8, StandardCharsets.UTF_8);
        int kind = buffer.get() & 0xFF;
        if (kind != Catalog.NODE_KIND && kind != Catalog.REL_KIND) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引种类无效");
        }
        int typeId = buffer.getInt();
        int count = buffer.getShort() & 0xFFFF;
        if (count == 0 || buffer.remaining() < count * 2 + 2) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引记录不完整");
        }
        int[] propertyIndexes = new int[count];
        for (int i = 0; i < count; i++) {
            propertyIndexes[i] = buffer.getShort() & 0xFFFF;
        }
        int unique = buffer.get() & 0xFF;
        int ready = buffer.get() & 0xFF;
        if ((unique != 0 && unique != 1) || (ready != 0 && ready != 1) || buffer.hasRemaining()) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引记录不完整");
        }
        return new Decoded(name, kind, typeId, propertyIndexes, unique == 1, ready == 1);
    }

    private static long integer(ValueType type, Object value) {
        if (value instanceof Long number) {
            return number;
        }
        throw mismatch(type, value);
    }

    private static double real(Object value) {
        double number;
        if (value instanceof Double found) {
            number = found;
        } else if (value instanceof Long found) {
            number = found.doubleValue();
        } else {
            throw mismatch(ValueType.DOUBLE, value);
        }
        if (!Double.isFinite(number)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "浮点数不能是 NaN 或无穷大");
        }
        return number;
    }

    static long ordered(double number) {
        long bits = number == 0.0d ? 0L : Double.doubleToRawLongBits(number);
        if (bits < 0) {
            return ~bits;
        }
        return bits ^ Long.MIN_VALUE;
    }

    private static byte[] longBytes(long bits) {
        byte[] encoded = new byte[8];
        for (int i = 7; i >= 0; i--) {
            encoded[i] = (byte) bits;
            bits >>>= 8;
        }
        return encoded;
    }

    private static CengyanException mismatch(ValueType type, Object value) {
        String actual = value == null ? "空" : value.getClass().getSimpleName();
        return new CengyanException(ErrorCode.TYPE_MISMATCH, "索引列需要 " + type + "，实际是 " + actual);
    }

    public record Decoded(
            String name, int kind, int typeId, int[] propertyIndexes, boolean unique, boolean ready) {
    }
}
