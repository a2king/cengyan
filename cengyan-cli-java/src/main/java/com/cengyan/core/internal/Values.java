package com.cengyan.core.internal;

import java.math.BigInteger;
import java.time.Instant;

/**
 * 结果值的严格类型转换（Record 取值和 POJO 映射共用）。null 原样返回 null；
 * 类型不符或越界时抛 {@link IllegalArgumentException}，不做有损的隐式转换。
 */
public final class Values {
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private Values() {
    }

    public static Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof BigInteger) {
            BigInteger number = (BigInteger) value;
            if (number.compareTo(LONG_MIN) >= 0 && number.compareTo(LONG_MAX) <= 0) return number.longValue();
            throw new IllegalArgumentException("整数超出 long 范围：" + number);
        }
        throw mismatch(value, "整数");
    }

    public static Integer toInt(Object value) {
        Long number = toLong(value);
        if (number == null) return null;
        if (number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) throw new IllegalArgumentException("整数超出 int 范围：" + number);
        return number.intValue();
    }

    public static Short toShort(Object value) {
        Long number = toLong(value);
        if (number == null) return null;
        if (number < Short.MIN_VALUE || number > Short.MAX_VALUE) throw new IllegalArgumentException("整数超出 short 范围：" + number);
        return number.shortValue();
    }

    public static Byte toByte(Object value) {
        Long number = toLong(value);
        if (number == null) return null;
        if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) throw new IllegalArgumentException("整数超出 byte 范围：" + number);
        return number.byteValue();
    }

    /** 整数和小数都可以取为 double（服务端 DOUBLE 列可能返回整数形式）。 */
    public static Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        throw mismatch(value, "数值");
    }

    public static Boolean toBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        throw mismatch(value, "布尔值");
    }

    public static String toText(Object value) {
        if (value == null) return null;
        if (value instanceof String) return (String) value;
        throw mismatch(value, "字符串");
    }

    /** DATETIME 在结果里是 ISO 字符串，按服务端规则解析为 UTC 时刻。 */
    public static Instant toInstant(Object value) {
        if (value == null) return null;
        if (value instanceof Instant) return (Instant) value;
        if (value instanceof String) return DateTimes.parse((String) value);
        throw mismatch(value, "日期时间字符串");
    }

    static IllegalArgumentException mismatch(Object value, String expected) {
        return new IllegalArgumentException("期望" + expected + "，实际是 " + value.getClass().getSimpleName() + "：" + abbreviate(value));
    }

    static String abbreviate(Object value) {
        String text = String.valueOf(value);
        return text.length() > 80 ? text.substring(0, 80) + "..." : text;
    }
}
