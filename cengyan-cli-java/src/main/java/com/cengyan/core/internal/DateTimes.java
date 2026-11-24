package com.cengyan.core.internal;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** 与服务端 DATETIME 相同的解析规则：带 T 的 ISO 文本（无时区视为 UTC），或 {@code yyyy-MM-dd HH:mm:ss}（UTC）。 */
public final class DateTimes {
    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DateTimes() {
    }

    /** 解析失败抛 {@link IllegalArgumentException}。 */
    public static Instant parse(String text) {
        if (text == null) throw new IllegalArgumentException("日期时间不能为空");
        String value = text.trim();
        try {
            if (value.indexOf('T') >= 0) {
                if (!hasZone(value)) value = value + "Z";
                return java.time.OffsetDateTime.parse(value).toInstant();
            }
            return LocalDateTime.parse(value, LOCAL).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("日期时间格式不正确：" + text, ex);
        }
    }

    private static boolean hasZone(String value) {
        if (value.endsWith("Z") || value.endsWith("z")) return true;
        int t = value.indexOf('T');
        return value.indexOf('+', t) >= 0 || value.indexOf('-', t) >= 0;
    }
}
