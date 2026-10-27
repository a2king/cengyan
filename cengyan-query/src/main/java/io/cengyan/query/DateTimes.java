package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 日期时间按 UTC 解析。没有时区的文本也当作 UTC。 */
final class DateTimes {
    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DateTimes() {
    }

    static long parse(String text) {
        if (text == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "日期时间不能为空");
        }
        String value = text.trim();
        try {
            if (value.contains("T")) {
                if (!hasZone(value)) {
                    value = value + "Z";
                }
                return Instant.parse(value).toEpochMilli();
            }
            return LocalDateTime.parse(value, LOCAL).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException ex) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "日期时间格式不正确");
        }
    }

    static String format(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    static Object plain(Object value) {
        if (value instanceof UtcTime time) {
            return format(time.millis());
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), plain(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(plain(item));
            }
            return copy;
        }
        return value;
    }

    private static boolean hasZone(String value) {
        return value.endsWith("Z") || value.contains("+") || value.lastIndexOf('-') > 9;
    }
}
