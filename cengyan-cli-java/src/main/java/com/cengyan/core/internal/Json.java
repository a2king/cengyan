package com.cengyan.core.internal;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 协议 JSON 的唯一编解码入口。发送和字节计量共用这里的规则，保证"计量多少"就是"发出多少"。
 *
 * <p>Java → JSON：
 * <ul>
 *   <li>{@code null}、{@code String}、{@code Character}、{@code Boolean} 原样；</li>
 *   <li>{@code Byte/Short/Integer/Long} 原样；{@code BigInteger} 必须在 long 范围内（服务端 INT64）；</li>
 *   <li>{@code Float/Double} 原样，NaN/Infinity 不是合法 JSON，直接拒绝；</li>
 *   <li>{@code BigDecimal}、{@code UUID} 写成字符串，{@code Enum} 写成 {@code name()}；</li>
 *   <li>{@code Instant/OffsetDateTime/ZonedDateTime/Date} 写成 UTC 毫秒精度的 ISO 字符串（{@code Instant.toString()}）；
 *       {@code LocalDateTime} 按 UTC 处理（与服务端一致：无时区的时间视为 UTC）；{@code LocalDate} 写成 {@code yyyy-MM-dd}；</li>
 *   <li>{@code Map}（键必须是 String）写成对象，{@code Collection} 和数组写成数组（{@code byte[]} 除外）；</li>
 *   <li>其余类型以 {@link IllegalArgumentException} 拒绝，不做隐式 {@code toString()}。</li>
 * </ul>
 *
 * <p>JSON → Java：整数为 {@code Long}（超出 long 时为 {@code BigInteger}），小数为 {@code Double}，
 * 对象为 {@code LinkedHashMap}，数组为 {@code ArrayList}。
 */
public final class Json {
    private static final JsonFactory FACTORY = JsonFactory.builder()
            // 结果大小由服务端 resultMaxBytes 约束，客户端不再额外限制单个字符串长度。
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
            .build();
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private Json() {
    }

    /** 编码为紧凑的 UTF-8 JSON。不支持的值抛 {@link IllegalArgumentException}。 */
    public static byte[] encode(Object value) {
        return encode(value, true);
    }

    /**
     * 导出结果用的编码：与线上编码相同，但允许超出 long 的整数（服务端结果里可能出现，
     * 只是不能再作为参数发回去）。
     */
    public static byte[] encodeForExport(Object value) {
        return encode(value, false);
    }

    private static byte[] encode(Object value, boolean wire) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        try (JsonGenerator generator = FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
            write(generator, value, 0, wire);
        } catch (IOException ex) {
            throw new IllegalArgumentException("无法编码为 JSON：" + ex.getMessage(), ex);
        }
        return out.toByteArray();
    }

    /** 编码后的 UTF-8 字节数。 */
    public static int encodedSize(Object value) {
        return encode(value).length;
    }

    /** 解析一段 JSON。格式错误抛 {@link IllegalArgumentException}。 */
    public static Object decode(byte[] bytes, int offset, int length) {
        try (JsonParser parser = FACTORY.createParser(bytes, offset, length)) {
            JsonToken token = parser.nextToken();
            if (token == null) throw new IllegalArgumentException("空的 JSON 文本");
            Object value = read(parser, token);
            if (parser.nextToken() != null) throw new IllegalArgumentException("JSON 文本后有多余内容");
            return value;
        } catch (IOException ex) {
            throw new IllegalArgumentException("无效的 JSON：" + ex.getMessage(), ex);
        }
    }

    public static Object decode(byte[] bytes) {
        return decode(bytes, 0, bytes.length);
    }

    /** 时间值的线上形式：UTC、毫秒精度。 */
    public static String instant(Instant value) {
        return value.truncatedTo(ChronoUnit.MILLIS).toString();
    }

    private static void write(JsonGenerator out, Object value, int depth, boolean wire) throws IOException {
        if (depth > 256) throw new IllegalArgumentException("值嵌套过深（可能存在循环引用）");
        if (value == null) {
            out.writeNull();
        } else if (value instanceof String) {
            out.writeString(checkText((String) value));
        } else if (value instanceof Character) {
            out.writeString(checkText(String.valueOf(value)));
        } else if (value instanceof Boolean) {
            out.writeBoolean((Boolean) value);
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            out.writeNumber(((Number) value).longValue());
        } else if (value instanceof Double) {
            double number = (Double) value;
            if (Double.isNaN(number) || Double.isInfinite(number)) throw new IllegalArgumentException("NaN/Infinity 不是合法的 JSON 数值");
            out.writeNumber(number);
        } else if (value instanceof Float) {
            float number = (Float) value;
            if (Float.isNaN(number) || Float.isInfinite(number)) throw new IllegalArgumentException("NaN/Infinity 不是合法的 JSON 数值");
            out.writeNumber(number);
        } else if (value instanceof BigInteger) {
            BigInteger number = (BigInteger) value;
            if (number.compareTo(LONG_MIN) >= 0 && number.compareTo(LONG_MAX) <= 0) out.writeNumber(number.longValue());
            else if (wire) throw new IllegalArgumentException("整数超出 INT64 范围：" + number);
            else out.writeNumber(number);
        } else if (value instanceof BigDecimal) {
            out.writeString(((BigDecimal) value).toPlainString());
        } else if (value instanceof UUID) {
            out.writeString(value.toString());
        } else if (value instanceof Enum) {
            out.writeString(((Enum<?>) value).name());
        } else if (value instanceof Instant) {
            out.writeString(instant((Instant) value));
        } else if (value instanceof OffsetDateTime) {
            out.writeString(instant(((OffsetDateTime) value).toInstant()));
        } else if (value instanceof ZonedDateTime) {
            out.writeString(instant(((ZonedDateTime) value).toInstant()));
        } else if (value instanceof LocalDateTime) {
            out.writeString(instant(((LocalDateTime) value).toInstant(ZoneOffset.UTC)));
        } else if (value instanceof LocalDate) {
            out.writeString(value.toString());
        } else if (value instanceof Date) {
            // java.sql.Date 不支持 toInstant()，统一按毫秒数转换。
            out.writeString(instant(Instant.ofEpochMilli(((Date) value).getTime())));
        } else if (value instanceof Map) {
            out.writeStartObject();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("JSON 对象的键必须是 String：" + entry.getKey());
                }
                out.writeFieldName(checkText((String) entry.getKey()));
                write(out, entry.getValue(), depth + 1, wire);
            }
            out.writeEndObject();
        } else if (value instanceof Collection) {
            out.writeStartArray();
            for (Object item : (Collection<?>) value) write(out, item, depth + 1, wire);
            out.writeEndArray();
        } else if (value.getClass().isArray() && !(value instanceof byte[])) {
            out.writeStartArray();
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) write(out, Array.get(value, i), depth + 1, wire);
            out.writeEndArray();
        } else {
            throw new IllegalArgumentException("不支持的值类型：" + value.getClass().getName()
                    + "（支持字符串、布尔、整数、浮点、时间、UUID、BigDecimal、枚举、Map、List；POJO 请用批量写入或先转成 Map）");
        }
    }

    /** 孤立的 UTF-16 代理字符无法编码为合法 UTF-8；服务端也拒绝这种 ID。 */
    static String checkText(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i))) {
                    throw new IllegalArgumentException("字符串包含无效的 Unicode 代理字符");
                }
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException("字符串包含无效的 Unicode 代理字符");
            }
        }
        return text;
    }

    private static Object read(JsonParser parser, JsonToken token) throws IOException {
        switch (token) {
            case START_OBJECT: {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_OBJECT; next = parser.nextToken()) {
                    String name = parser.currentName();
                    map.put(name, read(parser, parser.nextToken()));
                }
                return map;
            }
            case START_ARRAY: {
                List<Object> list = new ArrayList<Object>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_ARRAY; next = parser.nextToken()) {
                    list.add(read(parser, next));
                }
                return list;
            }
            case VALUE_STRING:
                return parser.getText();
            case VALUE_NUMBER_INT:
                if (parser.getNumberType() == JsonParser.NumberType.BIG_INTEGER) return parser.getBigIntegerValue();
                return parser.getLongValue();
            case VALUE_NUMBER_FLOAT:
                return parser.getDoubleValue();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NULL:
                return null;
            default:
                throw new IllegalArgumentException("意外的 JSON 记号：" + token);
        }
    }
}
