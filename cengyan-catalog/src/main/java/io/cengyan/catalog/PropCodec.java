package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 属性值的固定格式。空位图第 0 位表示第 0 个属性为空。
 * 旧行可以少末尾的可空列，读出来按空处理。
 */
public final class PropCodec {
    public static final int VERSION = 1;
    public static final int MAX_STRING_BYTES = 64 * 1024;

    private PropCodec() {
    }

    public static byte[] encode(int schemaVersion, List<PropDef> properties, Map<String, Object> values) {
        if (properties == null) {
            throw new IllegalArgumentException("属性定义不能为空");
        }
        if (values == null) {
            values = Map.of();
        }
        if (properties.size() > 0xFFFF) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "属性个数超过上限");
        }
        java.util.Set<String> known = new java.util.HashSet<>();
        for (PropDef property : properties) {
            known.add(property.name());
        }
        for (String key : values.keySet()) {
            if (!known.contains(key)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + key);
            }
        }
        List<Object> present = new ArrayList<>(properties.size());
        byte[] bitmap = new byte[(properties.size() + 7) / 8];
        for (int i = 0; i < properties.size(); i++) {
            PropDef property = properties.get(i);
            boolean has = values.containsKey(property.name());
            Object value = has ? values.get(property.name()) : null;
            if (value == null) {
                if (!property.nullable()) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "属性 " + property.name() + " 不能为空");
                }
                bitmap[i / 8] |= (byte) (1 << (i % 8));
                present.add(null);
            } else {
                present.add(check(property, value));
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(estimate(present));
        buffer.put((byte) VERSION);
        buffer.putInt(schemaVersion);
        buffer.putShort((short) properties.size());
        buffer.put(bitmap);
        for (int i = 0; i < properties.size(); i++) {
            Object value = present.get(i);
            if (value != null) {
                writeValue(buffer, properties.get(i).type(), value);
            }
        }
        byte[] encoded = new byte[buffer.position()];
        buffer.flip();
        buffer.get(encoded);
        return encoded;
    }

    public static Map<String, Object> decode(List<PropDef> current, int currentVersion, byte[] raw) {
        if (raw == null || raw.length < 7) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性值太短");
        }
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        int version = buffer.get() & 0xFF;
        if (version != VERSION) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性格式版本不支持：" + version);
        }
        int storedVersion = buffer.getInt();
        int storedCount = buffer.getShort() & 0xFFFF;
        if (storedVersion > currentVersion) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性版本比目录新");
        }
        int extra = current.size() - storedCount;
        int versionGap = currentVersion - storedVersion;
        if (extra != versionGap || storedCount > current.size()) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性列数和版本对不上");
        }
        for (int i = storedCount; i < current.size(); i++) {
            if (!current.get(i).nullable()) {
                throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "新增列必须是可空列");
            }
        }
        int bitmapBytes = (storedCount + 7) / 8;
        if (buffer.remaining() < bitmapBytes) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性空位图不完整");
        }
        byte[] bitmap = new byte[bitmapBytes];
        buffer.get(bitmap);
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < storedCount; i++) {
            PropDef property = current.get(i);
            boolean missing = (bitmap[i / 8] & (1 << (i % 8))) != 0;
            if (missing) {
                values.put(property.name(), null);
            } else {
                values.put(property.name(), readValue(buffer, property.type()));
            }
        }
        for (int i = storedCount; i < current.size(); i++) {
            values.put(current.get(i).name(), null);
        }
        if (buffer.hasRemaining()) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性值后面还有多余字节");
        }
        return values;
    }

    private static Object check(PropDef property, Object value) {
        return switch (property.type()) {
            case BOOL -> {
                if (!(value instanceof Boolean)) {
                    throw mismatch(property, value);
                }
                yield value;
            }
            case INT64 -> {
                if (value instanceof Long || value instanceof Integer || value instanceof Short) {
                    yield ((Number) value).longValue();
                }
                throw mismatch(property, value);
            }
            case DOUBLE -> {
                if (!(value instanceof Double number)) {
                    throw mismatch(property, value);
                }
                if (number.isNaN() || number.isInfinite()) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "浮点数不能是 NaN 或无穷大");
                }
                yield number;
            }
            case STRING -> {
                if (!(value instanceof String text)) {
                    throw mismatch(property, value);
                }
                if (text.getBytes(StandardCharsets.UTF_8).length > MAX_STRING_BYTES) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串最长 64KiB");
                }
                yield text;
            }
            case DATETIME -> {
                if (!(value instanceof Long)) {
                    throw mismatch(property, value);
                }
                yield value;
            }
        };
    }

    private static CengyanException mismatch(PropDef property, Object value) {
        String actual = value == null ? "空" : value.getClass().getSimpleName();
        return new CengyanException(ErrorCode.TYPE_MISMATCH,
                "属性 " + property.name() + " 需要 " + property.type() + "，实际是 " + actual);
    }

    private static void writeValue(ByteBuffer buffer, ValueType type, Object value) {
        switch (type) {
            case BOOL -> buffer.put((byte) ((Boolean) value ? 1 : 0));
            case INT64, DATETIME -> buffer.putLong((Long) value);
            case DOUBLE -> buffer.putDouble((Double) value);
            case STRING -> {
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            }
            default -> throw new CengyanException(ErrorCode.INTERNAL, "未知列类型 " + type);
        }
    }

    private static Object readValue(ByteBuffer buffer, ValueType type) {
        try {
            return switch (type) {
                case BOOL -> {
                    int raw = buffer.get() & 0xFF;
                    if (raw > 1) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "布尔值只能是 0 或 1");
                    }
                    yield raw == 1;
                }
                case INT64, DATETIME -> buffer.getLong();
                case DOUBLE -> buffer.getDouble();
                case STRING -> {
                    int length = buffer.getInt();
                    if (length < 0 || length > MAX_STRING_BYTES || buffer.remaining() < length) {
                        throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "字符串长度不合法");
                    }
                    byte[] bytes = new byte[length];
                    buffer.get(bytes);
                    yield new String(bytes, StandardCharsets.UTF_8);
                }
            };
        } catch (java.nio.BufferUnderflowException ex) {
            throw new CengyanException(ErrorCode.SCHEMA_MISMATCH, "属性值被截断", ex);
        }
    }

    private static int estimate(List<Object> values) {
        int size = 1 + 4 + 2 + (values.size() + 7) / 8;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (value instanceof String text) {
                size += 4 + text.getBytes(StandardCharsets.UTF_8).length;
            } else if (value instanceof Boolean) {
                size += 1;
            } else {
                size += 8;
            }
        }
        return size;
    }
}
