package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** 目录列族里的记录格式。键由 KeyCodec 负责，这里只编值。 */
final class CatalogCodec {
    private CatalogCodec() {
    }

    static byte[] nameValue(int kind, int graphId) {
        byte[] value = new byte[3];
        value[0] = (byte) kind;
        value[1] = (byte) (graphId >>> 8);
        value[2] = (byte) graphId;
        return value;
    }

    static int nameKind(byte[] value) {
        require(value, 3, "名字记录");
        return value[0] & 0xFF;
    }

    static int nameGraphId(byte[] value) {
        require(value, 3, "名字记录");
        return ((value[1] & 0xFF) << 8) | (value[2] & 0xFF);
    }

    static byte[] graphRecord(String name) {
        byte[] utf8 = utf8(name);
        byte[] value = new byte[2 + utf8.length];
        value[0] = (byte) (utf8.length >>> 8);
        value[1] = (byte) utf8.length;
        System.arraycopy(utf8, 0, value, 2, utf8.length);
        return value;
    }

    static String graphRecordName(byte[] value) {
        require(value, 2, "图记录");
        int length = ((value[0] & 0xFF) << 8) | (value[1] & 0xFF);
        if (value.length != 2 + length) {
            throw new CengyanException(ErrorCode.INTERNAL, "图记录长度不对");
        }
        return new String(value, 2, length, StandardCharsets.UTF_8);
    }

    static byte[] typeRecord(int schemaVersion, String name, List<PropDef> properties) {
        byte[] nameBytes = utf8(name);
        int size = 4 + 2 + nameBytes.length + 2;
        List<byte[]> propertyNames = new ArrayList<>(properties.size());
        for (PropDef property : properties) {
            byte[] propertyName = utf8(property.name());
            propertyNames.add(propertyName);
            size += 2 + propertyName.length + 2;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(schemaVersion);
        putText(buffer, nameBytes);
        buffer.putShort((short) properties.size());
        for (int i = 0; i < properties.size(); i++) {
            putText(buffer, propertyNames.get(i));
            buffer.put((byte) properties.get(i).type().code());
            buffer.put((byte) (properties.get(i).nullable() ? 1 : 0));
        }
        return buffer.array();
    }

    static DecodedType decodeType(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value == null ? new byte[0] : value);
        try {
            int schemaVersion = buffer.getInt();
            String name = readText(buffer);
            int count = buffer.getShort() & 0xFFFF;
            List<PropDef> properties = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String propertyName = readText(buffer);
                ValueType type = ValueType.fromCode(buffer.get() & 0xFF);
                boolean nullable = (buffer.get() & 0xFF) == 1;
                properties.add(new PropDef(propertyName, type, nullable));
            }
            if (buffer.hasRemaining()) {
                throw new CengyanException(ErrorCode.INTERNAL, "类型记录后面还有多余字节");
            }
            return new DecodedType(schemaVersion, name, properties);
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "类型记录损坏", ex);
        }
    }

    static int readUint16(byte[] value) {
        if (value == null) {
            return 0;
        }
        require(value, 2, "计数");
        return ((value[0] & 0xFF) << 8) | (value[1] & 0xFF);
    }

    static byte[] writeUint16(int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new CengyanException(ErrorCode.INTERNAL, "计数超出 uint16");
        }
        return new byte[] {(byte) (value >>> 8), (byte) value};
    }

    static int readUint32(byte[] value) {
        if (value == null) {
            return 0;
        }
        require(value, 4, "计数");
        return ByteBuffer.wrap(value).getInt();
    }

    static byte[] writeUint32(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static void putText(ByteBuffer buffer, byte[] utf8) {
        if (utf8.length > 0xFFFF) {
            throw new CengyanException(ErrorCode.SYNTAX, "名字最长 128 字节");
        }
        buffer.putShort((short) utf8.length);
        buffer.put(utf8);
    }

    private static String readText(ByteBuffer buffer) {
        int length = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() < length) {
            throw new java.nio.BufferUnderflowException();
        }
        byte[] utf8 = new byte[length];
        buffer.get(utf8);
        return new String(utf8, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String text) {
        if (text == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "名字不能为空");
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void require(byte[] value, int length, String what) {
        if (value == null || value.length < length) {
            throw new CengyanException(ErrorCode.INTERNAL, what + "不完整");
        }
    }

    record DecodedType(int schemaVersion, String name, List<PropDef> properties) {
    }
}
