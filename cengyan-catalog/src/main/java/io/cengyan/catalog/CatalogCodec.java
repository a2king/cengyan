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

    static byte[] graphRecord(String name) { return graphRecord(name, null); }

    static byte[] graphRecord(String name, String comment) {
        byte[] utf8 = utf8(name);
        byte[] note = utf8(comment == null ? "" : comment);
        byte[] value = new byte[2 + utf8.length + 1 + 2 + note.length];
        value[0] = (byte) (utf8.length >>> 8);
        value[1] = (byte) utf8.length;
        System.arraycopy(utf8, 0, value, 2, utf8.length);
        value[2 + utf8.length] = 0x7F;
        value[3 + utf8.length] = (byte) (note.length >>> 8);
        value[4 + utf8.length] = (byte) note.length;
        System.arraycopy(note, 0, value, 5 + utf8.length, note.length);
        return value;
    }

    static String graphRecordName(byte[] value) {
        require(value, 2, "图记录");
        int length = ((value[0] & 0xFF) << 8) | (value[1] & 0xFF);
        if (value.length < 2 + length) {
            throw new CengyanException(ErrorCode.INTERNAL, "图记录长度不对");
        }
        return new String(value, 2, length, StandardCharsets.UTF_8);
    }

    static String graphRecordComment(byte[] value) {
        int length = ((value[0] & 0xFF) << 8) | (value[1] & 0xFF);
        int offset = 2 + length;
        if (value.length < offset + 3 || value[offset] != 0x7F) return null;
        int size = ((value[offset + 1] & 0xFF) << 8) | (value[offset + 2] & 0xFF);
        if (value.length != offset + 3 + size) throw new CengyanException(ErrorCode.INTERNAL, "图注释记录长度不对");
        return new String(value, offset + 3, size, StandardCharsets.UTF_8);
    }

    static byte[] typeRecord(
            int schemaVersion, String name, List<PropDef> properties, IdType idType, int idLength) {
        return typeRecord(schemaVersion, name, properties, idType, idLength, null);
    }

    static byte[] typeRecord(
            int schemaVersion, String name, List<PropDef> properties, IdType idType, int idLength, String comment) {
        byte[] nameBytes = utf8(name);
        byte[] note = utf8(comment == null ? "" : comment);
        int size = 4 + 1 + 2 + 2 + nameBytes.length + 2;
        List<byte[]> propertyNames = new ArrayList<>(properties.size());
        for (int i = 0; i < properties.size(); i++) {
            PropDef property = properties.get(i);
            byte[] propertyName = utf8(property.name());
            propertyNames.add(propertyName);
            byte[] propertyComment = utf8(property.comment() == null ? "" : property.comment());
            size += 2 + propertyName.length + 2;
        }
        size += 1 + 2 + note.length + 2;
        for (PropDef property : properties) {
            size += 2 + utf8(property.comment() == null ? "" : property.comment()).length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(schemaVersion);
        buffer.put((byte) idType.code());
        buffer.putShort((short) idLength);
        putText(buffer, nameBytes);
        buffer.putShort((short) properties.size());
        for (int i = 0; i < properties.size(); i++) {
            putText(buffer, propertyNames.get(i));
            buffer.put((byte) properties.get(i).type().code());
            buffer.put((byte) (properties.get(i).nullable() ? 1 : 0));
        }
        buffer.put((byte) 0x7F);
        putText(buffer, note);
        buffer.putShort((short) properties.size());
        for (PropDef property : properties) putText(buffer, utf8(property.comment() == null ? "" : property.comment()));
        return buffer.array();
    }

    static DecodedType decodeType(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value == null ? new byte[0] : value);
        try {
            int schemaVersion = buffer.getInt();
            IdType idType = IdType.fromCode(buffer.get() & 0xFF);
            int idLength = buffer.getShort() & 0xFFFF;
            String name = readText(buffer);
            int count = buffer.getShort() & 0xFFFF;
            List<PropDef> properties = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String propertyName = readText(buffer);
                ValueType type = ValueType.fromCode(buffer.get() & 0xFF);
                boolean nullable = (buffer.get() & 0xFF) == 1;
                properties.add(new PropDef(propertyName, type, nullable));
            }
            String comment = null;
            if (buffer.hasRemaining() && (buffer.get(buffer.position()) & 0xFF) == 0x7F) {
                buffer.get(); comment = readText(buffer);
                int commentCount = buffer.getShort() & 0xFFFF;
                if (commentCount != count) throw new CengyanException(ErrorCode.INTERNAL, "类型注释列数不对");
                for (int i = 0; i < count; i++) {
                    String fieldComment = readText(buffer);
                    PropDef old = properties.get(i);
                    properties.set(i, new PropDef(old.name(), old.type(), old.nullable(), fieldComment.isEmpty() ? null : fieldComment));
                }
            }
            if (buffer.hasRemaining()) {
                throw new CengyanException(ErrorCode.INTERNAL, "类型记录后面还有多余字节");
            }
            return new DecodedType(schemaVersion, name, properties, idType, idLength, comment);
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

    record DecodedType(int schemaVersion, String name, List<PropDef> properties, IdType idType, int idLength, String comment) {
        DecodedType(int schemaVersion, String name, List<PropDef> properties, IdType idType, int idLength) {
            this(schemaVersion, name, properties, idType, idLength, null);
        }
    }
}
