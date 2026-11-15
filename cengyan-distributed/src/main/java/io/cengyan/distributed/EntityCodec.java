package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.PartitionRouter;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.nio.ByteBuffer;
import java.util.*;

/** 实体主键、邻接键与值的唯一规范编码。类型不参与实体身份。 */
public final class EntityCodec {
    private EntityCodec() { }
    public static Value value(Object value) {
        var builder = Value.newBuilder();
        if (value == null) return builder.setNullValue(true).build();
        if (value instanceof String text) return builder.setStringValue(text).build();
        if (value instanceof Boolean bool) return builder.setBoolValue(bool).build();
        if (value instanceof Float || value instanceof Double) return builder.setDoubleValue(((Number) value).doubleValue()).build();
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) return builder.setIntValue(((Number) value).longValue()).build();
        throw new ClusterException("TYPE_MISMATCH", "不支持的属性类型：" + value.getClass().getSimpleName());
    }
    public static Object object(Value value) {
        return switch (value.getKindCase()) {
            case INT_VALUE -> value.getIntValue();
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BYTES_VALUE -> value.getBytesValue().toByteArray();
            case NULL_VALUE, KIND_NOT_SET -> null;
        };
    }
    public static EntityId id(Value value, IdType type) {
        if (type == IdType.INT64 && value.getKindCase() == Value.KindCase.INT_VALUE) return new EntityId.Int64(value.getIntValue());
        if (type == IdType.STRING && value.getKindCase() == Value.KindCase.STRING_VALUE) return new EntityId.Text(value.getStringValue());
        throw new ClusterException("TYPE_MISMATCH", "实体 ID 必须符合图定义的 " + type + " 类型");
    }
    public static ByteString key(boolean node, Value id, GraphDefinition graph) {
        byte[] encoded = id(id, node ? graph.getVertexIdType() : graph.getEdgeIdType()).canonicalBytes();
        return ByteString.copyFrom(ByteBuffer.allocate(9 + encoded.length).put(node ? (byte) 16 : (byte) 17).putLong(graph.getGeneration()).put(encoded).array());
    }
    public static int part(boolean node, Value id, GraphDefinition graph) {
        return PartitionRouter.partOf(id(id, node ? graph.getVertexIdType() : graph.getEdgeIdType()), graph.getPartitions());
    }
    public static ByteString adjacency(boolean outgoing, Value vertex, Value edge, GraphDefinition graph) {
        byte[] v = id(vertex, graph.getVertexIdType()).canonicalBytes(), e = id(edge, graph.getEdgeIdType()).canonicalBytes();
        return ByteString.copyFrom(ByteBuffer.allocate(13 + v.length + e.length).put(outgoing ? (byte) 18 : (byte) 19)
                .putLong(graph.getGeneration()).putInt(v.length).put(v).put(e).array());
    }
    public static ByteString adjacencyPrefix(boolean outgoing, Value vertex, GraphDefinition graph) {
        byte[] v = id(vertex, graph.getVertexIdType()).canonicalBytes();
        return ByteString.copyFrom(ByteBuffer.allocate(13 + v.length).put(outgoing ? (byte) 18 : (byte) 19).putLong(graph.getGeneration()).putInt(v.length).put(v).array());
    }
    public static Entity entity(ByteString bytes) {
        try { return Entity.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "实体记录损坏", ex); }
    }
    public static ByteString uniqueKey(IndexDefinition index, Entity entity, GraphDefinition graph) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        bytes.writeBytes(indexPrefix(index, graph).toByteArray());
        for (String property : index.getPropertiesList()) {
            Value value = entity.getPropertiesMap().get(property);
            if (value == null || value.getKindCase() == Value.KindCase.NULL_VALUE) return null;
            byte[] encoded = value.toByteArray();
            bytes.writeBytes(ByteBuffer.allocate(4).putInt(encoded.length).array());
            bytes.writeBytes(encoded);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }

    /** 物理索引键前缀；删除/重建索引时不能通过实体值反推完整扫描范围。 */
    public static ByteString indexPrefix(IndexDefinition index, GraphDefinition graph) {
        String identity = index.getIndexId().isBlank() ? index.getName() : index.getIndexId();
        byte[] name = identity.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return ByteString.copyFrom(ByteBuffer.allocate(1 + 8 + 4 + name.length)
                .put((byte) 32).putLong(graph.getGeneration()).putInt(name.length).put(name).array());
    }
    public static ByteString indexEntry(IndexDefinition index, Entity entity, GraphDefinition graph) {
        ByteString key = uniqueKey(index, entity, graph);
        if (key == null || index.getUnique()) return key;
        ByteString id = ByteString.copyFrom(id(entity.getId(), entity.getNode() ? graph.getVertexIdType() : graph.getEdgeIdType()).canonicalBytes());
        return key.concat(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(id.size()).array())).concat(id);
    }
    public static Entity validate(Entity input, GraphDefinition graph) {
        TypeDefinition type = graph.getTypesList().stream().filter(t -> t.getNode() == input.getNode() && t.getName().equals(input.getType()))
                .findFirst().orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "类型不存在：" + input.getType()));
        IdType idType = input.getNode() && type.getIdType() != IdType.ID_UNSPECIFIED ? type.getIdType()
                : input.getNode() ? graph.getVertexIdType() : graph.getEdgeIdType();
        id(input.getId(), idType);
        if (input.getNode()) validateIdLength(input.getId(), idType, type.getIdLength());
        if (!input.getNode()) { id(input.getSource(), graph.getVertexIdType()); id(input.getTarget(), graph.getVertexIdType()); }
        Set<String> declared = new HashSet<>();
        var result = input.toBuilder().clearProperties().setState("ACTIVE");
        for (PropertyDefinition field : type.getPropertiesList()) {
            declared.add(field.getName());
            Value value = input.getPropertiesMap().get(field.getName());
            if (value == null || value.getKindCase() == Value.KindCase.NULL_VALUE) {
                if (!field.getNullable()) throw new ClusterException("TYPE_MISMATCH", "属性不能为空：" + field.getName());
                result.putProperties(field.getName(), value(null));
                continue;
            }
            boolean valid = switch (field.getType().toUpperCase(Locale.ROOT)) {
                case "STRING", "DATETIME" -> value.getKindCase() == Value.KindCase.STRING_VALUE;
                case "INT", "INT64" -> value.getKindCase() == Value.KindCase.INT_VALUE;
                case "DOUBLE" -> value.getKindCase() == Value.KindCase.DOUBLE_VALUE || value.getKindCase() == Value.KindCase.INT_VALUE;
                case "BOOLEAN", "BOOL" -> value.getKindCase() == Value.KindCase.BOOL_VALUE;
                default -> false;
            };
            if (!valid) throw new ClusterException("TYPE_MISMATCH", "属性类型不匹配：" + field.getName());
            if (field.getType().equalsIgnoreCase("DOUBLE") && value.getKindCase() == Value.KindCase.INT_VALUE) value = Value.newBuilder().setDoubleValue(value.getIntValue()).build();
            result.putProperties(field.getName(), value);
        }
        for (String name : input.getPropertiesMap().keySet()) if (!declared.contains(name)) throw new ClusterException("TYPE_MISMATCH", "未定义属性：" + name);
        return result.build();
    }

    private static void validateIdLength(Value value, IdType type, int length) {
        if (length == 0) return;
        if (type == IdType.STRING && value.getStringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > length) {
            throw new ClusterException("TYPE_MISMATCH", "实体 ID 超过类型定义的长度限制");
        }
        if (type == IdType.INT64 && (value.getIntValue() < 0 || Long.toString(value.getIntValue()).length() > length)) {
            throw new ClusterException("TYPE_MISMATCH", "实体 ID 超过类型定义的长度限制");
        }
    }
}
