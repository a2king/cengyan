package io.cengyan.compute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.DistributedGraph;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.protocol.proto.ClusterProtocol.DataGroupSpec;
import io.cengyan.protocol.proto.ClusterProtocol.EntityTypeCount;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.ImportChunk;
import io.cengyan.protocol.proto.ClusterProtocol.ImportConflict;
import io.cengyan.protocol.proto.ClusterProtocol.ImportDirective;
import io.cengyan.protocol.proto.ClusterProtocol.ImportHeartbeat;
import io.cengyan.protocol.proto.ClusterProtocol.Node;
import io.cengyan.protocol.proto.ClusterProtocol.PropertyDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.TypeDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.WriteBatch;
import io.cengyan.protocol.proto.ClusterProtocol.RpcResponse;

import java.time.Duration;
import java.util.Base64;

/** 把客户端的导入文件块转到分片的每个副本，再提交只含校验和的摄入指令。 */
final class BulkImport {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    private final DistributedGraph graph;

    BulkImport(StorageGateway storage, DistributedCatalog catalog, DistributedGraph graphs) {
        this.storage = storage;
        this.catalog = catalog;
        this.graph = graphs;
    }

    ObjectNode handle(ObjectNode request) throws Exception {
        return switch (request.path("op").asText()) {
            case "importPlan" -> plan(request);
            case "importStage" -> stage(request);
            case "importCommit" -> commit(request);
            case "importStatus" -> status(request);
            case "importRecover" -> recover(request);
            case "importHeartbeat" -> heartbeat(request);
            case "importFinish" -> finish(request);
            default -> throw new ClusterException("UNSUPPORTED", "未知导入操作");
        };
    }

    private ObjectNode plan(ObjectNode request) throws Exception {
        String name = request.path("graph").asText();
        boolean node = request.path("node").asBoolean();
        String typeName = request.path("type").asText();
        GraphDefinition definition = catalog.graph(name);
        TypeDefinition type = definition.getTypesList().stream().filter(item -> item.getNode() == node && item.getName().equals(typeName))
                .findFirst().orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "类型不存在：" + typeName));
        long count = graph.statistics(definition).getTypesList().stream()
                .filter(item -> item.getNode() == node && item.getType().equals(typeName)).mapToLong(EntityTypeCount::getCount).sum();
        ObjectNode result = ok().put("graphId", definition.getId()).put("generation", definition.getGeneration())
                .put("partitions", definition.getPartitions()).put("vertexIdType", definition.getVertexIdType().name())
                .put("edgeIdType", definition.getEdgeIdType().name()).put("count", count);
        ArrayNode properties = result.putArray("properties");
        for (PropertyDefinition property : type.getPropertiesList()) {
            properties.addObject().put("name", property.getName()).put("type", property.getType()).put("nullable", property.getNullable());
        }
        ArrayNode parts = result.putArray("parts");
        for (int part = 0; part < definition.getPartitions(); part++) {
            ByteString encoded = storage.get(0, 0, StorageGateway.groupKey(definition.getId(), part));
            if (encoded == null) throw new ClusterException("GROUP_UNAVAILABLE", "分片成员不存在：" + part);
            DataGroupSpec spec = DataGroupSpec.parseFrom(encoded);
            ObjectNode item = parts.addObject().put("part", part);
            ArrayNode members = item.putArray("members");
            for (Node member : spec.getMembersList()) {
                var address = DistributedCatalog.address(member);
                String host = address.getAddress() == null ? address.getHostString() : address.getAddress().getHostAddress();
                members.addObject().put("host", host).put("port", address.getPort());
            }
        }
        return result;
    }

    private ObjectNode stage(ObjectNode request) throws Exception {
        byte[] checksum = hex(request.path("checksum").asText());
        byte[] data = Base64.getDecoder().decode(request.path("data").asText());
        var chunk = ImportChunk.newBuilder().setChecksum(ByteString.copyFrom(checksum)).setOffset(request.path("offset").asLong())
                .setData(ByteString.copyFrom(data)).setLast(request.path("last").asBoolean()).setSessionId(request.path("sessionId").asText())
                .setGraphId(request.path("graphId").asLong()).setPartId(request.path("part").asInt()).build();
        int delivered = 0;
        RuntimeException failure = null;
        for (Node member : members(chunk.getGraphId(), chunk.getPartId())) {
            try {
                var response = storage.direct(DistributedCatalog.address(member), "import.stage", chunk.toByteString(), Duration.ofMinutes(2));
                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
                delivered++;
            } catch (RuntimeException ex) {
                failure = ex;
            }
        }
        if (delivered == 0) throw failure == null ? new ClusterException("GROUP_UNAVAILABLE", "没有副本接收导入文件") : failure;
        return ok().put("delivered", delivered);
    }

    private ObjectNode commit(ObjectNode request) throws Exception {
        long graphId = request.path("graphId").asLong();
        int part = request.path("part").asInt();
        String session = request.path("sessionId").asText();
        byte[] checksum = hex(request.path("checksum").asText());
        var directive = ImportDirective.newBuilder().setChecksum(ByteString.copyFrom(checksum))
                .setConflict(conflict(request.path("conflict").asText("IMPORT_IGNORE")))
                .setGeneration(request.path("generation").asLong()).build();
        String requestId = session + "-" + part + "-" + request.path("checksum").asText("").substring(0, 12);
        var command = WriteBatch.newBuilder().setRequestId(requestId).setExpiresAtMs(System.currentTimeMillis() + 86_400_000L)
                .setImportFile(directive).build();
        var result = storage.write(graphId, part, command, Duration.ofMinutes(30));
        if (!result.getCode().equals("OK")) throw new ClusterException(result.getCode(), result.getMessage());
        return ok().put("affected", result.getAffected());
    }

    private ObjectNode status(ObjectNode request) throws Exception {
        long graphId = request.path("graphId").asLong();
        int part = request.path("part").asInt();
        String checksum = request.path("checksum").asText();
        hex(checksum);
        ObjectNode result = ok().put("graphId", graphId).put("part", part).put("checksum", checksum);
        var replicas = result.putArray("replicas");
        for (Node member : members(graphId, part)) {
            ObjectNode item = replicas.addObject().put("nodeId", member.getNodeId());
            try {
                RpcResponse response = storage.direct(DistributedCatalog.address(member), "import.status",
                        ByteString.copyFrom(JSON.writeValueAsBytes(request)), Duration.ofSeconds(10));
                if (response.getCode().equals("OK")) item.setAll((ObjectNode) JSON.readTree(response.getBody().toByteArray()));
                else item.put("code", response.getCode()).put("message", response.getMessage());
            } catch (RuntimeException ex) {
                item.put("code", "FAILED").put("message", ex.getMessage() == null ? ex.toString() : ex.getMessage());
            }
        }
        return result;
    }

    private ObjectNode recover(ObjectNode request) throws Exception {
        if (request.path("sessionId").asText().isBlank()) throw new ClusterException("INVALID_ARGUMENT", "缺少 sessionId");
        // importCommit 在服务端按 sessionId-part-checksum 前缀生成稳定 requestId，重复调用是幂等的。
        return commit(request);
    }

    private ObjectNode heartbeat(ObjectNode request) {
        var beat = ImportHeartbeat.newBuilder().setSessionId(request.path("sessionId").asText()).build().toByteString();
        int delivered = 0;
        for (Node node : catalog.nodes()) {
            try {
                var response = storage.direct(DistributedCatalog.address(node), "import.heartbeat", beat, Duration.ofSeconds(5));
                if (response.getCode().equals("OK")) delivered++;
            } catch (RuntimeException ignored) { /* 单个副本心跳失败不中断导入。 */ }
        }
        return ok().put("delivered", delivered);
    }

    private ObjectNode finish(ObjectNode request) {
        var beat = ImportHeartbeat.newBuilder().setSessionId(request.path("sessionId").asText()).build().toByteString();
        int delivered = 0;
        RuntimeException failure = null;
        for (Node node : catalog.nodes()) {
            try {
                var response = storage.direct(DistributedCatalog.address(node), "import.finish", beat, Duration.ofMinutes(30));
                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
                delivered++;
            } catch (RuntimeException ex) { failure = ex; }
        }
        if (delivered == 0 && failure != null) throw failure;
        return ok().put("delivered", delivered);
    }

    private static ObjectNode ok() {
        return new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("code", "OK");
    }

    private java.util.List<Node> members(long graphId, int part) throws Exception {
        ByteString encoded = storage.get(0, 0, StorageGateway.groupKey(graphId, part));
        if (encoded == null) throw new ClusterException("GROUP_UNAVAILABLE", "分片成员不存在：" + part);
        return DataGroupSpec.parseFrom(encoded).getMembersList();
    }

    private static ImportConflict conflict(String name) {
        return switch (name) {
            case "IMPORT_REPLACE", "replace" -> ImportConflict.IMPORT_REPLACE;
            case "IMPORT_ERROR", "error" -> ImportConflict.IMPORT_ERROR;
            default -> ImportConflict.IMPORT_IGNORE;
        };
    }

    private static byte[] hex(String text) {
        if (text.length() != 64) throw new ClusterException("INVALID_ARGUMENT", "导入校验和必须是 64 位十六进制");
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(text.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }
}
