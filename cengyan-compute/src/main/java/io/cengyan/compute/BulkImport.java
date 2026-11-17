package io.cengyan.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.cengyan.protocol.proto.ClusterProtocol.ImportHeartbeat;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPrepare;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPrepared;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPublish;
import io.cengyan.protocol.proto.ClusterProtocol.Node;
import io.cengyan.protocol.proto.ClusterProtocol.PropertyDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.RpcResponse;
import io.cengyan.protocol.proto.ClusterProtocol.TypeDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.WriteBatch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 批量导入的 Compute 入口。一次导入是一个会话、一个点类型或边类型：
 * <ol>
 *   <li>importStage：上传段的分块转给分片的<b>每个</b>副本，全部收到才算成功；</li>
 *   <li>importPrepare：每个副本把会话的上传段归并成可摄入的文件，全部成功才能发布；</li>
 *   <li>importPublish：一条只带上传段清单的 Raft 指令，各副本在 apply 里原子摄入；</li>
 *   <li>importFinish：删掉各副本的暂存文件。</li>
 * </ol>
 * 先对全部分片做完 importPrepare 再发布，导入内容有问题时一个分片都不会生效。
 */
final class BulkImport {
    private static final ObjectMapper JSON = new ObjectMapper();
    /**
     * 分片成员缓存。原来每个分块都去 Metadata 线性一致读一次成员；Metadata 一慢，导入和认证一起超时。
     * 成员变化时下一个分块会因为副本不在而失败重试，30 秒后读到新成员。
     */
    private static final long MEMBER_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final ConcurrentHashMap<String, Members> MEMBERS = new ConcurrentHashMap<>();
    private record Members(List<Node> nodes, long fetchedAt) { }

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
            case "importPrepare" -> prepare(request);
            case "importPublish", "importRecover" -> publish(request);
            case "importStatus" -> status(request);
            case "importHeartbeat" -> heartbeat(request);
            case "importFinish" -> finish(request);
            case "importCommit" -> throw new ClusterException("UNSUPPORTED", "导入客户端版本过旧，请换用与集群同版本的 cengyan-client");
            default -> throw new ClusterException("UNSUPPORTED", "未知导入操作");
        };
    }

    private ObjectNode plan(ObjectNode request) throws Exception {
        String name = request.path("graph").asText();
        boolean node = request.path("node").asBoolean();
        String typeName = request.path("type").asText();
        GraphDefinition definition = catalog.graph(name);
        DistributedCatalog.requireWritable(definition);
        TypeDefinition type = definition.getTypesList().stream().filter(item -> item.getNode() == node && item.getName().equals(typeName))
                .findFirst().orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "类型不存在：" + typeName));
        if (definition.getIndexesList().stream().anyMatch(index -> index.getNode() == node && index.getTypeName().equals(typeName))) {
            throw new ClusterException("UNSUPPORTED", "类型 " + typeName + " 上有索引，批量导入不维护索引。请先删除索引，导入完成后再创建");
        }
        long count = graph.statistics(definition).getTypesList().stream()
                .filter(item -> item.getNode() == node && item.getType().equals(typeName)).mapToLong(EntityTypeCount::getCount).sum();
        ObjectNode result = ok().put("graphId", definition.getId()).put("generation", definition.getGeneration())
                .put("partitions", definition.getPartitions()).put("vertexIdType", definition.getVertexIdType().name())
                .put("edgeIdType", definition.getEdgeIdType().name()).put("typeId", type.getId()).put("count", count);
        ArrayNode properties = result.putArray("properties");
        for (PropertyDefinition property : type.getPropertiesList()) {
            properties.addObject().put("name", property.getName()).put("type", property.getType()).put("nullable", property.getNullable());
        }
        ArrayNode parts = result.putArray("parts");
        for (int part = 0; part < definition.getPartitions(); part++) {
            ObjectNode item = parts.addObject().put("part", part);
            ArrayNode members = item.putArray("members");
            for (Node member : members(definition.getId(), part)) {
                var address = DistributedCatalog.address(member);
                String host = address.getAddress() == null ? address.getHostString() : address.getAddress().getHostAddress();
                members.addObject().put("nodeId", member.getNodeId()).put("host", host).put("port", address.getPort());
            }
        }
        return result;
    }

    /** 分块必须落到分片的每个副本上。只到了一部分时报错，客户端重发这一块；副本按偏移覆盖，重发是幂等的。 */
    private ObjectNode stage(ObjectNode request) throws Exception {
        byte[] checksum = hex(request.path("checksum").asText());
        byte[] data = Base64.getDecoder().decode(request.path("data").asText());
        var chunk = ImportChunk.newBuilder().setChecksum(ByteString.copyFrom(checksum)).setOffset(request.path("offset").asLong())
                .setData(ByteString.copyFrom(data)).setLast(request.path("last").asBoolean()).setSessionId(request.path("sessionId").asText())
                .setGraphId(request.path("graphId").asLong()).setPartId(request.path("part").asInt()).build().toByteString();
        List<Node> members = members(request.path("graphId").asLong(), request.path("part").asInt());
        everyReplica(members, member -> storage.direct(DistributedCatalog.address(member), "import.stage", chunk, Duration.ofMinutes(2)));
        return ok().put("delivered", members.size());
    }

    /** 每个副本归并这个会话在本分片上的全部上传段。各副本的统计必须一致。 */
    private ObjectNode prepare(ObjectNode request) throws Exception {
        long graphId = request.path("graphId").asLong();
        int part = request.path("part").asInt();
        ImportPublish publish = publishOf(request);
        ByteString body = ImportPrepare.newBuilder().setGraphId(graphId).setPartId(part).setPublish(publish).build().toByteString();
        List<Node> members = members(graphId, part);
        List<RpcResponse> responses = everyReplica(members,
                member -> storage.direct(DistributedCatalog.address(member), "import.prepare", body, Duration.ofMinutes(30)));
        ImportPrepared first = null;
        for (int i = 0; i < responses.size(); i++) {
            ImportPrepared prepared = ImportPrepared.parseFrom(responses.get(i).getBody());
            if (!prepared.getCode().equals("OK")) throw new ClusterException(prepared.getCode(), prepared.getMessage());
            if (first == null) first = prepared;
            else if (first.getRows() != prepared.getRows() || first.getPrimaries() != prepared.getPrimaries()) {
                throw new ClusterException("REPLICA_MISMATCH", "副本 " + members.get(i).getNodeId() + " 归并出的行数与其他副本不同");
            }
        }
        return ok().put("rows", first.getRows()).put("primaries", first.getPrimaries()).put("duplicates", first.getDuplicates())
                .put("bytes", first.getBytes());
    }

    /** 一条 Raft 指令。requestId 由会话、分片和类型确定，重试和 importRecover 都按幂等记录返回首次结果。 */
    private ObjectNode publish(ObjectNode request) throws Exception {
        long graphId = request.path("graphId").asLong();
        int part = request.path("part").asInt();
        ImportPublish publish = publishOf(request);
        if (publish.getSessionId().isBlank()) throw new ClusterException("INVALID_ARGUMENT", "缺少 sessionId");
        String requestId = "import.publish/" + publish.getSessionId() + "/" + part + "/" + (publish.getNode() ? "n" : "e") + publish.getTypeId();
        var command = WriteBatch.newBuilder().setRequestId(requestId).setExpiresAtMs(System.currentTimeMillis() + 86_400_000L)
                .setImportPublish(publish).build();
        var result = storage.write(graphId, part, command, Duration.ofMinutes(30));
        if (!result.getCode().equals("OK")) throw new ClusterException(result.getCode(), result.getMessage());
        return ok().put("affected", result.getAffected()).put("skipped", result.getSkipped()).put("replaced", result.getReplaced());
    }

    private static ImportPublish publishOf(JsonNode request) {
        var publish = ImportPublish.newBuilder().setSessionId(request.path("sessionId").asText())
                .setNode(request.path("node").asBoolean()).setTypeId(request.path("typeId").asInt())
                .setTypeName(request.path("type").asText()).setGeneration(request.path("generation").asLong())
                .setConflict(conflict(request.path("conflict").asText("IMPORT_IGNORE")));
        for (JsonNode run : request.path("runs")) publish.addRuns(ByteString.copyFrom(hex(run.asText())));
        if (publish.getRunsCount() == 0) throw new ClusterException("INVALID_ARGUMENT", "发布清单为空");
        return publish.build();
    }

    private ObjectNode status(ObjectNode request) throws Exception {
        long graphId = request.path("graphId").asLong();
        int part = request.path("part").asInt();
        ObjectNode result = ok().put("graphId", graphId).put("part", part).put("sessionId", request.path("sessionId").asText());
        var replicas = result.putArray("replicas");
        ByteString body = ByteString.copyFrom(JSON.writeValueAsBytes(JSON.createObjectNode().put("graphId", graphId).put("part", part)
                .put("sessionId", request.path("sessionId").asText())));
        for (Node member : members(graphId, part)) {
            ObjectNode item = replicas.addObject().put("nodeId", member.getNodeId());
            try {
                RpcResponse response = storage.direct(DistributedCatalog.address(member), "import.status", body, Duration.ofSeconds(10));
                if (response.getCode().equals("OK")) item.setAll((ObjectNode) JSON.readTree(response.getBody().toByteArray()));
                else item.put("code", response.getCode()).put("message", response.getMessage());
            } catch (RuntimeException ex) {
                item.put("code", "FAILED").put("message", ex.getMessage() == null ? ex.toString() : ex.getMessage());
            }
        }
        return result;
    }

    private ObjectNode heartbeat(ObjectNode request) {
        var beat = ImportHeartbeat.newBuilder().setSessionId(request.path("sessionId").asText()).build().toByteString();
        List<Supplier<Boolean>> calls = new ArrayList<>();
        for (Node node : catalog.nodes()) calls.add(() -> {
            try { return storage.direct(DistributedCatalog.address(node), "import.heartbeat", beat, Duration.ofSeconds(5)).getCode().equals("OK"); }
            catch (RuntimeException ignored) { return false; /* 单个节点心跳失败不中断导入。 */ }
        });
        return ok().put("delivered", StorageGateway.parallel(calls).stream().filter(Boolean::booleanValue).count());
    }

    /** 删掉各节点上这个会话的暂存文件。导入失败时也要调用；abort 表示还没有发出任何发布指令。 */
    private ObjectNode finish(ObjectNode request) {
        var beat = ImportHeartbeat.newBuilder().setSessionId(request.path("sessionId").asText())
                .setAbort(request.path("abort").asBoolean(false)).build().toByteString();
        List<Node> nodes = catalog.nodes();
        List<Supplier<RuntimeException>> calls = new ArrayList<>();
        for (Node node : nodes) calls.add(() -> {
            try {
                var response = storage.direct(DistributedCatalog.address(node), "import.finish", beat, Duration.ofMinutes(10));
                return response.getCode().equals("OK") ? null : new ClusterException(response.getCode(), response.getMessage());
            } catch (RuntimeException ex) { return ex; }
        });
        List<RuntimeException> failures = StorageGateway.parallel(calls);
        long delivered = failures.stream().filter(failure -> failure == null).count();
        if (delivered == 0 && !failures.isEmpty()) throw failures.getFirst();
        return ok().put("delivered", delivered);
    }

    private interface ReplicaCall { RpcResponse call(Node member); }

    /** 并行发给每个副本，全部返回 OK 才算成功；否则报第一个失败的副本。 */
    private static List<RpcResponse> everyReplica(List<Node> members, ReplicaCall call) {
        List<Supplier<Object>> calls = new ArrayList<>();
        for (Node member : members) calls.add(() -> {
            try {
                RpcResponse response = call.call(member);
                return response.getCode().equals("OK") ? response
                        : new ClusterException(response.getCode(), "副本 " + member.getNodeId() + "：" + response.getMessage());
            } catch (ClusterException ex) {
                return new ClusterException(ex.code(), "副本 " + member.getNodeId() + "：" + ex.getMessage());
            } catch (RuntimeException ex) {
                return new ClusterException("FAILED", "副本 " + member.getNodeId() + " 没有响应：" + ex);
            }
        });
        List<RpcResponse> responses = new ArrayList<>();
        for (Object outcome : StorageGateway.parallel(calls)) {
            if (outcome instanceof ClusterException failure) throw failure;
            responses.add((RpcResponse) outcome);
        }
        return responses;
    }

    private static ObjectNode ok() { return JSON.createObjectNode().put("code", "OK"); }

    private List<Node> members(long graphId, int part) throws Exception {
        String key = graphId + "/" + part;
        Members cached = MEMBERS.get(key);
        if (cached != null && System.nanoTime() - cached.fetchedAt() < MEMBER_TTL_NANOS) return cached.nodes();
        ByteString encoded = storage.get(0, 0, StorageGateway.groupKey(graphId, part));
        if (encoded == null) throw new ClusterException("GROUP_UNAVAILABLE", "分片成员不存在：" + part);
        List<Node> nodes = List.copyOf(DataGroupSpec.parseFrom(encoded).getMembersList());
        if (nodes.isEmpty()) throw new ClusterException("GROUP_UNAVAILABLE", "分片没有副本：" + part);
        MEMBERS.put(key, new Members(nodes, System.nanoTime()));
        return nodes;
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
        try { return HexFormat.of().parseHex(text); }
        catch (IllegalArgumentException ex) { throw new ClusterException("INVALID_ARGUMENT", "导入校验和必须是 64 位十六进制"); }
    }
}
