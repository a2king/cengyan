package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

/** Metadata Raft 中的图定义和持久化建图过程，读取不依赖 Compute 本地目录。 */
public final class DistributedCatalog {
    private final StorageGateway storage;
    public DistributedCatalog(StorageGateway storage) { this.storage = storage; }
    public static ByteString graphKey(String name) { return TransactionEngine.text("catalog/graph/" + name); }

    public GraphDefinition graph(String name) {
        ByteString bytes = storage.get(0, 0, graphKey(name));
        if (bytes == null) {
            ByteString alias = storage.get(0, 0, TransactionEngine.text("catalog/alias/" + name));
            if (alias != null) bytes = storage.get(0, 0, graphKey(alias.toStringUtf8()));
        }
        if (bytes == null) throw new ClusterException("GRAPH_NOT_FOUND", "图不存在：" + name);
        try {
            GraphDefinition graph = GraphDefinition.parseFrom(bytes);
            if (graph.getState().equals("DROPPED")) throw new ClusterException("GRAPH_NOT_FOUND", "图已删除：" + name);
            return graph;
        }
        catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "图元数据损坏", ex); }
    }
    public List<GraphDefinition> graphs() {
        List<GraphDefinition> result = new ArrayList<>();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        try {
            do {
                ScanPage page = storage.scan(0, 0, TransactionEngine.text("catalog/graph/"), after, 100, cursor);
                for (var item : page.getEntriesList()) {
                    GraphDefinition graph = GraphDefinition.parseFrom(item.getValue());
                    if (!graph.getState().equals("DROPPED")) result.add(graph);
                }
                if (!page.getMore()) break;
                after = page.getLastKey();
                cursor = page.getCursorId();
            } while (true);
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "图目录损坏", ex); }
        return List.copyOf(result);
    }

    public GraphDefinition create(String requestId, String name, int partitions, int replicas, IdType vertexType, IdType edgeType) {
        return create(requestId, name, partitions, replicas, vertexType, edgeType, null);
    }

    public GraphDefinition create(String requestId, String name, int partitions, int replicas,
                                   IdType vertexType, IdType edgeType, String comment) {
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}") || partitions < 1 || partitions > 65535
                || replicas < 1 || vertexType == IdType.ID_UNSPECIFIED || edgeType == IdType.ID_UNSPECIFIED) {
            throw new ClusterException("INVALID_ARGUMENT", "图名、分片、副本或 ID 类型无效");
        }
        List<Node> nodes = placementNodes();
        if (replicas > nodes.size()) throw new ClusterException("INSUFFICIENT_NODES", "可用存储节点不足以满足副本数");
        ByteString key = graphKey(name);
        if (storage.get(0, 0, TransactionEngine.text("catalog/alias/" + name)) != null) throw new ClusterException("NAME_CONFLICT", "图名已被别名占用");
        ByteString existing = storage.get(0, 0, key);
        GraphDefinition definition;
        if (existing != null) {
            GraphDefinition stored = parseGraph(existing);
            if (stored.getState().equals("DROPPED")) {
                // DROP 只逻辑删除目录，旧 Data Group/历史版本仍受租约和 GC 管理。
                // 同名重建必须分配新 graphId，绝不能复用旧分片的 Raft 身份。
                    definition = allocate(name, requestId, partitions, replicas, vertexType, edgeType, comment, existing);
            } else {
                definition = graph(name);
                ByteString owner = storage.get(0, 0, TransactionEngine.text("catalog/createOwner/" + name));
                if (!definition.getState().equals("CREATING")) {
                    if (owner != null && owner.toStringUtf8().equals(requestId)) return definition;
                    throw new ClusterException("NAME_CONFLICT", "图已存在：" + name);
                }
                // 建图是一个可恢复的控制任务。请求在逐分片 addGroup 期间失败后，
                // 后续请求可以继续同一个 CREATING 定义，但不能以不同参数覆盖原计划。
                if (definition.getPartitions() != partitions || definition.getReplicas() != replicas
                        || definition.getVertexIdType() != vertexType || definition.getEdgeIdType() != edgeType) {
                    throw new ClusterException("REQUEST_CONFLICT", "同名图正在以不同参数创建");
                }
            }
        } else {
            definition = allocate(name, requestId, partitions, replicas, vertexType, edgeType, comment, null);
        }
        for (int part = 0; part < definition.getPartitions(); part++) {
            String mode = nodes.stream().allMatch(node -> node.getDataMode().equals("rebuild")) ? "rebuild" : "raft";
            if (nodes.stream().anyMatch(node -> node.getDataMode().equals("rebuild")) && !mode.equals("rebuild"))
                throw new ClusterException("CONFIG_MISMATCH", "不能混用 raft/rebuild Storage 模式");
            DataGroupSpec.Builder spec = DataGroupSpec.newBuilder().setGraphId(definition.getId()).setPartId(part).setDataMode(mode);
            Map<String, Integer> rotation = new HashMap<>();
            for (int i = 0; i < nodes.size(); i++) rotation.put(nodes.get(i).getNodeId(), Math.floorMod(i - part, nodes.size()));
            for (int r = 0; r < definition.getReplicas(); r++) spec.addMembers(ReplicaPlacement.choose(nodes, spec.getMembersList(), rotation));
            ByteString routeKey = StorageGateway.groupKey(definition.getId(), part);
            ByteString route = storage.get(0, 0, routeKey);
            if (route != null) {
                try { spec = DataGroupSpec.parseFrom(route).toBuilder(); }
                catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "图分片任务损坏", ex); }
            } else {
                storage.write(0, 0, StorageGateway.batch("graph.route/" + definition.getId() + "/" + part)
                        .addConditions(StorageGateway.condition(routeKey, null)).addMutations(TransactionEngine.put(routeKey, spec.build().toByteString())).build());
            }
            for (Node node : spec.getMembersList()) {
                RpcResponse response = storage.direct(address(node), "data.addGroup", spec.build().toByteString(), Duration.ofSeconds(20));
                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
            }
            storage.route(definition.getId(), part, spec.getMembersList().stream().map(DistributedCatalog::address).toList());
        }
        GraphDefinition active = definition.toBuilder().setState("ACTIVE").build();
        try {
            storage.write(0, 0, StorageGateway.batch("graph.activate/" + definition.getId())
                    .addConditions(StorageGateway.condition(key, definition.toByteString())).addMutations(TransactionEngine.put(key, active.toByteString())).build());
            return active;
        } catch (ClusterException ex) {
            if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
            GraphDefinition current = graph(name);
            if (current.getId() == definition.getId() && current.getState().equals("ACTIVE")
                    && current.getPartitions() == partitions && current.getReplicas() == replicas
                    && current.getVertexIdType() == vertexType && current.getEdgeIdType() == edgeType) return current;
            throw ex;
        }
    }

    private GraphDefinition allocate(String name, String requestId, int partitions, int replicas,
                                     IdType vertexType, IdType edgeType, String comment, ByteString expectedGraph) {
        ByteString graph = graphKey(name);
        ByteString allocation = TransactionEngine.text("catalog/nextGraphId");
        ByteString expected = expectedGraph;
        List<Node> storageNodes = nodes();
        String mode = storageNodes.stream().allMatch(node -> node.getDataMode().equals("rebuild")) ? "rebuild" : "raft";
        if (storageNodes.stream().anyMatch(node -> node.getDataMode().equals("rebuild")) && !mode.equals("rebuild"))
            throw new ClusterException("CONFIG_MISMATCH", "不能混用 raft/rebuild Storage 模式");
        for (;;) {
            ByteString counter = storage.get(0, 0, allocation);
            long next = counter == null ? 1 : Long.parseLong(counter.toStringUtf8());
            GraphDefinition definition = GraphDefinition.newBuilder().setId(next).setName(name)
                    .setPartitions(partitions).setReplicas(replicas).setVertexIdType(vertexType).setEdgeIdType(edgeType)
                    .setSchemaVersion(1).setState("CREATING").setDataMode(mode).setComment(comment == null ? "" : comment).build();
            try {
                storage.write(0, 0, StorageGateway.batch("graph.allocate/" + requestId + "/" + next)
                        .addConditions(StorageGateway.condition(graph, expected))
                        .addConditions(StorageGateway.condition(allocation, counter))
                        .addConditions(StorageGateway.condition(TransactionEngine.text("catalog/alias/" + name), null))
                        .addMutations(TransactionEngine.put(graph, definition.toByteString()))
                        .addMutations(TransactionEngine.put(TransactionEngine.text("catalog/createOwner/" + name), TransactionEngine.text(requestId)))
                        .addMutations(TransactionEngine.put(allocation, TransactionEngine.text(Long.toString(next + 1)))).build());
                return definition;
            } catch (ClusterException ex) {
                if (!ex.code().equals("WRITE_CONFLICT")) throw ex;
                ByteString current = storage.get(0, 0, graph);
                if (current != null) {
                    GraphDefinition currentDefinition = parseGraph(current);
                    if (!currentDefinition.getState().equals("DROPPED")) return graph(name);
                    expected = current;
                } else expected = null;
            }
        }
    }

    private static GraphDefinition parseGraph(ByteString bytes) {
        try { return GraphDefinition.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "图元数据损坏", ex); }
    }

    public GraphDefinition addType(String requestId, String graphName, TypeDefinition type) {
        GraphDefinition before = graph(graphName);
        requireWritable(before);
        if (type.getName().isBlank() || before.getTypesList().stream().anyMatch(t -> t.getNode() == type.getNode() && t.getName().equals(type.getName()))) {
            throw new ClusterException("NAME_CONFLICT", "类型已存在或名称为空");
        }
        GraphDefinition graph = before;
        TypeDefinition.Builder normalized = type.toBuilder();
        if (type.getNode()) {
            IdType requested = type.getIdType() == IdType.ID_UNSPECIFIED ? graph.getVertexIdType() : type.getIdType();
            if (requested == IdType.ID_UNSPECIFIED) requested = IdType.INT64;
            boolean hasNodeType = graph.getTypesList().stream().anyMatch(TypeDefinition::getNode);
            if (hasNodeType && requested != graph.getVertexIdType()) {
                throw new ClusterException("REQUEST_CONFLICT", "同一张图的节点类型必须使用相同的 ID 类型");
            }
            if (!hasNodeType && requested != graph.getVertexIdType()) {
                // 空图的第一个节点类型可以确定图级 vertex ID 编码；已有数据不能这样改编码。
                graph = graph.toBuilder().setVertexIdType(requested).build();
            }
            int length = type.getIdLength() == 0 ? (requested == IdType.STRING ? 4096 : 19) : type.getIdLength();
            validateIdLength(requested, length);
            normalized.setIdType(requested).setIdLength(length);
        }
        if (normalized.getId() == 0) {
            int nextId = graph.getTypesList().stream().mapToInt(TypeDefinition::getId).max().orElse(0) + 1;
            normalized.setId(nextId);
        }
        Set<String> fields = new HashSet<>();
        for (var field : type.getPropertiesList()) if (!field.getName().matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}") || field.getName().equals("id") || !fields.add(field.getName())
                || !Set.of("STRING", "INT64", "INT", "DOUBLE", "BOOLEAN", "BOOL", "DATETIME").contains(field.getType().toUpperCase(Locale.ROOT))) {
            throw new ClusterException("INVALID_ARGUMENT", "属性定义不合法");
        }
        GraphDefinition next = graph.toBuilder().addTypes(normalized.build()).setSchemaVersion(graph.getSchemaVersion() + 1).build();
        update(requestId, before, next);
        return next;
    }

    private static void validateIdLength(IdType type, int length) {
        int max = type == IdType.INT64 ? 19 : 4096;
        if (length < 1 || length > max) throw new ClusterException("INVALID_ARGUMENT", "ID 长度必须在 1 到 " + max + " 之间");
    }
    public void createAlias(String requestId, String alias, String target) {
        if (alias == null || !alias.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) throw new ClusterException("INVALID_ARGUMENT", "别名格式无效");
        GraphDefinition graph = graph(target);
        if (storage.get(0, 0, graphKey(alias)) != null) throw new ClusterException("NAME_CONFLICT", "图名与别名冲突");
        ByteString key = TransactionEngine.text("catalog/alias/" + alias);
        storage.write(0, 0, StorageGateway.batch("alias.create/" + requestId)
                .addConditions(StorageGateway.condition(key, null)).addConditions(StorageGateway.condition(graphKey(alias), null))
                .addMutations(TransactionEngine.put(key, TransactionEngine.text(graph.getName()))).build());
    }
    public List<List<Object>> aliases() {
        List<List<Object>> rows = new ArrayList<>(); ByteString after = ByteString.EMPTY; String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("catalog/alias/"), after, 100, cursor);
            for (var entry : page.getEntriesList()) rows.add(List.of(entry.getKey().toStringUtf8().substring("catalog/alias/".length()), entry.getValue().toStringUtf8()));
            if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
        } while (true);
        return List.copyOf(rows);
    }
    public void dropAlias(String requestId, String alias) {
        ByteString key = TransactionEngine.text("catalog/alias/" + alias), current = storage.get(0, 0, key);
        if (current == null) throw new ClusterException("GRAPH_NOT_FOUND", "别名不存在");
        storage.write(0, 0, StorageGateway.batch("alias.drop/" + requestId).addConditions(StorageGateway.condition(key, current))
                .addMutations(Mutation.newBuilder().setKey(key).setDelete(true)).build());
    }
    public void dropGraph(String requestId, String name) {
        GraphDefinition graph = graph(name);
        requireWritable(graph);
        update(requestId, graph, graph.toBuilder().setState("DROPPED").build());
        // 先持久化 DROPPED 和写栅栏，再逐副本删除本地 Data Group。删除只影响该 graphId，
        // 同名重建会获得新的 graphId，不会误删新图；历史 Metadata 记录仍保留用于审计。
        for (int part = 0; part < graph.getPartitions(); part++) {
            ByteString routeKey = StorageGateway.groupKey(graph.getId(), part);
            ByteString routeBytes = storage.get(0, 0, routeKey);
            DataGroupSpec route;
            try { route = DataGroupSpec.parseFrom(routeBytes); }
            catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "图分片路由损坏", ex); }
            try {
                storage.write(graph.getId(), part,
                        StorageGateway.batch("graph.dropgate/" + requestId + "/" + part)
                                .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeGate"), TransactionEngine.text("DROPPED"))).build());
            } catch (ClusterException ex) {
                if (!ex.code().equals("DATA_LOST")) throw ex;
            }
            for (Node member : route.getMembersList()) {
                RpcResponse removed = storage.direct(address(member), "data.removeGroup", route.toByteString(), Duration.ofMinutes(2));
                if (!removed.getCode().equals("OK")) throw new ClusterException(removed.getCode(), removed.getMessage());
            }
        }
    }

    public void update(String requestId, GraphDefinition old, GraphDefinition next) {
        storage.write(0, 0, StorageGateway.batch("catalog.update/" + requestId)
                .addConditions(StorageGateway.condition(graphKey(old.getName()), old.toByteString()))
                .addMutations(TransactionEngine.put(versionKey(old.getId(), old.getSchemaVersion()), old.toByteString()))
                .addMutations(TransactionEngine.put(versionKey(next.getId(), next.getSchemaVersion()), next.toByteString()))
                .addMutations(TransactionEngine.put(graphKey(old.getName()), next.toByteString())).build());
    }
    public GraphDefinition version(String name, long version) {
        GraphDefinition current = graph(name);
        if (current.getSchemaVersion() == version) return current;
        ByteString bytes = storage.get(0, 0, versionKey(current.getId(), version));
        if (bytes == null) throw new ClusterException("SCHEMA_VERSION_MISMATCH", "所需 Schema 版本已失效");
        try { return GraphDefinition.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "Schema 历史版本损坏", ex); }
    }
    public static ByteString versionKey(long graph, long version) { return TransactionEngine.text("schema/version/" + graph + "/" + version); }
    public List<Node> nodes() {
        try {
            RpcResponse response = storage.call(0, 0, "metadata.nodes", "", ByteString.EMPTY, Duration.ofSeconds(10));
            return DataGroupSpec.parseFrom(response.getBody()).getMembersList();
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("PROTOCOL_ERROR", "节点目录无效", ex); }
    }
    public static InetSocketAddress address(Node node) { return new InetSocketAddress(node.getRpc().getHost(), node.getRpc().getPort()); }
    public List<Node> placementNodes() {
        return nodes().stream().filter(node -> storage.get(0, 0, TransactionEngine.text("nodes/draining/" + node.getNodeId())) == null
                        && storage.get(0, 0, TransactionEngine.text("health/offline/" + node.getNodeId())) == null).toList();
    }
    public static void requireWritable(GraphDefinition graph) {
        if (!graph.getState().equals("ACTIVE")) throw new ClusterException("GRAPH_STATE", "目标图 " + graph.getName() + " 当前状态为 " + graph.getState() + "，禁止写入");
    }
}
