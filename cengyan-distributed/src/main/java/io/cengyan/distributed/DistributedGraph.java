package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.time.Duration;
import java.util.*;
import io.cengyan.protocol.PartitionRouter;

/** 图实体访问层，所有数据通过 Storage RPC，不打开 Compute 本地 RocksDB。 */
public final class DistributedGraph {
    public record BulkResult(long affected, long duplicates, long skipped) { }
    private record Located(int part, ByteString key) { }
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    private final TransactionEngine transactions;
    public DistributedGraph(StorageGateway storage) {
        this.storage = storage;
        catalog = new DistributedCatalog(storage);
        transactions = new TransactionEngine(storage);
    }
    public TransactionEngine.QueryScope readScope(long graph) {
        GraphDefinition definition = catalog.graphs().stream().filter(g -> g.getId() == graph).findFirst()
                .orElseThrow(() -> new ClusterException("GRAPH_NOT_FOUND", "图不存在"));
        return transactions.new QueryScope(graph, definition.getPartitions());
    }
    public BulkResult replayWrite(String requestId, String graphName, Duration timeout) {
        GraphDefinition target = catalog.graph(graphName);
        ByteString encoded = storage.get(ticketGraph(target), ticketPart(target, requestId), TransactionEngine.text("requests/" + requestId));
        if (encoded == null) return null;
        try {
            RequestTicket ticket = RequestTicket.parseFrom(encoded);
            if (ticket.hasPlan() && ticket.getPlan().getGraphId() != target.getId()) throw new ClusterException("REQUEST_CONFLICT", "请求目标图不匹配");
            return replayTicket(target, ticket, requestId, timeout);
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "写请求票据损坏", ex); }
    }
    public Entity get(GraphDefinition graph, boolean node, Value id, TransactionEngine.QueryScope scope) {
        ByteString value = scope.value(storage.get(graph.getId(), EntityCodec.part(node, id, graph), EntityCodec.key(node, id, graph)));
        return value == null ? null : EntityCodec.entity(value);
    }
    public Map<Value, Entity> getMany(GraphDefinition graph, boolean node, List<Value> ids, TransactionEngine.QueryScope scope) {
        Map<Integer, List<Value>> grouped = new LinkedHashMap<>();
        for (Value id : ids) grouped.computeIfAbsent(EntityCodec.part(node, id, graph), ignored -> new ArrayList<>()).add(id);
        Map<Value, Entity> result = new LinkedHashMap<>();
        for (var partition : grouped.entrySet()) {
            List<ByteString> keys = partition.getValue().stream().map(id -> EntityCodec.key(node, id, graph)).toList();
            List<ByteString> values = storage.getMany(graph.getId(), partition.getKey(), keys);
            for (int i = 0; i < values.size(); i++) {
                ByteString visible = scope.value(values.get(i));
                if (visible != null) {
                    Entity entity = EntityCodec.entity(visible);
                    StorageGateway.charge(entity.getSerializedSize() * 3L + 128);
                    result.put(partition.getValue().get(i), entity);
                }
            }
        }
        return result;
    }
    public List<Entity> scan(GraphDefinition graph, boolean node, String type, TransactionEngine.QueryScope scope) {
        List<java.util.function.Supplier<List<Entity>>> scans = new ArrayList<>();
        ByteString prefix = ByteString.copyFrom(java.nio.ByteBuffer.allocate(9).put(node ? (byte) 16 : (byte) 17).putLong(graph.getGeneration()).array());
        for (int part = 0; part < graph.getPartitions(); part++) {
            int selected = part;
            scans.add(() -> {
            List<Entity> result = new ArrayList<>();
            storage.stream(graph.getId(), selected, prefix, type, List.of(), page -> {
                for (var entry : page.getEntriesList()) {
                    ByteString visible = scope.value(entry.getValue());
                    if (visible != null) {
                        Entity entity = EntityCodec.entity(visible);
                        if (type == null || type.isBlank() || entity.getType().equals(type)) { StorageGateway.charge(entity.getSerializedSize() * 3L + 128); result.add(entity); }
                    }
                }
            });
            return result;
            });
        }
        List<Entity> result = new ArrayList<>();
        for (List<Entity> partition : StorageGateway.parallel(scans)) result.addAll(partition);
        return result;
    }

    /** 兼容调用方；统计使用已提交落地的元数据计数，不再扫描实体和事务记录。 */
    public long count(GraphDefinition graph, boolean node, String type, TransactionEngine.QueryScope scope) {
        EntityCounts counts = statistics(graph);
        if (type == null || type.isBlank()) return node ? counts.getTotalNodes() : counts.getTotalEdges();
        return counts.getTypesList().stream().filter(item -> item.getNode() == node && item.getType().equals(type))
                .mapToLong(EntityTypeCount::getCount).sum();
    }

    /** 一次读取全部类型计数，分片并行度受 StorageGateway 限制；任何分片失败则整体失败。 */
    public EntityCounts statistics(GraphDefinition graph) {
        List<java.util.function.Supplier<EntityCounts>> requests = new ArrayList<>();
        for (int part = 0; part < graph.getPartitions(); part++) {
            int selected = part;
            requests.add(() -> storage.entityCounts(graph.getId(), selected, graph.getGeneration()));
        }
        Map<String, Long> nodes = new TreeMap<>(), edges = new TreeMap<>();
        long totalNodes = 0, totalEdges = 0;
        for (EntityCounts counts : StorageGateway.parallel(requests)) {
            totalNodes = Math.addExact(totalNodes, counts.getTotalNodes());
            totalEdges = Math.addExact(totalEdges, counts.getTotalEdges());
            for (EntityTypeCount count : counts.getTypesList()) {
                (count.getNode() ? nodes : edges).merge(count.getType(), count.getCount(), Math::addExact);
            }
        }
        var result = EntityCounts.newBuilder().setFormatVersion(1).setGeneration(graph.getGeneration())
                .setTotalNodes(totalNodes).setTotalEdges(totalEdges);
        nodes.forEach((type, count) -> result.addTypesBuilder().setNode(true).setType(type).setCount(count));
        edges.forEach((type, count) -> result.addTypesBuilder().setNode(false).setType(type).setCount(count));
        return result.build();
    }

    /**
     * 逐实体读取，不把整张图收集到 Compute 堆内存。控制面迁移和索引构建使用此接口，
     * 由调用方自行按有限批次提交进度。
     */
    public void forEachEntity(GraphDefinition graph, boolean node, String type,
                              TransactionEngine.QueryScope scope, java.util.function.Consumer<Entity> consumer) {
        ByteString prefix = ByteString.copyFrom(java.nio.ByteBuffer.allocate(9)
                .put(node ? (byte) 16 : (byte) 17).putLong(graph.getGeneration()).array());
        for (int part = 0; part < graph.getPartitions(); part++) {
            int selected = part;
            storage.stream(graph.getId(), selected, prefix, type, List.of(), page -> {
                for (var entry : page.getEntriesList()) {
                    ByteString visible = scope.value(entry.getValue());
                    if (visible == null) continue;
                    Entity entity = EntityCodec.entity(visible);
                    if (type == null || type.isBlank() || entity.getType().equals(type)) {
                        StorageGateway.charge(entity.getSerializedSize() * 3L + 128);
                        consumer.accept(entity);
                    }
                }
            });
        }
    }
    public List<Entity> lookup(GraphDefinition graph, boolean node, String type, Map<String, Value> equals,
                               TransactionEngine.QueryScope scope) {
        IndexDefinition selected = graph.getIndexesList().stream().filter(index -> index.getNode() == node
                        && index.getTypeName().equals(type) && equals.keySet().containsAll(index.getPropertiesList()))
                .max(Comparator.comparingInt(IndexDefinition::getPropertiesCount)).orElse(null);
        if (selected == null) return scan(graph, node, type, scope);
        Entity probe = Entity.newBuilder().setNode(node).setType(type).putAllProperties(equals).build();
        ByteString prefix = EntityCodec.uniqueKey(selected, probe, graph);
        if (prefix == null) return List.of();
        List<Entity> result = new ArrayList<>();
        if (selected.getUnique()) {
            int part = PartitionRouter.partOf(prefix.toByteArray(), graph.getPartitions());
            ByteString owner = scope.value(storage.get(graph.getId(), part, prefix));
            if (owner != null) {
                try {
                    Entity entity = get(graph, node, Value.parseFrom(owner), scope);
                    if (entity != null) result.add(entity);
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "索引实体身份损坏", ex); }
            }
        } else for (int part = 0; part < graph.getPartitions(); part++) {
            ByteString after = ByteString.EMPTY; String cursor = "";
            do {
                ScanPage page = storage.scan(graph.getId(), part, prefix, after, 256, cursor);
                for (var entry : page.getEntriesList()) {
                    ByteString owner = scope.value(entry.getValue());
                    if (owner != null) try {
                        Entity entity = get(graph, node, Value.parseFrom(owner), scope);
                        if (entity != null) result.add(entity);
                    } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "索引实体身份损坏", ex); }
                }
                if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
            } while (true);
        }
        return result;
    }
    public List<Entity> neighbors(GraphDefinition graph, Value vertex, boolean out, String edgeType, TransactionEngine.QueryScope scope) {
        List<Entity> result = new ArrayList<>(); ByteString after = ByteString.EMPTY;
        String cursor = "";
        int part = EntityCodec.part(true, vertex, graph);
        do {
            ScanPage page = storage.scan(graph.getId(), part, EntityCodec.adjacencyPrefix(out, vertex, graph), after, 256, cursor);
            List<Value> edgeIds = new ArrayList<>();
            for (var entry : page.getEntriesList()) {
                ByteString visible = scope.value(entry.getValue());
                if (visible != null) {
                    Entity hint = EntityCodec.entity(visible);
                    if (edgeType == null || edgeType.isBlank() || hint.getType().equals(edgeType)) {
                        edgeIds.add(hint.getId());
                    }
                }
            }
            result.addAll(getMany(graph, false, edgeIds, scope).values());
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (true);
        return result;
    }
    private List<Entity> deletionPage(GraphDefinition graph, Value vertex, boolean out, int limit) {
        var scope = readScope(graph.getId());
        ByteString after = ByteString.EMPTY; String cursor = "";
        List<Entity> result = new ArrayList<>();
        int part = EntityCodec.part(true, vertex, graph);
        try {
            do {
                ScanPage page = storage.scan(graph.getId(), part, EntityCodec.adjacencyPrefix(out, vertex, graph), after, limit, cursor);
                cursor = page.getCursorId();
                for (var entry : page.getEntriesList()) {
                    ByteString visible = scope.value(entry.getValue());
                    if (visible != null) {
                        Entity hint = EntityCodec.entity(visible);
                        Entity edge = get(graph, false, hint.getId(), scope);
                        if (edge != null) result.add(edge);
                    }
                }
                if (!page.getMore() || result.size() >= limit) break;
                after = page.getLastKey();
            } while (true);
        } finally {
            if (!cursor.isEmpty()) storage.closeCursor(graph.getId(), part, cursor);
        }
        return result;
    }

    public BulkResult put(String requestId, String graphName, List<Entity> input, boolean skipMissing, Duration timeout) {
        return put(requestId, graphName, input, skipMissing, timeout, false);
    }
    public BulkResult put(String requestId, String graphName, List<Entity> input, boolean skipMissing, Duration timeout, boolean ordinary) {
        return put(requestId, graphName, input, skipMissing, timeout, ordinary, List.of());
    }
    public BulkResult put(String requestId, String graphName, List<Entity> input, boolean skipMissing, Duration timeout,
                          boolean ordinary, List<Entity> expectedEntities) {
        GraphDefinition graph = catalog.graph(graphName); DistributedCatalog.requireWritable(graph);
        ByteString inputDigest = inputDigest(graph, input, skipMissing);
        ByteString ticketKey = TransactionEngine.text("requests/" + requestId);
        ByteString ticket = storage.get(ticketGraph(graph), ticketPart(graph, requestId), ticketKey);
        if (ticket != null) {
            try {
                RequestTicket savedTicket = RequestTicket.parseFrom(ticket);
                if (!savedTicket.getInputDigest().equals(inputDigest)) throw new ClusterException("REQUEST_CONFLICT", "相同 requestId 对应不同写入内容");
                BulkResult replayed = replayTicket(graph, savedTicket, requestId, timeout);
                if (replayed != null) return replayed;
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "提交票据损坏", ex); }
        }
        // 覆盖按主键点读。版本可见性只点查这些单元格里的事务 id，不扫描全部分片的 __tx/。
        var scope = transactions.new QueryScope(graph.getId());
        Map<ByteString, Entity> expected = new HashMap<>();
        for (Entity entity : expectedEntities) expected.put(EntityCodec.key(entity.getNode(), entity.getId(), graph), entity);
        LinkedHashMap<ByteString, Entity> unique = new LinkedHashMap<>(); long duplicates = 0, skipped = 0;
        for (Entity record : input) {
            // 实体代次由服务端确定，客户端不能覆盖已有代次或将旧删除任务套到新实体。
            Entity entity = EntityCodec.validate(record, graph).toBuilder().setIncarnation(requestId).build();
            ByteString key = EntityCodec.key(entity.getNode(), entity.getId(), graph);
            Entity previous = unique.put(key, entity);
            if (previous != null) { sameStructure(previous, entity); duplicates++; }
        }
        Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
        Map<Located, ByteString> cells = new HashMap<>(snapshots(graph, unique.values()));
        preloadEdges(graph, unique.values(), cells);
        for (Entity entity : unique.values()) {
            if (entity.getNode()) addEntity(graph, entity, scope, changes, expected, cells);
        }
        for (Entity entity : unique.values()) {
            if (entity.getNode()) continue;
            boolean missing = false;
            for (Value endpoint : List.of(entity.getSource(), entity.getTarget())) {
                ByteString endpointKey = EntityCodec.key(true, endpoint, graph);
                Entity local = unique.get(endpointKey);
                if (local == null) {
                    Located location = new Located(EntityCodec.part(true, endpoint, graph), endpointKey);
                    ByteString encoded = cached(graph, cells, location);
                    ByteString visible = scope.value(encoded);
                    local = visible == null ? null : EntityCodec.entity(visible);
                }
                if (local == null) { missing = true; break; }
                if (!local.getState().equals("ACTIVE")) throw new ClusterException("NODE_DELETING", "端点正在删除，禁止创建边");
            }
            if (missing) {
                if (skipMissing) { skipped++; continue; }
                throw new ClusterException("ENDPOINT_NOT_FOUND", "边的起点或终点不存在");
            }
            addEntity(graph, entity, scope, changes, expected, cells);
            Entity hint = entity.toBuilder().clearProperties().build();
            add(graph, EntityCodec.part(true, entity.getSource(), graph), EntityCodec.adjacency(true, entity.getSource(), entity.getId(), graph), hint.toByteString(), false, changes, cells);
            add(graph, EntityCodec.part(true, entity.getTarget(), graph), EntityCodec.adjacency(false, entity.getTarget(), entity.getId(), graph), hint.toByteString(), false, changes, cells);
            for (Value endpoint : List.of(entity.getSource(), entity.getTarget())) {
                Located location = new Located(EntityCodec.part(true, endpoint, graph), EntityCodec.key(true, endpoint, graph));
                if (!changes.containsKey(location)) {
                    ByteString encoded = cached(graph, cells, location);
                    ByteString visible = scope.value(encoded);
                    if (visible == null) throw new ClusterException("WRITE_CONFLICT", "端点在建边规划期间已被更新或删除");
                    Entity existing = EntityCodec.entity(visible);
                    if (!existing.getState().equals("ACTIVE")) throw new ClusterException("NODE_DELETING", "端点正在删除");
                    var reserved = ConditionalMutation.newBuilder().setKey(location.key).setExpectedPresent(true).setReadOnly(true);
                    if (encoded != null) reserved.setExpectedCell(encoded);
                    changes.put(location, reserved.build());
                }
            }
        }
        if (changes.isEmpty()) {
            var empty = RequestTicket.newBuilder().setInputDigest(inputDigest).setAffected(0).setDuplicates(duplicates).setSkipped(skipped).build();
            storage.write(ticketGraph(graph), ticketPart(graph, requestId), StorageGateway.batch("request.empty/" + requestId).addConditions(StorageGateway.condition(ticketKey, null))
                    .addMutations(TransactionEngine.put(ticketKey, empty.toByteString())).build());
            return new BulkResult(0, duplicates, skipped);
        }
        BulkResult stats = new BulkResult(unique.size() - skipped, duplicates, skipped);
        boolean compensate = ordinary && unique.values().stream().filter(entity -> !entity.getNode()).count() == 1
                && graph.getIndexesList().stream().noneMatch(IndexDefinition::getUnique);
        int coordinator = compensate ? EntityCodec.part(true, unique.values().stream().filter(entity -> !entity.getNode()).findFirst().orElseThrow().getSource(), graph) : -1;
        TransactionRecord record = commit(requestId, graph, changes, timeout, inputDigest, stats, coordinator);
        if (record.getState() != TransactionState.COMMITTED) throw new ClusterException(record.getErrorCode(), record.getErrorMessage());
        return new BulkResult(unique.size() - skipped, duplicates, skipped);
    }

    private void addEntity(GraphDefinition graph, Entity entity, TransactionEngine.QueryScope scope, Map<Located, ConditionalMutation> changes) {
        addEntity(graph, entity, scope, changes, Map.of());
    }
    private void addEntity(GraphDefinition graph, Entity entity, TransactionEngine.QueryScope scope,
                            Map<Located, ConditionalMutation> changes, Map<ByteString, Entity> expectedEntities) {
        addEntity(graph, entity, scope, changes, expectedEntities, Map.of());
    }
    private Map<Located, ByteString> snapshots(GraphDefinition graph, Collection<Entity> entities) {
        Map<Integer, List<ByteString>> grouped = new LinkedHashMap<>();
        for (Entity entity : entities) grouped.computeIfAbsent(EntityCodec.part(entity.getNode(), entity.getId(), graph),
                ignored -> new ArrayList<>()).add(EntityCodec.key(entity.getNode(), entity.getId(), graph));
        List<java.util.function.Supplier<Map<Located, ByteString>>> reads = new ArrayList<>();
        grouped.forEach((part, keys) -> reads.add(() -> {
            List<ByteString> values = storage.getMany(graph.getId(), part, keys);
            Map<Located, ByteString> result = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                ByteString value = values.get(i);
                if (value != null) StorageGateway.charge(value.size() * 3L + 128);
                result.put(new Located(part, keys.get(i)), value);
            }
            return result;
        }));
        Map<Located, ByteString> result = new HashMap<>();
        for (var partition : StorageGateway.parallel(reads)) result.putAll(partition);
        return result;
    }
    private void addEntity(GraphDefinition graph, Entity entity, TransactionEngine.QueryScope scope,
                           Map<Located, ConditionalMutation> changes, Map<ByteString, Entity> expectedEntities,
                           Map<Located, ByteString> snapshots) {
        int entityPart = EntityCodec.part(entity.getNode(), entity.getId(), graph);
        ByteString entityKey = EntityCodec.key(entity.getNode(), entity.getId(), graph);
        Located locationOfEntity = new Located(entityPart, entityKey);
        ByteString expectedEntity = snapshots.containsKey(locationOfEntity) ? snapshots.get(locationOfEntity)
                : storage.get(graph.getId(), entityPart, entityKey);
        ByteString visibleEntity = scope.value(expectedEntity);
        Entity previous = visibleEntity == null ? null : EntityCodec.entity(visibleEntity);
        Entity planned = expectedEntities.get(entityKey);
        if (planned != null && !planned.equals(previous)) {
            throw new ClusterException("WRITE_CONFLICT", "GQL 读取的实体在提交规划前已改变，拒绝覆盖并发更新");
        }
        if (previous != null) {
            sameStructure(previous, entity);
            if (previous.getState().equals("DELETING")) throw new ClusterException("NODE_DELETING", "实体正在删除，禁止覆盖");
            entity = entity.toBuilder().setIncarnation(previous.getIncarnation()).build();
        }
        for (IndexDefinition index : graph.getIndexesList()) {
            if (index.getNode() != entity.getNode() || !index.getTypeName().equals(entity.getType())) continue;
            ByteString oldKey = previous == null ? null : EntityCodec.indexEntry(index, previous, graph);
            ByteString newKey = EntityCodec.indexEntry(index, entity, graph);
            if (oldKey != null && !oldKey.equals(newKey)) {
                int oldPart = index.getUnique() ? PartitionRouter.partOf(oldKey.toByteArray(), graph.getPartitions()) : entityPart;
                add(graph, oldPart, oldKey, ByteString.EMPTY, true, changes);
            }
            if (newKey != null) {
                int part = index.getUnique() ? PartitionRouter.partOf(newKey.toByteArray(), graph.getPartitions()) : entityPart;
                Located location = new Located(part, newKey);
                ByteString expected = changes.containsKey(location)
                        ? (changes.get(location).getExpectedPresent() ? changes.get(location).getExpectedCell() : null)
                        : storage.get(graph.getId(), part, newKey);
                ByteString owner = changes.containsKey(location) ? (changes.get(location).getDelete() ? null : changes.get(location).getValue())
                        : scope.value(expected);
                ByteString id = entity.getId().toByteString();
                if (index.getUnique() && owner != null && !owner.equals(id)) throw new ClusterException("UNIQUE_VIOLATION", "唯一索引冲突：" + index.getName());
                var change = ConditionalMutation.newBuilder().setKey(newKey).setExpectedPresent(expected != null).setValue(id);
                if (expected != null) change.setExpectedCell(expected);
                changes.put(location, change.build());
            }
        }
        var replacement = ConditionalMutation.newBuilder().setKey(entityKey).setExpectedPresent(expectedEntity != null).setValue(entity.toByteString());
        if (expectedEntity != null) replacement.setExpectedCell(expectedEntity);
        changes.put(new Located(entityPart, entityKey), replacement.build());
    }

    public GraphDefinition createUniqueIndex(String requestId, String graphName, IndexDefinition index) {
        // 索引的显示名称可重复使用，但物理身份不可重用，否则 DROP 后的旧键会污染新索引。
        IndexDefinition definition = index.toBuilder().setIndexId(requestId).build();
        ByteString taskKey = TransactionEngine.text("tasks/index/" + requestId);
        ByteString encoded = storage.get(0, 0, taskKey);
        IndexBuildTask task;
        if (encoded != null) {
            try { task = IndexBuildTask.parseFrom(encoded); }
            catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "索引构建任务损坏", ex); }
            if (!task.getIndex().equals(definition) || !task.getBefore().getName().equals(catalog.graph(graphName).getName())) throw new ClusterException("REQUEST_CONFLICT", "索引请求内容改变");
            if (task.getStage().equals("FINISHED")) return catalog.graph(graphName);
            if (task.getStage().equals("FAILED")) throw new ClusterException("UNSUPPORTED", task.getError());
            return buildIndex(task);
        }
        GraphDefinition graph = catalog.graph(graphName); DistributedCatalog.requireWritable(graph);
        if (index.getName().isBlank() || index.getPropertiesCount() == 0
                || graph.getIndexesList().stream().anyMatch(existing -> existing.getName().equals(index.getName()))) {
            throw new ClusterException("INVALID_ARGUMENT", "索引定义无效或已存在");
        }
        TypeDefinition type = graph.getTypesList().stream().filter(t -> t.getNode() == index.getNode() && t.getName().equals(index.getTypeName()))
                .findFirst().orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "索引类型不存在"));
        for (String property : index.getPropertiesList()) if (type.getPropertiesList().stream().noneMatch(field -> field.getName().equals(property))) {
            throw new ClusterException("TYPE_MISMATCH", "索引属性不存在：" + property);
        }
        GraphDefinition barrier = graph.toBuilder().setState("INDEX_BUILDING").build();
        task = IndexBuildTask.newBuilder().setRequestId(requestId).setBefore(graph).setIndex(definition).setStage("BUILDING").build();
        storage.write(0, 0, StorageGateway.batch("index.task/" + requestId).addConditions(StorageGateway.condition(taskKey, null))
                .addConditions(StorageGateway.condition(DistributedCatalog.graphKey(graph.getName()), graph.toByteString()))
                .addMutations(TransactionEngine.put(taskKey, task.toByteString()))
                .addMutations(TransactionEngine.put(DistributedCatalog.graphKey(graph.getName()), barrier.toByteString())).build());
        return buildIndex(task);
    }
    private GraphDefinition buildIndex(IndexBuildTask task) {
        String requestId = task.getRequestId(); GraphDefinition graph = task.getBefore(); IndexDefinition index = task.getIndex();
        ByteString taskKey = TransactionEngine.text("tasks/index/" + requestId);
        GraphDefinition barrier = graph.toBuilder().setState("INDEX_BUILDING").build();
        for (int part = 0; part < graph.getPartitions(); part++) storage.write(graph.getId(), part,
                StorageGateway.batch("index.barrier/" + requestId + "/" + part)
                        .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeGate"), TransactionEngine.text("MIGRATING"))).build());
        try {
            settleTransactions(graph);
            GraphDefinition next = graph.toBuilder().addIndexes(index).setSchemaVersion(graph.getSchemaVersion() + 1).setWriteEpoch(graph.getWriteEpoch() + 1).build();
            List<Entity> existingEntities = new ArrayList<>(128);
            int[] offset = {0};
            TransactionEngine.QueryScope scope = readScope(graph.getId());
            java.util.function.Consumer<List<Entity>> process = batch -> {
                Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
                Map<ByteString, ByteString> owners = new HashMap<>();
                for (Entity entity : batch) {
                    ByteString indexKey = EntityCodec.indexEntry(index, entity, graph);
                    if (indexKey == null) continue;
                    ByteString owner = entity.getId().toByteString();
                    if (index.getUnique()) {
                        ByteString previous = owners.putIfAbsent(indexKey, owner);
                        if (previous != null && !previous.equals(owner)) throw indexBuildConflict(index);
                        ByteString stored = storage.get(graph.getId(), PartitionRouter.partOf(indexKey.toByteArray(), graph.getPartitions()), indexKey);
                        if (stored != null && !stored.equals(owner)) throw indexBuildConflict(index);
                    }
                    int part = index.getUnique() ? PartitionRouter.partOf(indexKey.toByteArray(), graph.getPartitions())
                            : EntityCodec.part(entity.getNode(), entity.getId(), graph);
                    add(graph, part, indexKey, owner, false, changes);
                }
                if (!changes.isEmpty()) executeIndexChunk(requestId, graph, changes, offset[0]);
                offset[0] += batch.size();
            };
            forEachEntity(graph, index.getNode(), index.getTypeName(), scope, entity -> {
                existingEntities.add(entity);
                if (existingEntities.size() == 128) {
                    process.accept(List.copyOf(existingEntities));
                    existingEntities.clear();
                }
            });
            if (!existingEntities.isEmpty()) process.accept(List.copyOf(existingEntities));
            GraphDefinition current = catalog.graph(graph.getName());
            if (!current.equals(next)) catalog.update(requestId + "/activate", barrier, next);
            unblockIndex(graph, requestId, next.getWriteEpoch());
            storage.write(0, 0, StorageGateway.batch("index.finished/" + requestId)
                    .addMutations(TransactionEngine.put(taskKey, task.toBuilder().setStage("FINISHED").build().toByteString())).build());
            return next;
        } catch (RuntimeException failure) {
            failIndexBuild(task, graph, barrier, failure);
            throw failure;
        }
    }

    private void failIndexBuild(IndexBuildTask task, GraphDefinition graph, GraphDefinition barrier, RuntimeException failure) {
        try {
            // 已提交的索引分块必须清掉，否则下一次同名重建会看到残留 owner。
            dropIndexKeys(task);
            GraphDefinition current = catalog.graph(graph.getName());
            if (current.equals(barrier)) catalog.update(task.getRequestId() + "/restore", barrier, graph);
            unblockIndex(graph, task.getRequestId(), graph.getWriteEpoch());
            String message = failure instanceof ClusterException ex ? ex.code() + ": " + ex.getMessage() : failure.getMessage();
            storage.write(0, 0, StorageGateway.batch("index.failed/" + task.getRequestId())
                    .addMutations(TransactionEngine.put(TransactionEngine.text("tasks/index/" + task.getRequestId()),
                            task.toBuilder().setStage("FAILED").setError(message == null ? "索引构建失败" : message).build().toByteString())).build());
        } catch (RuntimeException cleanupFailure) {
            System.err.println("索引构建失败后的清理未完成：" + cleanupFailure);
        }
    }

    private void unblockIndex(GraphDefinition graph, String requestId, long writeEpoch) {
        for (int part = 0; part < graph.getPartitions(); part++) {
            var command = StorageGateway.batch("index.unblock/" + requestId + "/" + part);
            if (writeEpoch == 0) command.addMutations(Mutation.newBuilder().setKey(TransactionEngine.text("__schema/writeEpoch")).setDelete(true));
            else command.addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeEpoch"), TransactionEngine.text(Long.toString(writeEpoch))));
            command.addMutations(Mutation.newBuilder().setKey(TransactionEngine.text("__schema/writeGate")).setDelete(true));
            storage.write(graph.getId(), part, command.build());
        }
    }

    private static ClusterException indexBuildConflict(IndexDefinition index) {
        return new ClusterException("UNIQUE_VIOLATION", "唯一索引构建发现重复值：" + index.getName());
    }

    private void executeIndexChunk(String requestId, GraphDefinition graph, Map<Located, ConditionalMutation> changes, int offset) {
        TreeMap<Integer, ParticipantPlan.Builder> participants = new TreeMap<>();
        for (var entry : changes.entrySet()) participants.computeIfAbsent(entry.getKey().part,
                part -> ParticipantPlan.newBuilder().setPartId(part)).addChanges(entry.getValue());
        var plan = TransactionPlan.newBuilder().setTransactionId(requestId + "_idx" + offset).setGraphId(graph.getId()).setSchemaMigration(true)
                .setGeneration(graph.getGeneration()).setDeadlineMs(System.currentTimeMillis() + Duration.ofMinutes(5).toMillis());
        participants.values().forEach(part -> plan.addParticipants(part.build()));
        var normalized = TransactionEngine.normalize(plan.build());
        ByteString ticketKey = TransactionEngine.text("index/chunk/" + normalized.getTransactionId());
        ByteString saved = storage.get(0, 0, ticketKey);
        if (saved == null) storage.write(0, 0, StorageGateway.batch("index.chunk." + normalized.getTransactionId())
                .addConditions(StorageGateway.condition(ticketKey, null)).addMutations(TransactionEngine.put(ticketKey, normalized.toByteString())).build());
        else try { normalized = TransactionPlan.parseFrom(saved); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "索引分块票据损坏", ex); }
        storage.call(graph.getId(), normalized.getCoordinatorPart(), "tx.execute", normalized.getTransactionId(), normalized.toByteString(), Duration.ofMinutes(5));
    }

    /** 删除索引定义及其所有 generation 键；物理清理先于 Catalog 激活，避免旧键继续占用空间。 */
    public GraphDefinition dropIndex(String requestId, String graphName, String indexName) {
        GraphDefinition graph = catalog.graph(graphName); DistributedCatalog.requireWritable(graph);
        ByteString taskKey = TransactionEngine.text("tasks/index/" + requestId);
        ByteString saved = storage.get(0, 0, taskKey);
        IndexBuildTask task;
        if (saved == null) {
            IndexDefinition index = graph.getIndexesList().stream().filter(item -> item.getName().equals(indexName)).findFirst()
                    .orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "索引不存在：" + indexName));
            task = IndexBuildTask.newBuilder().setRequestId(requestId).setBefore(graph).setIndex(index).setStage("DROPPING").build();
            storage.write(0, 0, StorageGateway.batch("index.drop.task." + requestId)
                    .addConditions(StorageGateway.condition(taskKey, null)).addMutations(TransactionEngine.put(taskKey, task.toByteString())).build());
        } else {
            task = parseIndexTask(saved);
            if (!task.getRequestId().equals(requestId) || !task.getBefore().getName().equals(graphName)
                    || !task.getIndex().getName().equals(indexName)) {
                throw new ClusterException("REQUEST_CONFLICT", "相同 requestId 对应不同索引删除请求");
            }
            if (task.getStage().equals("FINISHED")) return graph;
        }
        dropIndexKeys(task);
        GraphDefinition current = catalog.graph(graphName);
        List<IndexDefinition> remaining = current.getIndexesList().stream().filter(item -> !item.getName().equals(indexName)).toList();
        GraphDefinition next = current.toBuilder().clearIndexes().addAllIndexes(remaining)
                .setSchemaVersion(current.getSchemaVersion() + 1).setWriteEpoch(current.getWriteEpoch() + 1).build();
        if (!current.equals(next)) catalog.update(requestId + "/activate", current, next);
        GraphDefinition result = catalog.graph(graphName);
        try {
            storage.write(0, 0, StorageGateway.batch("index.drop.finished." + requestId)
                    .addMutations(TransactionEngine.put(taskKey, task.toBuilder().setStage("FINISHED").build().toByteString())).build());
        } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        return result;
    }

    private void dropIndexKeys(IndexBuildTask task) {
        GraphDefinition graph = task.getBefore();
        for (int part = 0; part < graph.getPartitions(); part++) {
            ByteString after = ByteString.EMPTY; String cursor = "";
            do {
                ScanPage page = storage.scan(graph.getId(), part, EntityCodec.indexPrefix(task.getIndex(), graph), after, 128, cursor);
                if (page.getEntriesCount() > 0) {
                    var command = StorageGateway.batch("index.drop.keys." + task.getRequestId() + "/" + part + "/" + page.getLastKey().hashCode());
                    for (var entry : page.getEntriesList()) command.addMutations(Mutation.newBuilder().setKey(entry.getKey()).setDelete(true));
                    storage.write(graph.getId(), part, command.build());
                }
                if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
            } while (true);
        }
    }

    private static IndexBuildTask parseIndexTask(ByteString bytes) {
        try { return IndexBuildTask.parseFrom(bytes); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "索引任务损坏", ex); }
    }
    public void recoverIndexes() {
        ByteString after = ByteString.EMPTY; String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("tasks/index/"), after, 100, cursor);
            for (var entry : page.getEntriesList()) try {
                IndexBuildTask task = IndexBuildTask.parseFrom(entry.getValue());
                if (task.getStage().equals("BUILDING")) buildIndex(task);
                else if (task.getStage().equals("DROPPING")) resumeDropIndex(task);
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "索引恢复任务损坏", ex); }
            if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
        } while (true);
    }

    private void resumeDropIndex(IndexBuildTask task) {
        dropIndexKeys(task);
        GraphDefinition current = catalog.graph(task.getBefore().getName());
        if (current.getIndexesList().stream().anyMatch(index -> index.getName().equals(task.getIndex().getName()))) {
            GraphDefinition next = current.toBuilder().clearIndexes()
                    .addAllIndexes(current.getIndexesList().stream().filter(index -> !index.getName().equals(task.getIndex().getName())).toList())
                    .setSchemaVersion(current.getSchemaVersion() + 1).setWriteEpoch(current.getWriteEpoch() + 1).build();
            catalog.update(task.getRequestId() + "/recover-activate", current, next);
        }
        storage.write(0, 0, StorageGateway.batch("index.drop.recovered." + task.getRequestId())
                .addMutations(TransactionEngine.put(TransactionEngine.text("tasks/index/" + task.getRequestId()), task.toBuilder().setStage("FINISHED").build().toByteString())).build());
    }
    private void settleTransactions(GraphDefinition graph) {
        for (int part = 0; part < graph.getPartitions(); part++) {
            ByteString after = ByteString.EMPTY; String cursor = "";
            do {
                ScanPage page = storage.scan(graph.getId(), part, TransactionEngine.TX_PREFIX, after, 128, cursor);
                for (var entry : page.getEntriesList()) try {
                    TransactionRecord record = TransactionRecord.parseFrom(entry.getValue());
                    if (!record.getFinalized()) transactions.resume(record);
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "索引构建前事务记录损坏", ex); }
                if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
            } while (true);
        }
    }
    public void deleteEdge(String requestId, GraphDefinition graph, Value edgeId, Duration timeout) {
        ByteString ticket = storage.get(ticketGraph(graph), ticketPart(graph, requestId), TransactionEngine.text("requests/" + requestId));
        if (ticket != null) {
            try {
                if (replayTicket(graph, RequestTicket.parseFrom(ticket), requestId, timeout) != null) return;
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "重试票据损坏", ex); }
        }
        var scope = readScope(graph.getId());
        Entity edge = get(graph, false, edgeId, scope);
        if (edge == null) return;
        Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
        add(graph, EntityCodec.part(false, edgeId, graph), EntityCodec.key(false, edgeId, graph), ByteString.EMPTY, true, changes);
        add(graph, EntityCodec.part(true, edge.getSource(), graph), EntityCodec.adjacency(true, edge.getSource(), edgeId, graph), ByteString.EMPTY, true, changes);
        add(graph, EntityCodec.part(true, edge.getTarget(), graph), EntityCodec.adjacency(false, edge.getTarget(), edgeId, graph), ByteString.EMPTY, true, changes);
        for (IndexDefinition index : graph.getIndexesList()) if (!index.getNode() && index.getTypeName().equals(edge.getType())) {
            ByteString key = EntityCodec.indexEntry(index, edge, graph);
            if (key != null) add(graph, index.getUnique() ? PartitionRouter.partOf(key.toByteArray(), graph.getPartitions()) : EntityCodec.part(false, edgeId, graph), key, ByteString.EMPTY, true, changes);
        }
        commit(requestId, graph, changes, timeout, inputDigest(graph, List.of(edge), false), new BulkResult(1, 0, 0));
    }
    public void deleteNode(String requestId, GraphDefinition graph, Value vertexId) {
        int part = EntityCodec.part(true, vertexId, graph);
        ByteString taskKey = TransactionEngine.text("__delete/" + requestId);
        ByteString encoded = storage.get(graph.getId(), part, taskKey);
        if (encoded == null) {
            Entity entity = get(graph, true, vertexId, readScope(graph.getId()));
            if (entity == null) return;
            DeleteTask task = DeleteTask.newBuilder().setGraph(graph).setVertexId(vertexId).setRequestId(requestId).setIncarnation(entity.getIncarnation()).build();
            // 先落任务再标记节点，崩溃后恢复器可以补齐标记，不能留下无人接管的 DELETING 节点。
            storage.write(graph.getId(), part, StorageGateway.batch("delete.task/" + requestId)
                    .addConditions(StorageGateway.condition(taskKey, null)).addMutations(TransactionEngine.put(taskKey, task.toByteString())).build());
            Entity deleting = entity.toBuilder().setState("DELETING").build();
            Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
            add(graph, part, EntityCodec.key(true, vertexId, graph), deleting.toByteString(), false, changes);
            commit(requestId + "_mark", graph, changes, Duration.ofMinutes(5), inputDigest(graph, List.of(deleting), false), new BulkResult(1, 0, 0));
            encoded = task.toByteString();
        }
        try { resumeDelete(DeleteTask.parseFrom(encoded)); }
        catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "删除任务损坏", ex); }
    }

    public void resumeDelete(DeleteTask task) {
        if (task.getCompleted()) return;
        GraphDefinition graph = task.getGraph(); Value vertex = task.getVertexId();
        int part = EntityCodec.part(true, vertex, graph);
        ByteString currentTask = storage.get(graph.getId(), part, TransactionEngine.text("__delete/" + task.getRequestId()));
        if (currentTask != null) {
            try {
                DeleteTask latest = DeleteTask.parseFrom(currentTask);
                if (latest.getCompleted()) return;
                task = latest;
            } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "删除进度记录损坏", ex); }
        }
        Entity starting = get(graph, true, vertex, readScope(graph.getId()));
        if (starting != null && !starting.getIncarnation().equals(task.getIncarnation())) {
            ByteString key = TransactionEngine.text("__delete/" + task.getRequestId());
            storage.write(graph.getId(), part, StorageGateway.batch("delete.obsolete/" + task.getRequestId())
                    .addMutations(TransactionEngine.put(key, task.toBuilder().setCompleted(true).build().toByteString())).build());
            return;
        }
        if (starting != null && !starting.getState().equals("DELETING")) {
            Entity deleting = starting.toBuilder().setState("DELETING").build();
            Map<Located, ConditionalMutation> marking = new LinkedHashMap<>();
            add(graph, part, EntityCodec.key(true, vertex, graph), deleting.toByteString(), false, marking);
            commit(task.getRequestId() + "_mark", graph, marking, Duration.ofMinutes(5), inputDigest(graph, List.of(deleting), false), new BulkResult(1, 0, 0));
        }
        long deleted = task.getEdgesDeleted();
        ByteString taskKey = TransactionEngine.text("__delete/" + task.getRequestId());
        for (;;) {
        LinkedHashMap<Value, Entity> edges = new LinkedHashMap<>();
        for (Entity edge : deletionPage(graph, vertex, true, 64)) edges.put(edge.getId(), edge);
        if (edges.size() < 64) for (Entity edge : deletionPage(graph, vertex, false, 64)) edges.put(edge.getId(), edge);
        if (edges.isEmpty()) break;
        for (Entity edge : edges.values()) {
            String suffix;
            try { suffix = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(edge.getId().toByteArray())); }
            catch (java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
            String parent = task.getRequestId().length() > 40 ? task.getRequestId().substring(0, 40) : task.getRequestId();
            deleteEdge(parent + "_" + suffix, graph, edge.getId(), Duration.ofMinutes(5));
            DeleteTask progress = task.toBuilder().setEdgesDeleted(++deleted).build();
            saveDeleteProgress(graph.getId(), part, taskKey, progress);
        }
        }
        Entity node = get(graph, true, vertex, readScope(graph.getId()));
        if (node != null) {
            Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
            add(graph, part, EntityCodec.key(true, vertex, graph), ByteString.EMPTY, true, changes);
            for (IndexDefinition index : graph.getIndexesList()) if (index.getNode() && index.getTypeName().equals(node.getType())) {
                ByteString key = EntityCodec.indexEntry(index, node, graph);
                if (key != null) add(graph, index.getUnique() ? PartitionRouter.partOf(key.toByteArray(), graph.getPartitions()) : part, key, ByteString.EMPTY, true, changes);
            }
            commit(task.getRequestId() + "_finish", graph, changes, Duration.ofMinutes(5), inputDigest(graph, List.of(node), false), new BulkResult(1, 0, 0));
        }
        saveDeleteProgress(graph.getId(), part, taskKey, task.toBuilder().setCompleted(true).setEdgesDeleted(deleted).build());
    }
    private void saveDeleteProgress(long graph, int part, ByteString key, DeleteTask desired) {
        for (int attempt = 0; attempt < 16; attempt++) {
            ByteString old = storage.get(graph, part, key);
            try {
                DeleteTask previous = DeleteTask.parseFrom(old);
                if (previous.getCompleted()) return;
                DeleteTask next = desired.toBuilder().setEdgesDeleted(Math.max(previous.getEdgesDeleted(), desired.getEdgesDeleted())).build();
                String id = "delete.cas/" + desired.getRequestId() + "/" + UUID.randomUUID();
                storage.write(graph, part, StorageGateway.batch(id).addConditions(StorageGateway.condition(key, old))
                        .addMutations(TransactionEngine.put(key, next.toByteString())).build());
                return;
            } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
            catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "删除进度损坏", ex); }
        }
        throw new ClusterException("BUSY", "删除任务进度并发更新，稍后恢复");
    }

    /** 影子代次只由 Storage 迁移控制任务调用，普通业务仍检查 ACTIVE 和写栅栏。 */
    public void migrateEntities(String requestId, GraphDefinition target, List<Entity> input) {
        ByteString ticketKey = TransactionEngine.text("schema/chunk/" + requestId);
        ByteString existingPlan = storage.get(0, 0, ticketKey);
        if (existingPlan != null) {
            try {
                TransactionPlan plan = TransactionPlan.parseFrom(existingPlan);
                storage.call(target.getId(), plan.getCoordinatorPart(), "tx.execute", requestId, existingPlan, Duration.ofMinutes(5));
                return;
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "迁移提交票据损坏", ex); }
        }
        var scope = readScope(target.getId());
        Map<Located, ConditionalMutation> changes = new LinkedHashMap<>();
        for (Entity original : input) {
            var matchingType = target.getTypesList().stream().filter(t -> t.getNode() == original.getNode() && t.getName().equals(original.getType())).findFirst();
            if (matchingType.isEmpty()) continue;
            Set<String> names = matchingType.get().getPropertiesList().stream().map(PropertyDefinition::getName).collect(java.util.stream.Collectors.toSet());
            var converted = original.toBuilder();
            for (PropertyDefinition property : matchingType.get().getPropertiesList()) {
                if (!property.getSourceProperty().isBlank() && original.containsProperties(property.getSourceProperty())) {
                    converted.putProperties(property.getName(), original.getPropertiesOrThrow(property.getSourceProperty()));
                }
            }
            for (String field : original.getPropertiesMap().keySet()) if (!names.contains(field)) converted.removeProperties(field);
            Entity entity = EntityCodec.validate(converted.build(), target);
            addEntity(target, entity, scope, changes);
            if (!entity.getNode()) {
                Entity hint = entity.toBuilder().clearProperties().build();
                add(target, EntityCodec.part(true, entity.getSource(), target), EntityCodec.adjacency(true, entity.getSource(), entity.getId(), target), hint.toByteString(), false, changes);
                add(target, EntityCodec.part(true, entity.getTarget(), target), EntityCodec.adjacency(false, entity.getTarget(), entity.getId(), target), hint.toByteString(), false, changes);
            }
        }
        if (changes.isEmpty()) return;
        TreeMap<Integer, ParticipantPlan.Builder> participants = new TreeMap<>();
        for (var change : changes.entrySet()) participants.computeIfAbsent(change.getKey().part, part -> ParticipantPlan.newBuilder().setPartId(part)).addChanges(change.getValue());
        var plan = TransactionPlan.newBuilder().setTransactionId(requestId).setGraphId(target.getId()).setSchemaMigration(true)
                .setGeneration(target.getGeneration())
                .setDeadlineMs(System.currentTimeMillis() + Duration.ofMinutes(5).toMillis());
        participants.values().forEach(part -> plan.addParticipants(part.build()));
        var normalized = TransactionEngine.normalize(plan.build());
        try {
            storage.write(0, 0, StorageGateway.batch("schema.chunk/" + requestId).addConditions(StorageGateway.condition(ticketKey, null))
                    .addMutations(TransactionEngine.put(ticketKey, normalized.toByteString())).build());
        } catch (ClusterException ex) {
            if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT").contains(ex.code())) throw ex;
            try { normalized = TransactionPlan.parseFrom(storage.get(0, 0, ticketKey)); }
            catch (Exception failure) { throw new ClusterException("CORRUPT_DATA", "迁移提交票据损坏", failure); }
        }
        storage.call(target.getId(), normalized.getCoordinatorPart(), "tx.execute", requestId, normalized.toByteString(), Duration.ofMinutes(5));
    }
    private void add(GraphDefinition graph, int part, ByteString key, ByteString value, boolean delete, Map<Located, ConditionalMutation> changes) {
        add(graph, part, key, value, delete, changes, null);
    }
    private void add(GraphDefinition graph, int part, ByteString key, ByteString value, boolean delete, Map<Located, ConditionalMutation> changes,
                     Map<Located, ByteString> cells) {
        Located location = new Located(part, key);
        ConditionalMutation existing = changes.get(location);
        ByteString expected = existing != null ? (existing.getExpectedPresent() ? existing.getExpectedCell() : null) : cached(graph, cells, location);
        var mutation = ConditionalMutation.newBuilder().setKey(key).setExpectedPresent(expected != null).setValue(value).setDelete(delete);
        if (expected != null) mutation.setExpectedCell(expected);
        changes.put(location, mutation.build());
    }
    private ByteString cached(GraphDefinition graph, Map<Located, ByteString> cells, Located location) {
        if (cells != null && cells.containsKey(location)) return cells.get(location);
        ByteString value = storage.get(graph.getId(), location.part, location.key);
        if (cells != null) cells.put(location, value);
        return value;
    }
    private void preloadEdges(GraphDefinition graph, Collection<Entity> entities, Map<Located, ByteString> cells) {
        Map<Integer, List<ByteString>> grouped = new LinkedHashMap<>();
        for (Entity entity : entities) {
            if (entity.getNode()) continue;
            remember(graph, cells, grouped, EntityCodec.part(true, entity.getSource(), graph), EntityCodec.key(true, entity.getSource(), graph));
            remember(graph, cells, grouped, EntityCodec.part(true, entity.getTarget(), graph), EntityCodec.key(true, entity.getTarget(), graph));
            remember(graph, cells, grouped, EntityCodec.part(true, entity.getSource(), graph), EntityCodec.adjacency(true, entity.getSource(), entity.getId(), graph));
            remember(graph, cells, grouped, EntityCodec.part(true, entity.getTarget(), graph), EntityCodec.adjacency(false, entity.getTarget(), entity.getId(), graph));
            remember(graph, cells, grouped, EntityCodec.part(false, entity.getId(), graph), EntityCodec.key(false, entity.getId(), graph));
        }
        if (grouped.isEmpty()) return;
        List<java.util.function.Supplier<Map<Located, ByteString>>> reads = new ArrayList<>();
        grouped.forEach((part, keys) -> reads.add(() -> {
            List<ByteString> values = storage.getMany(graph.getId(), part, keys);
            Map<Located, ByteString> result = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) result.put(new Located(part, keys.get(i)), values.get(i));
            return result;
        }));
        for (var partition : StorageGateway.parallel(reads)) cells.putAll(partition);
    }
    private static void remember(GraphDefinition graph, Map<Located, ByteString> cells, Map<Integer, List<ByteString>> grouped, int part, ByteString key) {
        if (cells.containsKey(new Located(part, key))) return;
        List<ByteString> keys = grouped.computeIfAbsent(part, ignored -> new ArrayList<>());
        if (!keys.contains(key)) keys.add(key);
    }
    /** 票据里的计划没有参与者时只是协调分片定位；真正的计划在事务记录里。已结束则直接返回统计。 */
    private BulkResult replayTicket(GraphDefinition graph, RequestTicket saved, String requestId, Duration timeout) {
        if (!saved.hasPlan()) return new BulkResult(saved.getAffected(), saved.getDuplicates(), saved.getSkipped());
        TransactionPlan stub = saved.getPlan();
        if (stub.getGraphId() != graph.getId()) throw new ClusterException("REQUEST_CONFLICT", "写请求目标图改变");
        if (stub.getParticipantsCount() > 0) {
            RpcResponse response = storage.call(graph.getId(), stub.getCoordinatorPart(), "tx.execute", requestId, stub.toByteString(), timeout);
            try {
                TransactionRecord result = TransactionRecord.parseFrom(response.getBody());
                if (result.getState() != TransactionState.COMMITTED) throw new ClusterException(result.getErrorCode(), result.getErrorMessage());
            } catch (ClusterException ex) { throw ex; }
            catch (Exception ex) { throw new ClusterException("PROTOCOL_ERROR", "事务响应损坏", ex); }
            return new BulkResult(saved.getAffected(), saved.getDuplicates(), saved.getSkipped());
        }
        ByteString encoded = storage.get(graph.getId(), stub.getCoordinatorPart(), TransactionEngine.transactionKey(requestId));
        if (encoded == null) return null;
        try {
            TransactionRecord existing = TransactionRecord.parseFrom(encoded);
            if (!existing.getFinalized() || existing.getState() != TransactionState.COMMITTED) {
                if (existing.getPlan().getParticipantsCount() == 0) {
                    ByteString savedPlan = storage.get(graph.getId(), existing.getPlan().getCoordinatorPart(), TransactionEngine.transactionPlanKey(requestId));
                    if (savedPlan == null) return null;
                    existing = TransactionRecord.parseFrom(savedPlan);
                }
                RpcResponse response = storage.call(graph.getId(), existing.getPlan().getCoordinatorPart(), "tx.execute", requestId, existing.getPlan().toByteString(), timeout);
                TransactionRecord result = TransactionRecord.parseFrom(response.getBody());
                if (result.getState() != TransactionState.COMMITTED) throw new ClusterException(result.getErrorCode(), result.getErrorMessage());
            }
            return new BulkResult(saved.getAffected(), saved.getDuplicates(), saved.getSkipped());
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "事务记录损坏", ex); }
    }
    private TransactionRecord commit(String requestId, GraphDefinition graph, Map<Located, ConditionalMutation> changes, Duration timeout,
                                     ByteString inputDigest, BulkResult statistics) {
        return commit(requestId, graph, changes, timeout, inputDigest, statistics, -1);
    }
    private TransactionRecord commit(String requestId, GraphDefinition graph, Map<Located, ConditionalMutation> changes, Duration timeout,
                                     ByteString inputDigest, BulkResult statistics, int compensationCoordinator) {
        timeout = StorageGateway.remaining(timeout);
        TreeMap<Integer, ParticipantPlan.Builder> participants = new TreeMap<>();
        for (var change : changes.entrySet()) participants.computeIfAbsent(change.getKey().part, part -> ParticipantPlan.newBuilder().setPartId(part)).addChanges(change.getValue());
        var plan = TransactionPlan.newBuilder().setTransactionId(requestId).setGraphId(graph.getId())
                .setGeneration(graph.getGeneration())
                .setWriteEpoch(graph.getWriteEpoch())
                .setCoordinatorPart(new ArrayList<>(participants.keySet()).get(Math.floorMod(requestId.hashCode(), participants.size())))
                .setDeadlineMs(System.currentTimeMillis() + timeout.toMillis());
        if (compensationCoordinator >= 0) plan.setCompensation(true).setCompensationCoordinator(compensationCoordinator);
        participants.values().forEach(part -> plan.addParticipants(part.build()));
        TransactionPlan normalized = TransactionEngine.normalize(plan.build());
        ByteString ticketKey = TransactionEngine.text("requests/" + requestId);
        // 票据只保存摘要、统计和协调分片。完整计划只在事务记录里写一份。
        TransactionPlan locator = TransactionPlan.newBuilder().setTransactionId(requestId).setGraphId(graph.getId())
                .setCoordinatorPart(normalized.getCoordinatorPart()).setDeadlineMs(normalized.getDeadlineMs())
                .setContentDigest(normalized.getContentDigest()).build();
        try {
            storage.write(ticketGraph(graph), ticketPart(graph, requestId), StorageGateway.batch("request.ticket/" + requestId)
                    .addConditions(StorageGateway.condition(ticketKey, null)).addMutations(TransactionEngine.put(ticketKey,
                            RequestTicket.newBuilder().setPlan(locator).setInputDigest(inputDigest).setAffected(statistics.affected())
                                    .setDuplicates(statistics.duplicates()).setSkipped(statistics.skipped()).build().toByteString())).build());
        } catch (ClusterException ex) {
            if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT").contains(ex.code())) throw ex;
            try {
                RequestTicket saved = RequestTicket.parseFrom(storage.get(ticketGraph(graph), ticketPart(graph, requestId), ticketKey));
                if (!saved.getInputDigest().equals(inputDigest)) throw new ClusterException("REQUEST_CONFLICT", "请求内容已改变");
                if (saved.hasPlan() && saved.getPlan().getParticipantsCount() > 0) normalized = saved.getPlan();
                else if (saved.hasPlan() && !saved.getPlan().getContentDigest().isEmpty()
                        && !saved.getPlan().getContentDigest().equals(normalized.getContentDigest())) {
                    throw new ClusterException("REQUEST_CONFLICT", "请求内容已改变");
                }
            }
            catch (ClusterException conflict) { throw conflict; }
            catch (Exception invalid) { throw new ClusterException("CORRUPT_DATA", "事务提交票据不可用", invalid); }
        }
        RpcResponse response = storage.call(graph.getId(), normalized.getCoordinatorPart(), "tx.execute", requestId, normalized.toByteString(), timeout);
        try {
            TransactionRecord result = TransactionRecord.parseFrom(response.getBody());
            if (result.getState() != TransactionState.COMMITTED) throw new ClusterException(result.getErrorCode(), result.getErrorMessage());
            return result;
        }
        catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("PROTOCOL_ERROR", "事务响应损坏", ex); }
    }
    private static ByteString inputDigest(GraphDefinition graph, List<Entity> input, boolean skipMissing) {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            digest.update(java.nio.ByteBuffer.allocate(9).putLong(graph.getId()).put((byte) (skipMissing ? 1 : 0)).array());
            for (Entity entity : input) {
                byte[] bytes = new byte[entity.getSerializedSize()];
                var output = com.google.protobuf.CodedOutputStream.newInstance(bytes);
                output.useDeterministicSerialization(); entity.writeTo(output); output.checkNoSpaceLeft();
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
            }
            return ByteString.copyFrom(digest.digest());
        } catch (Exception ex) { throw new IllegalStateException(ex); }
    }
    /** 请求票据写在数据分片上，不再进入 Metadata 日志。 */
    public static long ticketGraph(GraphDefinition graph) { return graph.getId(); }
    public static int ticketPart(GraphDefinition graph, String requestId) {
        return Math.floorMod(requestId.hashCode(), graph.getPartitions());
    }
    private static void sameStructure(Entity old, Entity next) {
        if (old.getNode() != next.getNode() || !old.getType().equals(next.getType())
                || (!old.getNode() && (!old.getSource().equals(next.getSource()) || !old.getTarget().equals(next.getTarget())))) {
            throw new ClusterException("ID_CONFLICT", "已有 ID 的类型或边端点不能通过覆盖改变");
        }
    }
}
