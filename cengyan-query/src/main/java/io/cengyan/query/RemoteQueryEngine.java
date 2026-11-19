package io.cengyan.query;

import io.cengyan.distributed.*;
import io.cengyan.parser.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Compute 端查询协调器：实体访问只经过 Storage，不使用本地 Catalog/RocksDB。 */
public final class RemoteQueryEngine {
    /** RETURN 里的聚合函数，和 DuckResult 支持的聚合保持一致。 */
    static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "collect_list",
            "stddev_samp", "stddev_pop", "percentile_cont", "percentile_disc");
    private final DistributedCatalog catalog;
    private final DistributedGraph data;
    private final DuckSettings duck;
    private final StorageGateway storage;
    private TransactionEngine.QueryScope queryScope;
    private Stmt supplied;
    private final IdentityHashMap<Entity, Object> scalarValues = new IdentityHashMap<>();
    private List<Map<String, Entity>> incoming;
    private RemoteCancellation cancellation;
    /**
     * 本次查询读过的点和邻接边。路径展开会反复经过同一批点，不缓存的话每经过一次就重新读一次存储，
     * 读到的字节都记进查询内存额度，几跳之后就会误报"查询中间数据超过内存额度"。一个引擎只执行一条语句，缓存不会读到自己的写入。
     */
    private record NodeKey(String type, Value id) { }
    private record AdjacencyKey(Value vertex, boolean out, String type) { }
    /** 存储读取的接缝。默认读分布式存储；测试换成内存图。 */
    interface Reads {
        /** type 为空字符串时按点目录查，不限类型。 */
        Entity node(GraphDefinition graph, String type, Value id, TransactionEngine.QueryScope scope);
        List<Entity> neighbors(GraphDefinition graph, Value vertex, boolean out, String edgeType, TransactionEngine.QueryScope scope);
        List<Entity> scan(GraphDefinition graph, String type, Map<String, Value> filters, TransactionEngine.QueryScope scope);
    }
    Reads reads = new Reads() {
        @Override public Entity node(GraphDefinition graph, String type, Value id, TransactionEngine.QueryScope scope) {
            return type.isEmpty() ? data.get(graph, true, id, scope) : data.get(graph, true, type, id, scope);
        }
        @Override public List<Entity> neighbors(GraphDefinition graph, Value vertex, boolean out, String edgeType, TransactionEngine.QueryScope scope) {
            return data.neighbors(graph, vertex, out, edgeType, scope);
        }
        @Override public List<Entity> scan(GraphDefinition graph, String type, Map<String, Value> filters, TransactionEngine.QueryScope scope) {
            return filters.isEmpty() ? data.scan(graph, true, type, scope) : data.lookup(graph, true, type, filters, scope);
        }
    };
    /** 正在执行的图、参数和取消信号，嵌套查询（EXISTS、VALUE、内联 CALL）沿用。 */
    private GraphDefinition currentGraph;
    private Map<String, Object> currentParams = Map.of();
    private BooleanSupplier currentCanceled = () -> false;
    private final Map<NodeKey, Optional<Entity>> nodeCache = new HashMap<>();
    private final Map<AdjacencyKey, List<Entity>> adjacencyCache = new HashMap<>();
    public RemoteQueryEngine(StorageGateway storage, Path temporaryDirectory, String memory, String disk, int threads) {
        this.storage = storage;
        catalog = new DistributedCatalog(storage); data = new DistributedGraph(storage);
        duck = new DuckSettings(memory, temporaryDirectory, disk, threads);
    }

    public QueryResult execute(String requestId, String graphName, String gql, Map<String, Object> params, BooleanSupplier canceled) {
        return execute(requestId, graphName, gql, params, canceled, 0);
    }
    public QueryResult execute(String requestId, String graphName, String gql, Map<String, Object> params, BooleanSupplier canceled, long schemaVersion) {
        try (var memory = QueryMemory.open(Math.max(1, parseBudget(duck.memoryLimit())));
             var watch = new RemoteCancellation(canceled)) {
            cancellation = watch;
            return executeBounded(requestId, graphName, gql, params, canceled, schemaVersion);
        }
    }
    private QueryResult executeBounded(String requestId, String graphName, String gql, Map<String, Object> params, BooleanSupplier canceled, long schemaVersion) {
        List<Stmt> statements;
        try { statements = supplied == null ? Gql.parseAll(gql) : List.of(supplied); supplied = null; }
        catch (ParseException ex) { throw new ClusterException("SYNTAX", ex.getMessage()); }
        if (statements.size() != 1) throw new ClusterException("SYNTAX", "网络请求一次只执行一条语句");
        Stmt statement = statements.getFirst();
        // USE g 查询：只对这一条查询换图。Compute 入口已按这个图取了 Schema 租约。
        if (statement instanceof Stmt.UseQuery use) { graphName = use.graph(); statement = use.body(); }
        if (statement instanceof Stmt.Set || statement instanceof Stmt.Insert || statement instanceof Stmt.InsertGraph || statement instanceof Stmt.Delete || statement instanceof Stmt.Mutate || statement instanceof Stmt.InsertUsing) {
            com.google.protobuf.ByteString requestKey = TransactionEngine.text("gql/request/" + requestId);
            String body = graphName + "\n" + gql + "\n" + new java.util.TreeMap<>(params);
            com.google.protobuf.ByteString expected = TransactionEngine.text(body);
            var recorded = storage.get(0, 0, requestKey);
            if (recorded != null && !recorded.equals(expected)) throw new ClusterException("REQUEST_CONFLICT", "相同 requestId 对应不同 GQL 内容或参数");
            if (recorded == null) {
                storage.write(0, 0, StorageGateway.batch("gql.request/" + requestId).addConditions(StorageGateway.condition(requestKey, null))
                        .addMutations(TransactionEngine.put(requestKey, expected)).build());
            }
            DistributedGraph.BulkResult replay = data.replayWrite(requestId, graphName, Duration.ofMinutes(5));
            if (replay != null) {
                QueryResult stored = storedResult(requestId);
                return withAffected(stored, Math.toIntExact(replay.affected()));
            }
            var planned = storage.get(0, 0, TransactionEngine.text("gql/plan/" + requestId));
            if (planned != null) {
                try {
                    GqlWritePlan plan = GqlWritePlan.parseFrom(planned);
                    QueryResult applied = applyWritePlan(requestId, graphName, plan);
                    QueryResult stored = storedResult(requestId);
                    return stored != null ? withAffected(stored, applied.rowsAffected()) : applied;
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "GQL 写入计划损坏", ex); }
            }
        }
        ExprEval eval = new ExprEval(params, false);
        if (statement instanceof Stmt.CreateGraph create) return createGraph(requestId, create);
        if (statement instanceof Stmt.Show show && show.kind() == Stmt.Show.Kind.GRAPHS) {
            return new QueryResult(List.of("name", "comment", "vertexIdType", "vertexIdLength", "edgeIdType", "edgeIdLength"), catalog.graphs().stream().map(g -> {
                IdType vertex = g.getVertexIdType() == IdType.ID_UNSPECIFIED ? IdType.INT64 : g.getVertexIdType();
                IdType edge = g.getEdgeIdType() == IdType.ID_UNSPECIFIED ? vertex : g.getEdgeIdType();
                int vertexLength = DistributedCatalog.canonicalIdLength(vertex, g.getVertexIdLength());
                int edgeLength = DistributedCatalog.canonicalIdLength(edge, g.getEdgeIdLength());
                return List.<Object>of(g.getName(), g.getComment(), vertex.name(), vertexLength, edge.name(), edgeLength);
            }).toList(), 0);
        }
        if (statement instanceof Stmt.Show show && show.kind() == Stmt.Show.Kind.ALIASES) return new QueryResult(List.of("alias", "graph"), catalog.aliases(), 0);
        if (statement instanceof Stmt.CreateAlias alias) { catalog.createAlias(requestId, alias.alias(), alias.graph()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.DropAlias alias) { catalog.dropAlias(requestId, alias.name()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.DropGraph drop) { catalog.dropGraph(requestId, drop.name()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.Use use) graphName = use.name();
        boolean needsGraph = needsGraph(statement);
        GraphDefinition resolved = null;
        if (needsGraph || (graphName != null && !graphName.isBlank())) {
            if (graphName == null || graphName.isBlank()) throw new ClusterException("GRAPH_NOT_FOUND", "请求必须携带目标图");
            resolved = schemaVersion == 0 ? catalog.graph(graphName) : catalog.version(graphName, schemaVersion);
        }
        final GraphDefinition graph = resolved;
        // 子查询（EXISTS、VALUE、内联 CALL）沿用这条语句的图、参数和取消信号，单跳快速路径也一样。
        currentGraph = graph; currentParams = params; currentCanceled = canceled;
        // USE and Schema/catalog operations must not depend on Data Group availability.
        // Data-reading children of composite queries still establish/share their normal transaction view.
        if (statement instanceof Stmt.Use) return QueryResult.changed(0);
        if (needsGraph && !(statement instanceof Stmt.Show || statement instanceof Stmt.Describe
                || statement instanceof Stmt.Explain || statement instanceof Stmt.ExplainQuery
                || statement instanceof Stmt.CreateType || statement instanceof Stmt.AlterType
                || statement instanceof Stmt.CreateIndex || statement instanceof Stmt.DropIndex
                || statement instanceof Stmt.DropType) && queryScope == null) {
            queryScope = data.readScope(graph.getId());
        }
        if (statement instanceof Stmt.Call call) return procedure(graph, call, eval, canceled);
        if (statement instanceof Stmt.Explain || statement instanceof Stmt.ExplainQuery) {
            return new QueryResult(List.of("plan"), List.of(List.of("ComputeCoordinator"), List.of("StorageLeaderScan / IndexLookup / PredicatePushdown"),
                    List.of("ComputeJoin / DuckDBAggregate / Sort / Limit")), 0);
        }
        if (statement instanceof Stmt.Composite composite) {
            QueryResult left = child(composite.left(), requestId, graphName, params, canceled, schemaVersion);
            if (composite.op().equals("OTHERWISE")) {
                if (!left.rows().isEmpty()) return left;
                QueryResult right = child(composite.right(), requestId, graphName, params, canceled, schemaVersion);
                if (!left.columns().equals(right.columns())) throw new ClusterException("SYNTAX", "OTHERWISE 两侧列定义不同");
                return right;
            }
            QueryResult right = child(composite.right(), requestId, graphName, params, canceled, schemaVersion);
            if (!left.columns().equals(right.columns())) throw new ClusterException("SYNTAX", "集合运算两侧列定义不同");
            List<List<Object>> rows = new ArrayList<>(left.rows());
            if (composite.op().equals("UNION")) {
                rows.addAll(right.rows());
                if (!composite.all()) rows = DuckResult.arrange(rows, List.of(), List.of(), true, null, null, duck, cancellation);
            } else {
                var counts = new HashMap<List<Object>, Integer>();
                for (var row : right.rows()) counts.merge(row, 1, Integer::sum);
                List<List<Object>> kept = new ArrayList<>();
                Set<List<Object>> emitted = new HashSet<>();
                for (var row : left.rows()) {
                    int matches = counts.getOrDefault(row, 0);
                    boolean accept = composite.op().equals("INTERSECT") ? matches > 0 : matches == 0;
                    if (accept && (composite.all() || emitted.add(row))) kept.add(row);
                    if (composite.all() && matches > 0) counts.put(row, matches - 1);
                }
                rows = kept;
            }
            QueryMemory.current().observeRows(rows);
            return new QueryResult(left.columns(), rows, 0);
        }
        if (statement instanceof Stmt.Next next) {
            QueryResult previous = child(next.left(), requestId, graphName, params, canceled, schemaVersion);
            if (!next.yield().isEmpty()) previous = yieldColumns(previous, next.yield());
            RemoteQueryEngine engine = new RemoteQueryEngine(storage, duck.tempDirectory(), duck.memoryLimit(), duck.maxTempDirectorySize(), duck.threads());
            engine.queryScope = queryScope; engine.supplied = next.right();
            engine.incoming = new ArrayList<>();
            for (List<Object> row : previous.rows()) {
                var bound = new LinkedHashMap<String, Entity>();
                for (int i = 0; i < previous.columns().size(); i++) bound.put(previous.columns().get(i), engine.scalar(row.get(i)));
                engine.incoming.add(bound);
            }
            return engine.execute(requestId, graphName, "", params, canceled, schemaVersion);
        }
        if (statement instanceof Stmt.AggregateFilter filter) {
            return aggregateFilter(child(filter.query(), requestId, graphName, params, canceled, schemaVersion), filter.condition(), eval);
        }
        if (statement instanceof Stmt.InsertGraph insertion) {
            Map<String, Entity> bound = new LinkedHashMap<>();
            List<Entity> entities = insertElements(insertion.elements(), Map.of(), eval, bound);
            QueryResult projected = insertion.tail() == null ? null : select(List.of(bound), insertion.tail(), eval, canceled);
            if (projected != null) storeResult(requestId, projected);
            int affected = Math.toIntExact(data.put(requestId, graphName, entities, false, Duration.ofMinutes(5), true).affected());
            return withAffected(projected, affected);
        }
        if (statement instanceof Stmt.InsertUsing insertion) {
            List<Map<String, Entity>> rows = clauses(graph, insertion.clauses(), params, canceled);
            List<Entity> entities = new ArrayList<>();
            List<Map<String, Entity>> after = new ArrayList<>();
            for (var bound : rows) {
                check(canceled); slots(eval, bound);
                Map<String, Entity> inserted = new LinkedHashMap<>(bound);
                entities.addAll(insertElements(insertion.elements(), bound, eval, inserted));
                after.add(inserted);
                QueryMemory.current().observeRows(entities.size(), 8);
            }
            QueryResult projected = insertion.tail() == null ? null : select(after, insertion.tail(), eval, canceled);
            if (projected != null) storeResult(requestId, projected);
            int affected = entities.isEmpty() ? 0 : Math.toIntExact(data.put(requestId, graphName, entities, false, Duration.ofMinutes(5)).affected());
            return withAffected(projected, affected);
        }
        if (statement instanceof Stmt.CreateType type) {
            catalog.addType(requestId, graphName, typeDefinition(type)); return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.DropType drop) {
            if (drop.node() && !data.scan(graph, true, drop.name(), data.readScope(graph.getId())).isEmpty()) {
                throw new ClusterException("TYPE_NOT_EMPTY", "删除含节点的类型前先执行级联节点删除");
            }
            var types = graph.getTypesList().stream().filter(t -> !(t.getNode() == drop.node() && t.getName().equals(drop.name()))).toList();
            if (types.size() == graph.getTypesCount() && !drop.ifExists()) throw new ClusterException("TYPE_NOT_FOUND", "类型不存在");
            GraphDefinition target = graph.toBuilder().clearTypes().addAllTypes(types).build();
            storage.call(0, 0, "schema.migrate", requestId, target.toByteString(), Duration.ofMinutes(5));
            return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.Show show) {
            if (show.kind() == Stmt.Show.Kind.NODE_TYPES || show.kind() == Stmt.Show.Kind.REL_TYPES) {
                boolean node = show.kind() == Stmt.Show.Kind.NODE_TYPES;
                List<String> columns = node ? List.of("name", "id", "schemaVersion", "idType", "idLength", "comment")
                        : List.of("name", "id", "schemaVersion", "comment");
                List<List<Object>> rows = graph.getTypesList().stream().filter(t -> t.getNode() == node).map(t -> {
                    if (node) {
                        IdType idType = t.getIdType() == IdType.ID_UNSPECIFIED ? graph.getVertexIdType() : t.getIdType();
                        int fallback = DistributedCatalog.canonicalIdLength(idType, graph.getVertexIdLength());
                        int idLength = t.getIdLength() == 0 ? fallback : t.getIdLength();
                        return List.<Object>of(t.getName(), t.getId(), graph.getSchemaVersion(), idType.name(), idLength, t.getComment());
                    }
                    return List.<Object>of(t.getName(), t.getId(), graph.getSchemaVersion(), t.getComment());
                }).toList();
                return new QueryResult(columns, rows, 0);
            }
            if (show.kind() == Stmt.Show.Kind.INDEXES) return new QueryResult(List.of("name", "target", "type", "properties", "unique", "ready"),
                    graph.getIndexesList().stream().map(i -> List.<Object>of(i.getName(), i.getNode() ? "NODE" : "REL", i.getTypeName(), String.join(",", i.getPropertiesList()), i.getUnique(), true)).toList(), 0);
            if (show.kind() == Stmt.Show.Kind.ALIASES) return new QueryResult(List.of("alias", "graph"), List.of(), 0);
        }
        if (statement instanceof Stmt.Describe describe) {
            var type = graph.getTypesList().stream().filter(t -> t.getNode() == describe.node() && t.getName().equals(describe.name())).findFirst()
                    .orElseThrow(() -> new ClusterException("TYPE_NOT_FOUND", "类型不存在"));
            return new QueryResult(List.of("property", "type", "nullable", "comment"), type.getPropertiesList().stream().map(f -> List.<Object>of(f.getName(), f.getType(), f.getNullable(), f.getComment())).toList(), 0);
        }
        if (statement instanceof Stmt.CreateIndex index) {
            data.createUniqueIndex(requestId, graphName, IndexDefinition.newBuilder().setName(index.name()).setUnique(index.unique()).setNode(index.node())
                    .setTypeName(index.typeName()).addAllProperties(index.properties()).build());
            return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.DropIndex drop) {
            if (graph.getIndexesList().stream().noneMatch(index -> index.getName().equals(drop.name()))) {
                if (!drop.ifExists()) throw new ClusterException("TYPE_NOT_FOUND", "索引不存在");
                return QueryResult.changed(0);
            }
            data.dropIndex(requestId, graphName, drop.name());
            return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.AlterType alter) {
            var types = new ArrayList<>(graph.getTypesList());
            int position = -1;
            for (int i = 0; i < types.size(); i++) if (types.get(i).getNode() == alter.node() && types.get(i).getName().equals(alter.typeName())) position = i;
            if (position < 0) throw new ClusterException("TYPE_NOT_FOUND", "类型不存在：" + alter.typeName());
            var changed = types.get(position).toBuilder();
            String action = alter.action().toUpperCase(Locale.ROOT);
            if (action.equals("ADD")) {
                if (changed.getPropertiesList().stream().anyMatch(p -> p.getName().equals(alter.property()))) throw new ClusterException("NAME_CONFLICT", "属性已存在");
                changed.addProperties(PropertyDefinition.newBuilder().setName(alter.property()).setType(alter.valueType()).setNullable(true)
                        .setComment(alter.comment() == null ? "" : alter.comment()));
                types.set(position, changed.build());
                GraphDefinition next = graph.toBuilder().clearTypes().addAllTypes(types).setSchemaVersion(graph.getSchemaVersion() + 1).build();
                catalog.update(requestId, graph, next);
            } else if (action.equals("DROP")) {
                var fields = changed.getPropertiesList().stream().filter(p -> !p.getName().equals(alter.property())).toList();
                if (fields.size() == changed.getPropertiesCount()) throw new ClusterException("TYPE_NOT_FOUND", "属性不存在");
                changed.clearProperties().addAllProperties(fields); types.set(position, changed.build());
                GraphDefinition next = graph.toBuilder().clearTypes().addAllTypes(types).clearIndexes().addAllIndexes(graph.getIndexesList().stream()
                        .filter(index -> !(index.getNode() == alter.node() && index.getTypeName().equals(alter.typeName()) && index.getPropertiesList().contains(alter.property()))).toList()).build();
                storage.call(0, 0, "schema.migrate", requestId, next.toByteString(), Duration.ofMinutes(5));
            } else if (action.equals("RENAME")) {
                if (changed.getPropertiesList().stream().anyMatch(property -> property.getName().equals(alter.newName()))) {
                    throw new ClusterException("NAME_CONFLICT", "目标属性名已存在");
                }
                boolean found = false;
                for (int i = 0; i < changed.getPropertiesCount(); i++) {
                    if (changed.getProperties(i).getName().equals(alter.property())) {
                        changed.setProperties(i, changed.getProperties(i).toBuilder().setName(alter.newName()).setSourceProperty(alter.property()));
                        found = true;
                    }
                }
                if (!found) throw new ClusterException("TYPE_NOT_FOUND", "待重命名属性不存在");
                types.set(position, changed.build());
                var next = graph.toBuilder().clearTypes().addAllTypes(types);
                for (int i = 0; i < next.getIndexesCount(); i++) {
                    var index = next.getIndexes(i);
                    if (index.getNode() == alter.node() && index.getTypeName().equals(alter.typeName())) {
                        next.setIndexes(i, index.toBuilder().clearProperties().addAllProperties(index.getPropertiesList().stream()
                                .map(property -> property.equals(alter.property()) ? alter.newName() : property).toList()));
                    }
                }
                storage.call(0, 0, "schema.migrate", requestId, next.build().toByteString(), Duration.ofMinutes(5));
            } else throw new ClusterException("UNSUPPORTED", "该 Schema 变更动作尚不支持：" + action);
            return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.Insert insert) {
            List<Entity> entities = new ArrayList<>();
            addInsertNode(insert.source(), eval, entities);
            if (insert.edge() != null) {
                addInsertNode(insert.target(), eval, entities);
                Object edgeId = property(insert.edge().properties(), "id", eval);
                if (edgeId == null) throw new ClusterException("INVALID_ARGUMENT", "边必须提供 id 属性");
                var edge = Entity.newBuilder().setNode(false).setType(insert.edge().typeName()).setId(EntityCodec.value(edgeId))
                        .setSource(EntityCodec.value(property(insert.source().properties(), "id", eval)))
                        .setTarget(EntityCodec.value(property(insert.target().properties(), "id", eval)));
                for (var prop : insert.edge().properties()) if (!prop.name().equals("id")) edge.putProperties(prop.name(), EntityCodec.value(eval.eval(prop.value())));
                entities.add(edge.build());
            }
            return QueryResult.changed(Math.toIntExact(data.put(requestId, graphName, entities, false, Duration.ofMinutes(5), true).affected()));
        }
        if (statement instanceof Stmt.Match match) {
            var scope = queryScope;
            List<Map<String, Entity>> bindings = bindings(graph, match.source(), match.edge(), match.target(), match.where(), eval, scope, canceled);
            return project(bindings, match.items(), match.orderBy(), match.distinct(), null, match.skip(), match.limit(), eval, canceled);
        }
        if (statement instanceof Stmt.Select select) {
            return select(clauses(graph, select.clauses(), params, canceled), select.tail(), eval, canceled);
        }
        if (statement instanceof Stmt.Mutate mutate) {
            List<Map<String, Entity>> rows = clauses(graph, mutate.clauses(), params, canceled);
            LinkedHashMap<String, Entity> changes = new LinkedHashMap<>();
            LinkedHashMap<String, Entity> deletions = new LinkedHashMap<>();
            LinkedHashMap<String, Entity> expected = new LinkedHashMap<>();
            for (var row : rows) {
                row.values().forEach(entity -> { if (!scalarValues.containsKey(entity)) expected.put(identity(entity), entity); });
                slots(eval, row);
                for (Stmt.Assign assignment : mutate.assignments()) {
                    Entity entity = row.get(assignment.variable());
                    if (entity == null || assignment.field().equals("id")) throw new ClusterException("INVALID_ARGUMENT", "不能修改 ID 或未绑定实体");
                    String key = identity(entity);
                    changes.put(key, changes.getOrDefault(key, entity).toBuilder().putProperties(assignment.field(), EntityCodec.value(eval.eval(assignment.value()))).build());
                }
                for (Stmt.Assign replacement : mutate.replacements()) {
                    Entity entity = row.get(replacement.variable());
                    if (entity == null) throw new ClusterException("INVALID_ARGUMENT", "未绑定实体");
                    Object value = eval.eval(replacement.value());
                    if (!(value instanceof Map<?, ?> properties)) throw new ClusterException("TYPE_MISMATCH", "完整属性替换必须使用属性 Map");
                    var changed = entity.toBuilder().clearProperties();
                    properties.forEach((key, property) -> changed.putProperties(String.valueOf(key), EntityCodec.value(property)));
                    changes.put(identity(entity), changed.build());
                }
                for (String removal : mutate.removals()) {
                    int dot = removal.indexOf('.');
                    if (dot < 1) throw new ClusterException("UNSUPPORTED", "REMOVE 仅支持实体属性");
                    Entity entity = row.get(removal.substring(0, dot));
                    if (entity == null) throw new ClusterException("INVALID_ARGUMENT", "REMOVE 未绑定实体");
                    String key = identity(entity);
                    changes.put(key, changes.getOrDefault(key, entity).toBuilder().removeProperties(removal.substring(dot + 1)).build());
                }
                for (Stmt.Assign label : mutate.labelSets()) {
                    Entity entity = row.get(label.variable());
                    if (entity == null || scalarValues.containsKey(entity)) throw new ClusterException("INVALID_ARGUMENT", "SET 标签的变量没有绑定点或边");
                    if (!entity.getType().equals(label.field())) {
                        throw new ClusterException("UNSUPPORTED", "层岩一个点或边只有一个类型（标签），不能再加标签 " + label.field());
                    }
                }
                for (Stmt.Assign label : mutate.labelRemovals()) {
                    Entity entity = row.get(label.variable());
                    if (entity == null || scalarValues.containsKey(entity)) throw new ClusterException("INVALID_ARGUMENT", "REMOVE 标签的变量没有绑定点或边");
                    if (entity.getType().equals(label.field())) {
                        throw new ClusterException("UNSUPPORTED", "层岩一个点或边只有一个类型（标签），不能去掉 " + label.field());
                    }
                }
                for (String variable : mutate.deletes()) {
                    Entity entity = row.get(variable);
                    if (entity != null && !scalarValues.containsKey(entity)) deletions.put(identity(entity), entity);
                }
            }
            if (!changes.isEmpty() && !deletions.isEmpty()) throw new ClusterException("UNSUPPORTED", "第一版同一请求不能混合属性更新与级联删除");
            QueryResult projected = null;
            if (mutate.tail() != null) {
                List<Map<String, Entity>> after = new ArrayList<>();
                for (var row : rows) {
                    var updated = new LinkedHashMap<String, Entity>();
                    row.forEach((name, entity) -> updated.put(name, scalarValues.containsKey(entity) ? entity : changes.getOrDefault(identity(entity), entity)));
                    after.add(updated);
                }
                projected = select(after, mutate.tail(), eval, canceled);
                storeResult(requestId, projected);
            }
            QueryResult done = freezeWritePlan(requestId, graphName, new ArrayList<>(changes.values()), new ArrayList<>(deletions.values()), new ArrayList<>(expected.values()));
            return withAffected(projected, done.rowsAffected());
        }
        if (statement instanceof Stmt.Set set) {
            var scope = queryScope;
            var rows = bindings(graph, set.source(), set.edge(), set.target(), set.where(), eval, scope, canceled);
            LinkedHashMap<String, Entity> updates = new LinkedHashMap<>();
            LinkedHashMap<String, Entity> expected = new LinkedHashMap<>();
            for (var row : rows) {
                slots(eval, row);
                for (var assignment : set.assignments()) {
                    Entity entity = row.get(assignment.variable());
                    if (entity == null || assignment.field().equals("id")) throw new ClusterException("INVALID_ARGUMENT", "不能修改实体 ID 或未绑定实体");
                    String key = identity(entity);
                    expected.putIfAbsent(key, entity);
                    Entity old = updates.getOrDefault(key, entity);
                    updates.put(key, old.toBuilder().putProperties(assignment.field(), EntityCodec.value(eval.eval(assignment.value()))).build());
                }
            }
            return freezeWritePlan(requestId, graphName, new ArrayList<>(updates.values()), List.of(), new ArrayList<>(expected.values()));
        }
        if (statement instanceof Stmt.Delete delete) {
            var rows = bindings(graph, delete.source(), delete.edge(), delete.target(), delete.where(), eval, queryScope, canceled);
            LinkedHashMap<String, Entity> entities = new LinkedHashMap<>();
            for (var row : rows) for (String variable : delete.variables()) {
                Entity entity = row.get(variable);
                if (entity != null) entities.put(identity(entity), entity);
            }
            return freezeWritePlan(requestId, graphName, List.of(), new ArrayList<>(entities.values()), new ArrayList<>(entities.values()));
        }
        throw new ClusterException("UNSUPPORTED", "该 GQL 算子尚未接入分布式执行器：" + statement.getClass().getSimpleName());
    }
    /**
     * CREATE GRAPH，可以带标准的图类型 {@code { ... }} 或 {@code LIKE 另一张图}。类型先在这里校验完，
     * 避免图建好了、类型建到一半失败。{@code IF NOT EXISTS} 时图已存在就什么都不做。
     */
    private QueryResult createGraph(String requestId, Stmt.CreateGraph create) {
        if (create.ifNotExists()) {
            try {
                catalog.graph(create.name());
                return QueryResult.changed(0);
            } catch (ClusterException ex) {
                if (!ex.code().equals("GRAPH_NOT_FOUND")) throw ex;
            }
        }
        List<TypeDefinition> types = new ArrayList<>();
        if (create.like() != null) {
            for (TypeDefinition type : catalog.graph(create.like()).getTypesList()) {
                types.add(type.toBuilder().setId(0).clearIdType().clearIdLength().build());
            }
        }
        for (Stmt.CreateType type : create.types()) types.add(typeDefinition(type));
        Set<String> names = new HashSet<>();
        for (TypeDefinition type : types) {
            if (!type.getName().matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) throw new ClusterException("INVALID_ARGUMENT", "类型名无效：" + type.getName());
            if (!names.add((type.getNode() ? "node/" : "rel/") + type.getName())) throw new ClusterException("NAME_CONFLICT", "类型重复：" + type.getName());
            Set<String> fields = new HashSet<>();
            for (PropertyDefinition field : type.getPropertiesList()) {
                if (!field.getName().matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}") || field.getName().equals("id") || !fields.add(field.getName())
                        || !Set.of("STRING", "INT64", "INT", "INTEGER", "DOUBLE", "BOOLEAN", "BOOL", "DATETIME").contains(field.getType().toUpperCase(Locale.ROOT))) {
                    throw new ClusterException("INVALID_ARGUMENT", "属性定义不合法：" + type.getName() + "." + field.getName());
                }
            }
        }
        IdType idType = create.idType() != null && create.idType().equalsIgnoreCase("STRING") ? IdType.STRING : IdType.INT64;
        catalog.create(requestId, create.name(), create.parts(), Math.max(1, create.replicas()), idType, idType, create.comment(), create.idLength(), create.idLength());
        String prefix = requestId.substring(0, Math.min(100, requestId.length()));
        for (int i = 0; i < types.size(); i++) catalog.addType(prefix + "-t" + i, create.name(), types.get(i));
        return QueryResult.changed(1);
    }

    private static TypeDefinition typeDefinition(Stmt.CreateType type) {
        var definition = TypeDefinition.newBuilder().setName(type.name()).setNode(type.node()).setComment(type.comment() == null ? "" : type.comment());
        if (type.node() && type.idSpecified()) {
            definition.setIdType(type.idType().equalsIgnoreCase("STRING") ? IdType.STRING : IdType.INT64).setIdLength(type.idLength());
        }
        for (var field : type.fields()) {
            // 存储只认 INT / INT64 等几种写法，标准的 INTEGER 统一成 INT64。
            String valueType = field.valueType().equalsIgnoreCase("INTEGER") ? "INT64" : field.valueType();
            definition.addProperties(PropertyDefinition.newBuilder().setName(field.name()).setType(valueType)
                    .setNullable(field.nullable()).setComment(field.comment() == null ? "" : field.comment()));
        }
        return definition.build();
    }

    /** 语句要不要读图：只有 RETURN、LET、FOR、FILTER、ORDER BY 这类纯计算时不需要。 */
    static boolean needsGraph(Stmt statement) {
        return switch (statement) {
            case Stmt.Select select -> select.clauses().stream().anyMatch(RemoteQueryEngine::needsGraph)
                    || (select.tail() != null && tailNeedsGraph(select.tail()));
            case Stmt.Composite composite -> needsGraph(composite.left()) || needsGraph(composite.right());
            case Stmt.Next next -> needsGraph(next.left()) || needsGraph(next.right());
            case Stmt.AggregateFilter filter -> needsGraph(filter.query()) || subquery(filter.condition());
            case Stmt.UseQuery ignored -> true;
            default -> true;
        };
    }

    private static boolean needsGraph(Stmt.Clause clause) {
        return switch (clause) {
            case Stmt.Clause.Filter filter -> subquery(filter.expr());
            case Stmt.Clause.Let let -> subquery(let.value());
            case Stmt.Clause.For loop -> subquery(loop.list());
            case Stmt.Clause.OrderPage page -> page.orderBy().stream().anyMatch(sort -> subquery(sort.expr()))
                    || (page.skip() != null && subquery(page.skip())) || (page.limit() != null && subquery(page.limit()));
            case Stmt.Clause.Inline inline -> needsGraph(inline.body());
            default -> true;
        };
    }

    private static boolean tailNeedsGraph(Stmt.Tail tail) {
        return tail.items().stream().anyMatch(item -> subquery(item.expr())) || tail.orderBy().stream().anyMatch(sort -> subquery(sort.expr()))
                || (tail.postFilter() != null && subquery(tail.postFilter()));
    }

    private static boolean subquery(Expr expr) {
        if (expr instanceof Expr.Exists || expr instanceof Expr.ValueQuery) return true;
        for (Expr child : Expr.children(expr)) if (subquery(child)) return true;
        return false;
    }

    /** NEXT YIELD a, b AS c：只把这些列传给下一段，可以改名。 */
    private static QueryResult yieldColumns(QueryResult previous, List<Stmt.Item> yield) {
        List<String> columns = new ArrayList<>();
        int[] picks = new int[yield.size()];
        for (int i = 0; i < yield.size(); i++) {
            Stmt.Item item = yield.get(i);
            if (!(item.expr() instanceof Expr.Variable variable)) throw new ClusterException("SYNTAX", "NEXT YIELD 只能列出上一段返回的列名");
            picks[i] = previous.columns().indexOf(variable.name());
            if (picks[i] < 0) throw new ClusterException("SYNTAX", "上一段没有返回列 " + variable.name());
            columns.add(item.alias() == null ? variable.name() : item.alias());
        }
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : previous.rows()) {
            List<Object> picked = new ArrayList<>(picks.length);
            for (int pick : picks) picked.add(row.get(pick));
            rows.add(picked);
        }
        return new QueryResult(columns, rows, previous.rowsAffected());
    }

    private static QueryResult withAffected(QueryResult projected, int affected) {
        return projected == null ? QueryResult.changed(affected) : new QueryResult(projected.columns(), projected.rows(), affected);
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper RESULT_JSON = new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * 写入之后带 RETURN 时，先把要返回的结果按 requestId 存下，再执行写入。
     * 断线重试同一个 requestId 时写入不会重做，结果从这里取回。
     */
    private void storeResult(String requestId, QueryResult result) {
        var key = TransactionEngine.text("gql/result/" + requestId);
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("columns", result.columns());
            body.put("rows", DateTimes.plain(result.rows()));
            storage.write(0, 0, StorageGateway.batch("gql.result/" + requestId)
                    .addMutations(TransactionEngine.put(key, com.google.protobuf.ByteString.copyFrom(RESULT_JSON.writeValueAsBytes(body)))).build());
        } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            throw new ClusterException("INTERNAL", "写入结果无法保存", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private QueryResult storedResult(String requestId) {
        var stored = storage.get(0, 0, TransactionEngine.text("gql/result/" + requestId));
        if (stored == null) return null;
        try {
            Map<String, Object> body = RESULT_JSON.readValue(stored.toByteArray(), Map.class);
            return new QueryResult((List<String>) body.get("columns"), (List<List<Object>>) body.get("rows"), 0);
        } catch (java.io.IOException ex) {
            throw new ClusterException("CORRUPT_DATA", "写入结果损坏", ex);
        }
    }

    private QueryResult freezeWritePlan(String requestId, String graphName, List<Entity> updates, List<Entity> deletes, List<Entity> expected) {
        var key = TransactionEngine.text("gql/plan/" + requestId);
        GqlWritePlan plan = GqlWritePlan.newBuilder().addAllUpdates(updates).addAllDeletes(deletes).addAllExpectedEntities(expected)
                .setAffected(updates.size() + deletes.size()).build();
        try {
            storage.write(0, 0, StorageGateway.batch("gql.plan/" + requestId).addConditions(StorageGateway.condition(key, null))
                    .addMutations(TransactionEngine.put(key, plan.toByteString())).build());
        } catch (ClusterException ex) {
            if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT").contains(ex.code())) throw ex;
            try { plan = GqlWritePlan.parseFrom(storage.get(0, 0, key)); }
            catch (Exception failure) { throw new ClusterException("CORRUPT_DATA", "GQL 计划不可恢复", failure); }
        }
        return applyWritePlan(requestId, graphName, plan);
    }
    private QueryResult applyWritePlan(String requestId, String graphName, GqlWritePlan plan) {
        if (!plan.getCompleted()) {
            if (plan.getUpdatesCount() > 0) data.put(requestId, graphName, plan.getUpdatesList(), false, Duration.ofMinutes(5), false, plan.getExpectedEntitiesList());
            GraphDefinition graph = catalog.graph(graphName);
            int sequence = 0;
            for (Entity entity : plan.getDeletesList()) {
                String sub = requestId.substring(0, Math.min(105, requestId.length())) + "_d" + sequence++;
                if (entity.getNode()) data.deleteNode(sub, graph, entity.getId());
                else data.deleteEdge(sub, graph, entity.getType(), entity.getId(), Duration.ofMinutes(5));
            }
            var key = TransactionEngine.text("gql/plan/" + requestId);
            try { storage.write(0, 0, StorageGateway.batch("gql.complete/" + requestId).addConditions(StorageGateway.condition(key, plan.toByteString()))
                    .addMutations(TransactionEngine.put(key, plan.toBuilder().setCompleted(true).build().toByteString())).build()); }
            catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        return QueryResult.changed(Math.toIntExact(plan.getAffected()));
    }
    /** @param inserted 不为空时把模式变量绑定到新插入（或引用的已有）点和边，供 INSERT 之后的 RETURN 使用 */
    private List<Entity> insertElements(List<Stmt.Element> elements, Map<String, Entity> bound, ExprEval eval, Map<String, Entity> inserted) {
        List<Entity> entities = new ArrayList<>();
        Map<Stmt.NodePat, Value> nodeIds = new IdentityHashMap<>();
        for (Stmt.Element element : elements) if (element instanceof Stmt.Element.Node node) {
            Stmt.NodePat pattern = node.node();
            Entity existing = bound.get(pattern.variable());
            Object provided = property(pattern.properties(), "id", eval);
            Value id = provided == null && existing != null ? existing.getId() : EntityCodec.value(provided);
            if (id.getKindCase() == Value.KindCase.NULL_VALUE) throw new ClusterException("INVALID_ARGUMENT", "节点必须提供 id 或引用已绑定节点");
            nodeIds.put(pattern, id);
            if (pattern.typeName() != null) {
                var entity = Entity.newBuilder().setNode(true).setType(pattern.typeName()).setId(id);
                for (var property : pattern.properties()) if (!property.name().equals("id")) entity.putProperties(property.name(), EntityCodec.value(eval.eval(property.value())));
                Entity built = entity.build();
                entities.add(built);
                if (pattern.variable() != null) inserted.put(pattern.variable(), built);
            } else if (pattern.variable() != null && existing != null) {
                inserted.putIfAbsent(pattern.variable(), existing);
            }
        }
        for (int i = 1; i + 1 < elements.size(); i++) if (elements.get(i) instanceof Stmt.Element.Rel relation) {
            if (!(elements.get(i-1) instanceof Stmt.Element.Node source) || !(elements.get(i+1) instanceof Stmt.Element.Node target)) throw new ClusterException("SYNTAX", "边必须连接两个节点");
            Object edgeId = property(relation.edge().properties(), "id", eval);
            if (edgeId == null) throw new ClusterException("INVALID_ARGUMENT", "边必须提供独立 id");
            var entity = Entity.newBuilder().setNode(false).setId(EntityCodec.value(edgeId)).setType(relation.edge().typeName())
                    .setSource(nodeIds.get(source.node())).setTarget(nodeIds.get(target.node()));
            for (var property : relation.edge().properties()) if (!property.name().equals("id")) entity.putProperties(property.name(), EntityCodec.value(eval.eval(property.value())));
            if (relation.edge().direction() == Stmt.EdgePat.Direction.IN) {
                Value from = entity.getSource(); entity.setSource(entity.getTarget()).setTarget(from);
            }
            Entity built = entity.build();
            entities.add(built);
            if (relation.edge().variable() != null) inserted.put(relation.edge().variable(), built);
        }
        return entities;
    }
    private List<Map<String, Entity>> clauses(GraphDefinition graph, List<Stmt.Clause> clauses, Map<String, Object> params, BooleanSupplier canceled) {
        List<Map<String, Entity>> rows = incoming == null ? new ArrayList<>(List.of(new LinkedHashMap<>())) : new ArrayList<>(incoming);
        return clauses(graph, clauses, rows, params, canceled);
    }

    /** 按顺序执行管道里的查询语句，每条语句把一组绑定行变成下一组。 */
    private List<Map<String, Entity>> clauses(GraphDefinition graph, List<Stmt.Clause> clauses, List<Map<String, Entity>> start,
                                              Map<String, Object> params, BooleanSupplier canceled) {
        currentGraph = graph;
        currentParams = params;
        currentCanceled = canceled;
        ExprEval eval = new ExprEval(params, false);
        List<Map<String, Entity>> rows = start;
        for (Stmt.Clause clause : clauses) {
            check(canceled);
            if (clause instanceof Stmt.Clause.Filter filter) {
                rows = rows.stream().filter(row -> { slots(eval, row); return eval.isTrue(filter.expr()); }).toList();
            } else if (clause instanceof Stmt.Clause.Match match) {
                rows = match(graph, match, rows, params, canceled);
            } else if (clause instanceof Stmt.Clause.OptionalBlock block) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (Map<String, Entity> input : rows) {
                    List<Map<String, Entity>> matched = new ArrayList<>(List.of(input));
                    for (Stmt.Clause inner : block.clauses()) matched = match(graph, (Stmt.Clause.Match) inner, matched, params, canceled);
                    if (matched.isEmpty()) {
                        var missing = new LinkedHashMap<>(input);
                        for (Stmt.Clause inner : block.clauses()) bindMissing((Stmt.Clause.Match) inner, missing);
                        next.add(missing);
                    } else next.addAll(matched);
                }
                rows = next;
            } else if (clause instanceof Stmt.Clause.Let let) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (var row : rows) {
                    slots(eval, row);
                    var bound = new LinkedHashMap<>(row);
                    bound.put(let.name(), scalar(eval.eval(let.value())));
                    next.add(bound);
                }
                rows = next;
            } else if (clause instanceof Stmt.Clause.For loop) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (var row : rows) {
                    slots(eval, row);
                    Object collection = eval.eval(loop.list());
                    if (collection == null) continue;
                    if (!(collection instanceof Iterable<?> values)) throw new ClusterException("TYPE_MISMATCH", "FOR 必须展开列表");
                    long position = loop.zeroBased() ? 0 : 1;
                    for (Object value : values) {
                        check(canceled);
                        var bound = new LinkedHashMap<>(row); bound.put(loop.name(), scalar(value));
                        if (loop.position() != null) bound.put(loop.position(), scalar(position));
                        position++;
                        next.add(bound);
                        QueryMemory.current().observeRows(next.size(), Math.max(1, bound.size()));
                    }
                }
                rows = next;
            } else if (clause instanceof Stmt.Clause.OrderPage page) {
                rows = orderPage(rows, page, eval, canceled);
            } else if (clause instanceof Stmt.Clause.Call call) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (var input : rows) {
                    slots(eval, input);
                    QueryResult result = procedure(graph, call.call(), eval, canceled);
                    for (List<Object> values : result.rows()) {
                        var bound = new LinkedHashMap<>(input);
                        for (int i = 0; i < result.columns().size(); i++) bound.put(result.columns().get(i), scalar(values.get(i)));
                        next.add(bound);
                    }
                    if (result.rows().isEmpty() && call.call().optional()) {
                        var bound = new LinkedHashMap<>(input);
                        for (String column : result.columns()) bound.put(column, scalar(null));
                        next.add(bound);
                    }
                }
                rows = next;
            } else if (clause instanceof Stmt.Clause.Inline inline) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (var input : rows) {
                    check(canceled);
                    Map<String, Entity> visible = new LinkedHashMap<>();
                    if (inline.scope() == null) visible.putAll(input);
                    else for (String name : inline.scope()) {
                        if (!input.containsKey(name)) throw new ClusterException("SYNTAX", "未知变量 " + name);
                        visible.put(name, input.get(name));
                    }
                    QueryResult result = nested(graph, inline.body(), visible, params, canceled);
                    for (List<Object> values : result.rows()) {
                        var bound = new LinkedHashMap<>(input);
                        for (int i = 0; i < result.columns().size(); i++) {
                            String column = result.columns().get(i);
                            if (input.containsKey(column) && !(visible.containsKey(column) && sameBinding(input.get(column), values.get(i)))) {
                                throw new ClusterException("SYNTAX", "内联过程返回的列和外层变量重名：" + column);
                            }
                            bound.put(column, scalar(values.get(i)));
                        }
                        next.add(bound);
                    }
                    if (result.rows().isEmpty() && inline.optional()) {
                        var bound = new LinkedHashMap<>(input);
                        for (String column : result.columns()) bound.putIfAbsent(column, scalar(null));
                        next.add(bound);
                    }
                }
                rows = next;
            } else throw new ClusterException("UNSUPPORTED", "该查询子句尚未接入远程执行：" + clause.getClass().getSimpleName());
            QueryMemory.current().observe(rows);
        }
        return rows;
    }

    /** 外层变量传进子查询后原样返回时，不算重名。 */
    private boolean sameBinding(Entity outer, Object value) {
        if (scalarValues.containsKey(outer)) return Objects.equals(scalarValues.get(outer), value);
        return value instanceof Map<?, ?> body && Objects.equals(body.get("id"), EntityCodec.object(outer.getId()))
                && Objects.equals(body.get("type"), outer.getType());
    }

    /** 一条 MATCH：每个输入行依次匹配逗号分隔的各个路径模式，再按 WHERE 过滤。OPTIONAL 匹配不上时保留输入行。 */
    private List<Map<String, Entity>> match(GraphDefinition graph, Stmt.Clause.Match match, List<Map<String, Entity>> rows,
                                            Map<String, Object> params, BooleanSupplier canceled) {
        ExprEval eval = new ExprEval(params, false);
        List<Map<String, Entity>> next = new ArrayList<>();
        for (Map<String, Entity> input : rows) {
            List<Map<String, Entity>> matched = List.of(input);
            for (Stmt.Pattern pattern : match.patterns()) {
                List<Map<String, Entity>> combined = new ArrayList<>();
                Expr filter = match.patterns().size() == 1 ? match.where() : null;
                for (var seed : matched) combined.addAll(alternatives(graph, pattern, seed, filter, params, canceled));
                matched = combined;
            }
            int before = next.size();
            for (var bound : matched) {
                if ("DIFFERENT_EDGES".equals(match.matchMode()) && repeatsEdge(bound)) continue;
                slots(eval, bound);
                if (match.where() == null || eval.isTrue(match.where())) next.add(bound);
            }
            if (match.optional() && next.size() == before) {
                var missing = new LinkedHashMap<>(input);
                bindMissing(match, missing);
                next.add(missing);
            }
        }
        return next;
    }

    /** 路径模式的各个分支（{@code |} 去重，{@code |+|} 保留重复），每个分支按有没有路径变量、搜索前缀选择执行方式。 */
    private List<Map<String, Entity>> alternatives(GraphDefinition graph, Stmt.Pattern pattern, Map<String, Entity> seed, Expr where,
                                                   Map<String, Object> params, BooleanSupplier canceled) {
        List<List<Stmt.Element>> branches = new ArrayList<>();
        branches.add(pattern.elements());
        branches.addAll(pattern.union());
        List<Map<String, Entity>> result = new ArrayList<>();
        Set<List<Object>> seen = new HashSet<>();
        boolean dedupe = branches.size() > 1 && !pattern.multiset();
        for (List<Stmt.Element> original : branches) {
            // 路径并集按整条路径去重：匿名的点和边也要参与比较，先给它们起内部名字，去重后再去掉。
            List<Stmt.Element> branch = dedupe ? nameAnonymous(original) : original;
            List<Stmt.Element> elements = normalize(branch);
            boolean grouped = elements.stream().anyMatch(e -> e instanceof Stmt.Element.Rel rel && groups.containsKey(rel.edge()));
            List<Map<String, Entity>> found;
            if (pattern.pathVariable() != null || pattern.search() != null || grouped) {
                found = pathPattern(graph, elements, seed, pattern, where, params, canceled);
            } else {
                found = pattern(graph, elements, seed, params, canceled, pattern.mode(), where);
            }
            for (var row : found) {
                if (dedupe) {
                    if (!seen.add(bindingKey(row))) continue;
                    row.keySet().removeIf(name -> name.startsWith(ANONYMOUS));
                }
                result.add(row);
            }
        }
        return result;
    }

    /** 内部名字的前缀，用户写不出这个字符。 */
    private static final String ANONYMOUS = "\u0001";

    private static List<Stmt.Element> nameAnonymous(List<Stmt.Element> elements) {
        List<Stmt.Element> named = new ArrayList<>(elements.size());
        for (int i = 0; i < elements.size(); i++) {
            Stmt.Element element = elements.get(i);
            String name = ANONYMOUS + i;
            named.add(switch (element) {
                case Stmt.Element.Node node when node.node().variable() == null -> new Stmt.Element.Node(new Stmt.NodePat(
                        name, node.node().typeName(), node.node().properties(), node.node().where(), node.node().label()));
                case Stmt.Element.Rel rel when rel.edge().variable() == null -> {
                    Stmt.EdgePat e = rel.edge();
                    yield new Stmt.Element.Rel(new Stmt.EdgePat(e.direction(), name, e.typeNames(), e.rank(), e.properties(),
                            e.minHop(), e.maxHop(), e.label(), e.where()));
                }
                default -> element;
            });
        }
        return named;
    }

    /** 一行绑定的身份，用来给路径并集去重：点和边用身份，标量用值。 */
    private List<Object> bindingKey(Map<String, Entity> row) {
        List<Object> key = new ArrayList<>();
        for (var entry : new TreeMap<>(row).entrySet()) {
            key.add(entry.getKey());
            Entity entity = entry.getValue();
            key.add(scalarValues.containsKey(entity) ? scalarValues.get(entity) : identity(entity));
        }
        return key;
    }

    /** DIFFERENT EDGES：同一条 MATCH 里绑定的边不能重复（变长边的列表也算）。 */
    private boolean repeatsEdge(Map<String, Entity> row) {
        Set<String> edges = new HashSet<>();
        for (Entity entity : row.values()) {
            List<String> found = new ArrayList<>();
            if (scalarValues.containsKey(entity)) {
                Object value = scalarValues.get(entity);
                if (value instanceof List<?> list) for (Object item : list) {
                    if (item instanceof Map<?, ?> body && body.containsKey("src")) found.add("false/" + body.get("type") + "/" + body.get("id"));
                }
            } else if (!entity.getNode()) found.add(identity(entity));
            for (String edge : found) if (!edges.add(edge)) return true;
        }
        return false;
    }

    /** 管道中间的 ORDER BY / OFFSET / LIMIT：按表达式排好绑定行的顺序再截取，排序交给 DuckDB。 */
    private List<Map<String, Entity>> orderPage(List<Map<String, Entity>> rows, Stmt.Clause.OrderPage page, ExprEval eval,
                                                BooleanSupplier canceled) {
        if (rows.isEmpty()) return rows;
        List<List<Object>> indexes = new ArrayList<>(), keys = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            check(canceled);
            slots(eval, rows.get(i));
            indexes.add(List.of((long) i));
            List<Object> key = new ArrayList<>();
            for (Stmt.Sort sort : page.orderBy()) key.add(eval.eval(sort.expr()));
            keys.add(key);
        }
        Long offset = page.skip() == null ? null : ((Number) eval.eval(page.skip())).longValue();
        Long limit = page.limit() == null ? null : ((Number) eval.eval(page.limit())).longValue();
        List<List<Object>> kept = DuckResult.arrange(indexes, keys, page.orderBy(), false, offset, limit, duck, cancellation);
        List<Map<String, Entity>> result = new ArrayList<>(kept.size());
        for (List<Object> index : kept) result.add(rows.get(((Number) index.getFirst()).intValue()));
        return result;
    }

    /** 过程调用：按 YIELD 取列，再按 AS 改名。 */
    private QueryResult procedure(GraphDefinition graph, Stmt.Call call, ExprEval eval, BooleanSupplier canceled) {
        QueryResult result = new RemoteProcedures(data, graph, queryScope, canceled).execute(call, eval);
        if (call.yield().isEmpty() || call.as().equals(call.yield())) return result;
        return new QueryResult(call.as(), result.rows(), result.rowsAffected());
    }

    /** 在给定的外层绑定上执行一个嵌套查询（内联 CALL、EXISTS、VALUE），返回它的结果表。 */
    private QueryResult nested(GraphDefinition graph, Stmt body, Map<String, Entity> outer, Map<String, Object> params,
                               BooleanSupplier canceled) {
        RemoteQueryEngine engine = new RemoteQueryEngine(storage, duck.tempDirectory(), duck.memoryLimit(), duck.maxTempDirectorySize(), duck.threads());
        engine.queryScope = queryScope; engine.reads = reads; engine.cancellation = cancellation;
        engine.nodeCache.putAll(nodeCache); engine.adjacencyCache.putAll(adjacencyCache);
        engine.incoming = List.of(transfer(outer, engine));
        try {
            return engine.run(graph, body, params, canceled);
        } finally {
            nodeCache.putAll(engine.nodeCache); adjacencyCache.putAll(engine.adjacencyCache);
        }
    }

    /** 把一行绑定交给另一个引擎：标量要在对方那里重新登记。 */
    private Map<String, Entity> transfer(Map<String, Entity> row, RemoteQueryEngine target) {
        Map<String, Entity> copy = new LinkedHashMap<>();
        for (var entry : row.entrySet()) {
            Entity entity = entry.getValue();
            copy.put(entry.getKey(), scalarValues.containsKey(entity) ? target.scalar(scalarValues.get(entity)) : entity);
        }
        return copy;
    }

    /**
     * 只读查询在已知图上执行，不经过请求级的写入、图解析和事务视图（这些由外层查询负责）。
     * 用于嵌套查询；{@code Select} 没有 RETURN（EXISTS 的写法）时返回匹配到的绑定数，列为空。
     */
    QueryResult run(GraphDefinition graph, Stmt statement, Map<String, Object> params, BooleanSupplier canceled) {
        currentGraph = graph; currentParams = params; currentCanceled = canceled;
        ExprEval eval = new ExprEval(params, false);
        if (statement instanceof Stmt.Select select) {
            List<Map<String, Entity>> rows = clauses(graph, select.clauses(), params, canceled);
            if (select.tail() == null) return new QueryResult(List.of(), rows.stream().map(row -> List.<Object>of()).toList(), 0);
            return select(rows, select.tail(), eval, canceled);
        }
        if (statement instanceof Stmt.Match match) {
            return run(graph, new Stmt.Select(List.of(new Stmt.Clause.Match(false, null, null, null, null,
                    List.of(new Stmt.Pattern(match.edge() == null ? List.of(new Stmt.Element.Node(match.source()))
                            : List.of(new Stmt.Element.Node(match.source()), new Stmt.Element.Rel(match.edge()), new Stmt.Element.Node(match.target())))),
                    match.where())), new Stmt.Tail(match.distinct(), false, match.items(), List.of(), match.orderBy(), match.skip(), match.limit())),
                    params, canceled);
        }
        if (statement instanceof Stmt.AggregateFilter filter) {
            QueryResult result = run(graph, filter.query(), params, canceled);
            return aggregateFilter(result, filter.condition(), eval);
        }
        if (statement instanceof Stmt.Call call) return procedure(graph, call, eval, canceled);
        if (statement instanceof Stmt.Composite || statement instanceof Stmt.Next) {
            throw new ClusterException("UNSUPPORTED", "子查询里暂不支持集合运算和 NEXT");
        }
        throw new ClusterException("UNSUPPORTED", "子查询里只能写查询，不能写入：" + statement.getClass().getSimpleName());
    }

    private QueryResult aggregateFilter(QueryResult result, Expr condition, ExprEval eval) {
        List<List<Object>> kept = new ArrayList<>();
        for (var row : result.rows()) {
            var aliases = new HashMap<String, ExprEval.Slot>();
            for (int i = 0; i < result.columns().size(); i++) aliases.put(result.columns().get(i), ExprEval.Slot.scalar(row.get(i)));
            eval.slots(aliases);
            if (eval.isTrue(condition)) kept.add(row);
        }
        return new QueryResult(result.columns(), kept, result.rowsAffected());
    }

    /** RETURN 的投影：{@code RETURN *}、GROUP BY 校验、FINISH。 */
    private QueryResult select(List<Map<String, Entity>> rows, Stmt.Tail tail, ExprEval eval, BooleanSupplier canceled) {
        if (tail.finish()) return new QueryResult(List.of(), List.of(), 0);
        List<Stmt.Item> items = tail.items();
        if (tail.star()) {
            Set<String> variables = new TreeSet<>(); rows.forEach(row -> variables.addAll(row.keySet()));
            items = variables.stream().map(name -> new Stmt.Item(new Expr.Variable(name), name, name)).toList();
        }
        // 投影中非聚合项已经作为分组键；显式 GROUP BY 不允许悄悄忽略额外表达式。先检查，避免白算一遍。
        if (!tail.groupBy().isEmpty() && !groupsMatch(items, tail.groupBy()))
            throw new ClusterException("UNSUPPORTED", "GROUP BY 里的每一项都要出现在 RETURN 中（可以写返回项、别名，或用变量 n 代替 id(n)）");
        // GROUP BY 后的 FILTER 在 SKIP/LIMIT 之前生效，否则会先截断再过滤，漏掉满足条件的行。
        return project(rows, items, tail.orderBy(), tail.distinct(), tail.postFilter(), tail.skip(), tail.limit(), eval, canceled);
    }

    /** 把一个 Group（量化路径模式）展开成「起点 -[合成边]-> 终点」，合成边记住组内的点模式和 WHERE。 */
    private List<Stmt.Element> normalize(List<Stmt.Element> elements) {
        if (elements.stream().noneMatch(e -> e instanceof Stmt.Element.Group)) return elements;
        List<Stmt.Element> result = new ArrayList<>();
        for (int i = 0; i < elements.size(); i++) {
            Stmt.Element element = elements.get(i);
            if (!(element instanceof Stmt.Element.Group group)) { result.add(element); continue; }
            if (group.elements().size() != 3 || !(group.elements().get(0) instanceof Stmt.Element.Node from)
                    || !(group.elements().get(1) instanceof Stmt.Element.Rel rel) || !(group.elements().get(2) instanceof Stmt.Element.Node to)) {
                throw new ClusterException("UNSUPPORTED", "量化路径模式里暂时只能有一条边：((a)-[e]->(b)){m,n}");
            }
            if (rel.edge().minHop() != null) throw new ClusterException("UNSUPPORTED", "量化路径模式里的边不能再写跳数");
            int max = group.max() == Integer.MAX_VALUE ? -1 : group.max();
            Stmt.EdgePat inner = rel.edge();
            Stmt.EdgePat synthetic = new Stmt.EdgePat(inner.direction(), inner.variable(), inner.typeNames(), inner.rank(),
                    inner.properties(), group.min(), max, inner.label(), inner.where());
            groups.put(synthetic, new GroupInfo(from.node(), to.node(), group.where()));
            // 和组相邻的点就是组的起点、终点；组在开头或结尾时补一个不带约束的点。
            if (result.isEmpty() || !(result.getLast() instanceof Stmt.Element.Node)) {
                result.add(new Stmt.Element.Node(new Stmt.NodePat(null, null, List.of())));
            }
            result.add(new Stmt.Element.Rel(synthetic));
            boolean nodeFollows = i + 1 < elements.size() && elements.get(i + 1) instanceof Stmt.Element.Node;
            if (!nodeFollows) result.add(new Stmt.Element.Node(new Stmt.NodePat(null, null, List.of())));
        }
        return result;
    }

    /** 量化路径模式里每一跳都要满足的点模式和 WHERE。 */
    private record GroupInfo(Stmt.NodePat from, Stmt.NodePat to, Expr where) { }
    private final IdentityHashMap<Stmt.EdgePat, GroupInfo> groups = new IdentityHashMap<>();

    private QueryResult child(Stmt statement, String requestId, String graph, Map<String, Object> params, BooleanSupplier canceled, long version) {
        RemoteQueryEngine engine = new RemoteQueryEngine(storage, duck.tempDirectory(), duck.memoryLimit(), duck.maxTempDirectorySize(), duck.threads());
        engine.queryScope = queryScope; engine.supplied = statement;
        return engine.execute(requestId, graph, "", params, canceled, version);
    }

    private List<Map<String, Entity>> pattern(GraphDefinition graph, List<Stmt.Element> elements, Map<String, Entity> input,
                                             Map<String, Object> params, BooleanSupplier canceled, String mode, Expr hint) {
        if (elements.isEmpty() || !(elements.getFirst() instanceof Stmt.Element.Node first)) throw new ClusterException("UNSUPPORTED", "远程模式必须以节点开始");
        var eval = new ExprEval(params, false);
        Stmt.NodePat source = constrain(first.node(), input);
        var scope = queryScope;
        if (elements.size() == 1) {
            List<Map<String, Entity>> result = new ArrayList<>();
            for (var found : bindings(graph, source, null, null, null, hint, eval, scope, canceled, mode)) {
                var merged = mergeBindings(input, found);
                if (merged != null) result.add(merged);
            }
            return result;
        }
        List<Map<String, Entity>> rows = List.of(input);
        for (int position = 0; position + 2 < elements.size(); position += 2) {
            if (!(elements.get(position) instanceof Stmt.Element.Node a) || !(elements.get(position + 1) instanceof Stmt.Element.Rel e)
                    || !(elements.get(position + 2) instanceof Stmt.Element.Node b)) throw new ClusterException("UNSUPPORTED", "远程分组路径模式尚未接入");
            var next = new ArrayList<Map<String, Entity>>();
            for (var row : rows) {
                for (var found : bindings(graph, constrain(a.node(), row), e.edge(), constrain(b.node(), row), null, hint, eval, scope, canceled, mode)) {
                    var merged = mergeBindings(row, found);
                    if (merged != null) next.add(merged);
                }
            }
            rows = next;
            QueryMemory.current().observe(rows);
        }
        return rows;
    }
    /**
     * 带路径变量、路径搜索前缀或量化路径模式的单段模式：枚举路径（或按层找最短路），再按前缀挑选。
     * {@code filter} 是可以提前用来剪枝的 WHERE（只有一个路径模式时才传）。
     */
    private List<Map<String, Entity>> pathPattern(GraphDefinition graph, List<Stmt.Element> elements, Map<String, Entity> input,
                                                 Stmt.Pattern pattern, Expr filter, Map<String, Object> params, BooleanSupplier canceled) {
        if (elements.size() != 3 || !(elements.get(0) instanceof Stmt.Element.Node from)
                || !(elements.get(1) instanceof Stmt.Element.Rel rel) || !(elements.get(2) instanceof Stmt.Element.Node to)) {
            throw new ClusterException("UNSUPPORTED", "路径变量、路径搜索和量化路径第一版要求单段模式：(起点)-[边]->(终点)");
        }
        ExprEval eval = new ExprEval(params, false);
        String search = pattern.search(), pathVariable = pattern.pathVariable();
        int count = pattern.searchCount() == null ? 1 : pattern.searchCount();
        GroupInfo group = groups.get(rel.edge());
        Stmt.NodePat source = constrain(from.node(), input), target = constrain(to.node(), input);
        List<Entity> starts = starts(graph, source, filter, eval, queryScope);
        List<Object> targetIds = idCandidates(target, filter, eval);
        Set<Value> ends = null;
        if (targetIds != null) {
            ends = new HashSet<>();
            for (Object id : targetIds) ends.add(EntityCodec.value(id));
        }
        int min = rel.edge().minHop() == null ? 1 : rel.edge().minHop();
        int max = rel.edge().maxHop() == null ? 1 : rel.edge().maxHop();
        // 最短路按层搜索，跳数不写上界也会停；枚举所有路径必须有界。
        if (max < 0 && search != null && search.contains("SHORTEST")) max = 32;
        if (max < 0 || max > 32 || min < 0 || min > max) throw new ClusterException("PATH_LIMIT", "路径搜索必须提供不超过 32 的跳数边界");
        boolean breadthFirst = group == null && min == 1 && ("ALL_SHORTEST".equals(search) || "ANY_SHORTEST".equals(search)
                || ("SHORTEST".equals(search) && count == 1));
        List<Map<String, Entity>> output = new ArrayList<>();
        if (count == 0 && search != null && !search.equals("ALL_SHORTEST") && !search.equals("ANY_SHORTEST")) return output;
        for (Entity start : starts) {
            var bound = new LinkedHashMap<>(input);
            if (source.variable() != null) bound.put(source.variable(), start);
            if (!matches(source, start, eval, bound)) continue;
            List<PathFound> paths;
            if (breadthFirst) {
                paths = shortestPaths(graph, rel.edge(), target, bound, eval, canceled, start, max, ends, filter, pathVariable);
            } else {
                paths = new ArrayList<>();
                List<Entity> vertices = new ArrayList<>(); vertices.add(start);
                walkPaths(graph, rel.edge(), target, bound, eval, canceled, pattern.mode() == null ? "WALK" : pattern.mode(),
                        min, max, vertices, new ArrayList<>(), paths, filter, pathVariable);
            }
            paths.sort(Comparator.comparingInt(path -> path.edges().size()));
            Map<Value, List<PathFound>> byEnd = new LinkedHashMap<>();
            for (PathFound path : paths) byEnd.computeIfAbsent(path.vertices().getLast().getId(), ignored -> new ArrayList<>()).add(path);
            for (var choices : byEnd.values()) {
                int shortest = choices.getFirst().edges().size(), accepted = 0, groupsSeen = 0, lastLength = -1;
                for (PathFound path : choices) {
                    int length = path.edges().size();
                    if (search != null) {
                        boolean stop = switch (search) {
                            case "ALL_SHORTEST" -> length != shortest;
                            case "ANY_SHORTEST" -> accepted >= 1;
                            case "SHORTEST", "ANY" -> accepted >= count;
                            case "SHORTEST_GROUPS" -> {
                                if (length != lastLength) { groupsSeen++; lastLength = length; }
                                yield groupsSeen > count;
                            }
                            default -> false;
                        };
                        if (stop) break;
                    }
                    var row = new LinkedHashMap<>(bound);
                    if (target.variable() != null) row.put(target.variable(), path.vertices().getLast());
                    if (rel.edge().variable() != null) row.put(rel.edge().variable(), scalar(path.edges().stream().map(this::entityBody).toList()));
                    if (group != null) {
                        List<Entity> vertices = path.vertices();
                        if (group.from().variable() != null) row.put(group.from().variable(),
                                scalar(vertices.subList(0, vertices.size() - 1).stream().map(this::entityBody).toList()));
                        if (group.to().variable() != null) row.put(group.to().variable(),
                                scalar(vertices.subList(1, vertices.size()).stream().map(this::entityBody).toList()));
                    }
                    if (pathVariable != null) row.put(pathVariable, scalar(pathBody(path)));
                    output.add(row); accepted++;
                }
            }
        }
        return output;
    }
    private record PathFound(List<Entity> vertices, List<Entity> edges) { }
    /** 终点满足节点模式、整条路径满足 WHERE。先查终点，不满足时不必拼路径。 */
    private boolean acceptPath(Stmt.NodePat target, Stmt.EdgePat pattern, Map<String, Entity> initial, ExprEval eval, Expr where,
                               String pathVariable, List<Entity> vertices, List<Entity> edges) {
        Entity end = vertices.getLast();
        var row = new LinkedHashMap<>(initial);
        if (target.variable() != null) row.put(target.variable(), end);
        if (!matches(target, end, eval, row)) return false;
        if (where == null) return true;
        if (pattern.variable() != null) row.put(pattern.variable(), scalar(edges.stream().map(this::entityBody).toList()));
        if (pathVariable != null) row.put(pathVariable, scalar(pathBody(new PathFound(vertices, edges))));
        slots(eval, row);
        return eval.isTrue(where);
    }

    /**
     * 最短路：按层广度优先，每个点只展开一次，记下所有最短前驱，最后从前驱回溯出每个终点的全部最短路径。
     * 不会像枚举所有路径那样随跳数指数增长。最短路径不会重复经过点，TRAIL/ACYCLIC/SIMPLE 自然满足。
     * @param ends 已知的终点 ID；全部找到后这一层结束就停
     */
    private List<PathFound> shortestPaths(GraphDefinition graph, Stmt.EdgePat pattern, Stmt.NodePat target, Map<String, Entity> initial,
                                          ExprEval eval, BooleanSupplier canceled, Entity start, int max, Set<Value> ends,
                                          Expr where, String pathVariable) {
        record Step(Entity edge, Value from) { }
        Map<Value, Integer> depth = new HashMap<>();
        Map<Value, Entity> vertices = new HashMap<>();
        Map<Value, List<Step>> previous = new HashMap<>();
        List<Value> order = new ArrayList<>();
        depth.put(start.getId(), 0);
        vertices.put(start.getId(), start);
        List<Value> frontier = List.of(start.getId());
        for (int hop = 1; hop <= max && !frontier.isEmpty(); hop++) {
            List<Value> next = new ArrayList<>();
            for (Value current : frontier) {
                check(canceled);
                for (Entity edge : adjacent(graph, current, pattern, queryScope)) {
                    if (!edgeAccept(pattern, edge, eval, initial, null, null)) continue;
                    Value other = edge.getSource().equals(current) ? edge.getTarget() : edge.getSource();
                    Integer seen = depth.get(other);
                    if (seen != null && seen < hop) continue;
                    if (seen == null) {
                        Entity vertex = node(graph, null, other, queryScope);
                        if (vertex == null) continue;
                        depth.put(other, hop);
                        vertices.put(other, vertex);
                        next.add(other);
                        order.add(other);
                    }
                    previous.computeIfAbsent(other, ignored -> new ArrayList<>()).add(new Step(edge, current));
                }
            }
            QueryMemory.current().observeRows(depth.size(), 4);
            frontier = next;
            if (ends != null && depth.keySet().containsAll(ends)) break;
        }
        List<PathFound> found = new ArrayList<>();
        for (Value end : order) {
            if (ends != null && !ends.contains(end)) continue;
            var probe = new LinkedHashMap<>(initial);
            if (target.variable() != null) probe.put(target.variable(), vertices.get(end));
            if (!matches(target, vertices.get(end), eval, probe)) continue;
            // 从终点沿最短前驱回溯，每条完整路径都是一条最短路。
            Deque<List<Step>> stack = new ArrayDeque<>();
            stack.push(List.of());
            while (!stack.isEmpty()) {
                check(canceled);
                List<Step> partial = stack.pop();
                Value at = partial.isEmpty() ? end : partial.getLast().from();
                if (at.equals(start.getId())) {
                    List<Entity> pathVertices = new ArrayList<>(), pathEdges = new ArrayList<>();
                    pathVertices.add(start);
                    for (int i = partial.size() - 1; i >= 0; i--) {
                        pathEdges.add(partial.get(i).edge());
                        pathVertices.add(vertices.get(i == 0 ? end : partial.get(i - 1).from()));
                    }
                    if (acceptPath(target, pattern, initial, eval, where, pathVariable, pathVertices, pathEdges))
                        found.add(new PathFound(pathVertices, pathEdges));
                    if (found.size() >= 100000) throw new ClusterException("PATH_LIMIT_EXCEEDED", "路径数量超过安全上限");
                    continue;
                }
                for (Step step : previous.getOrDefault(at, List.of())) {
                    List<Step> longer = new ArrayList<>(partial);
                    longer.add(step);
                    stack.push(longer);
                }
            }
        }
        return found;
    }

    /**
     * 起点或终点的 ID 候选：节点模式里的 {id: ...}，以及 WHERE 里 AND 连接的 id(v) = c、v.id = c、id(v) IN [...]、v.id IN $list。
     * 有多个来源时取交集。拿不到时返回 null，表示只能扫描。
     */
    static List<Object> idCandidates(Stmt.NodePat node, Expr where, ExprEval eval) {
        List<Object> result = null;
        Object own = property(node.properties(), "id", eval);
        if (own != null) result = new ArrayList<>(List.of(own));
        List<Object> fromWhere = node.variable() == null ? null : idsFromWhere(where, node.variable(), eval);
        if (fromWhere == null) return result;
        if (result == null) return fromWhere;
        List<Object> both = new ArrayList<>();
        for (Object value : result) if (fromWhere.stream().anyMatch(other -> eval.sameValue(value, other))) both.add(value);
        return both;
    }

    private static List<Object> idsFromWhere(Expr where, String variable, ExprEval eval) {
        if (where instanceof Expr.Binary binary && binary.op().equals("AND")) {
            List<Object> left = idsFromWhere(binary.left(), variable, eval), right = idsFromWhere(binary.right(), variable, eval);
            if (left == null) return right;
            if (right == null) return left;
            List<Object> both = new ArrayList<>();
            for (Object value : left) if (right.stream().anyMatch(other -> eval.sameValue(value, other))) both.add(value);
            return both;
        }
        if (where instanceof Expr.Binary binary && binary.op().equals("=")) {
            Expr constant = isId(binary.left(), variable) ? binary.right() : isId(binary.right(), variable) ? binary.left() : null;
            if (constant == null || !constant(constant)) return null;
            Object value = eval.eval(constant);
            return value == null ? List.of() : List.of(value);
        }
        if (where instanceof Expr.In in && !in.negated() && isId(in.value(), variable)) {
            List<Object> values = new ArrayList<>();
            for (Expr option : in.options()) {
                if (!constant(option)) return null;
                Object value = eval.eval(option);
                if (value != null) values.add(value);
            }
            return values;
        }
        if (where instanceof Expr.Member member && !member.negated() && isId(member.value(), variable) && constant(member.collection())) {
            Object values = eval.eval(member.collection());
            if (values instanceof List<?> list) return list.stream().filter(Objects::nonNull).map(Object.class::cast).toList();
        }
        return null;
    }

    private static boolean isId(Expr expr, String variable) {
        if (expr instanceof Expr.Prop prop) return variable.equals(prop.variable()) && prop.field().equals("id");
        return expr instanceof Expr.Call call && call.name().equalsIgnoreCase("id") && call.args().size() == 1
                && call.args().getFirst() instanceof Expr.Variable ref && ref.name().equals(variable);
    }

    private static boolean constant(Expr expr) {
        if (expr instanceof Expr.ListLit list) return list.items().stream().allMatch(RemoteQueryEngine::constant);
        return expr instanceof Expr.Literal || expr instanceof Expr.Param;
    }

    /** 按 ID 候选逐个点读；没有候选时才扫描。 */
    private List<Entity> starts(GraphDefinition graph, Stmt.NodePat source, Expr where, ExprEval eval, TransactionEngine.QueryScope scope) {
        List<Object> ids = idCandidates(source, where, eval);
        if (ids == null) {
            Map<String, Value> filters = new HashMap<>();
            for (var prop : source.properties()) filters.put(prop.name(), EntityCodec.value(eval.eval(prop.value())));
            return reads.scan(graph, source.typeName(), filters, scope);
        }
        List<Entity> found = new ArrayList<>();
        Set<Value> seen = new HashSet<>();
        for (Object id : ids) {
            Value value = EntityCodec.value(id);
            if (!seen.add(value)) continue;
            Entity entity = node(graph, source.typeName(), value, scope);
            if (entity != null) found.add(entity);
        }
        return found;
    }
    private Map<String, Object> pathBody(PathFound path) {
        return Map.of("nodes", path.vertices().stream().map(this::entityBody).toList(), "edges", path.edges().stream().map(this::entityBody).toList(), "length", (long) path.edges().size());
    }
    private Map<String, Object> entityBody(Entity entity) {
        Map<String, Object> body = new LinkedHashMap<>(), props = new LinkedHashMap<>();
        body.put("id", EntityCodec.object(entity.getId())); body.put("type", entity.getType());
        entity.getPropertiesMap().forEach((key, value) -> props.put(key, EntityCodec.object(value))); body.put("properties", props);
        if (!entity.getNode()) { body.put("src", EntityCodec.object(entity.getSource())); body.put("dst", EntityCodec.object(entity.getTarget())); }
        return body;
    }
    private void walkPaths(GraphDefinition graph, Stmt.EdgePat pattern, Stmt.NodePat target, Map<String, Entity> initial, ExprEval eval,
                           BooleanSupplier canceled, String mode, int min, int max, List<Entity> vertices, List<Entity> edges,
                           List<PathFound> found, Expr where, String pathVariable) {
        check(canceled);
        if (found.size() >= 100000) throw new ClusterException("PATH_LIMIT_EXCEEDED", "路径数量超过安全上限");
        Entity current = vertices.getLast();
        if (edges.size() >= min) {
            if (acceptPath(target, pattern, initial, eval, where, pathVariable, vertices, edges))
                found.add(new PathFound(List.copyOf(vertices), List.copyOf(edges)));
        }
        if (edges.size() == max) return;
        if (mode.equalsIgnoreCase("SIMPLE") && vertices.size() > 1 && current.getId().equals(vertices.getFirst().getId())) return;
        for (Entity edge : adjacent(graph, current.getId(), pattern, queryScope)) {
            if (mode.equalsIgnoreCase("TRAIL") && edges.stream().anyMatch(existing -> identity(existing).equals(identity(edge)))) continue;
            Value id = edge.getSource().equals(current.getId()) ? edge.getTarget() : edge.getSource();
            boolean repeated = vertices.stream().anyMatch(vertex -> vertex.getId().equals(id));
            if (repeated && mode.equalsIgnoreCase("ACYCLIC")) continue;
            if (repeated && mode.equalsIgnoreCase("SIMPLE") && !id.equals(vertices.getFirst().getId())) continue;
            Entity next = node(graph, null, id, queryScope);
            if (next == null) continue;
            if (!edgeAccept(pattern, edge, eval, initial, current, next)) continue;
            edges.add(edge); vertices.add(next);
            walkPaths(graph, pattern, target, initial, eval, canceled, mode, min, max, vertices, edges, found, where, pathVariable);
            vertices.removeLast(); edges.removeLast();
        }
    }
    private static Stmt.NodePat constrain(Stmt.NodePat pattern, Map<String, Entity> bindings) {
        Entity bound = bindings.get(pattern.variable());
        if (bound == null) return pattern;
        var properties = new ArrayList<>(pattern.properties());
        properties.add(new Stmt.Prop("id", new Expr.Literal(EntityCodec.object(bound.getId()))));
        return new Stmt.NodePat(pattern.variable(), pattern.typeName(), properties, pattern.where(), pattern.label());
    }
    private static Map<String, Entity> mergeBindings(Map<String, Entity> left, Map<String, Entity> right) {
        var merged = new LinkedHashMap<>(left);
        for (var item : right.entrySet()) {
            Entity previous = merged.putIfAbsent(item.getKey(), item.getValue());
            if (previous != null && (previous.getNode() != item.getValue().getNode() || !previous.getId().equals(item.getValue().getId()))) return null;
        }
        return merged;
    }

    private List<Map<String, Entity>> bindings(GraphDefinition graph, Stmt.NodePat source, Stmt.EdgePat edge, Stmt.NodePat target,
                                                Expr where, ExprEval eval, TransactionEngine.QueryScope scope, BooleanSupplier canceled) {
        return bindings(graph, source, edge, target, where, eval, scope, canceled, "WALK");
    }
    private List<Map<String, Entity>> bindings(GraphDefinition graph, Stmt.NodePat source, Stmt.EdgePat edge, Stmt.NodePat target,
                                                Expr where, ExprEval eval, TransactionEngine.QueryScope scope, BooleanSupplier canceled, String mode) {
        return bindings(graph, source, edge, target, where, where, eval, scope, canceled, mode);
    }
    /** @param hint 只用来取起点 ID 候选的条件，例如 MATCH 子句的 WHERE；不在这里过滤 */
    private List<Map<String, Entity>> bindings(GraphDefinition graph, Stmt.NodePat source, Stmt.EdgePat edge, Stmt.NodePat target,
                                                Expr where, Expr hint, ExprEval eval, TransactionEngine.QueryScope scope, BooleanSupplier canceled, String mode) {
        List<Map<String, Entity>> result = new ArrayList<>();
        List<Entity> starts = starts(graph, source, hint, eval, scope);
        for (Entity node : starts) {
            check(canceled);
            Map<String, Entity> row = new LinkedHashMap<>();
            if (source.variable() != null) row.put(source.variable(), node);
            if (!matches(source, node, eval, row)) continue;
            if (edge == null) {
                slots(eval, row);
                if (where == null || eval.isTrue(where)) result.add(row);
                continue;
            }
            int min = edge.minHop() == null ? 1 : edge.minHop(), max = edge.maxHop() == null ? 1 : edge.maxHop();
            if (min != 1 || max != 1) {
                if (max < 0 || max > 32 || min < 0 || min > max) throw new ClusterException("PATH_LIMIT", "路径跳数必须有界且不超过 32");
                Set<Value> visited = new HashSet<>(); visited.add(node.getId());
                expand(graph, node, node, row, edge, target, where, eval, scope, canceled, 0, min, max, new HashSet<>(), visited, mode == null ? "WALK" : mode, result);
                continue;
            }
            for (Entity relation : adjacent(graph, node.getId(), edge, scope)) {
                Value otherId = relation.getSource().equals(node.getId()) ? relation.getTarget() : relation.getSource();
                Entity other = node(graph, target.typeName(), otherId, scope);
                if (other == null) continue;
                if (!edgeAccept(edge, relation, eval, row, node, other)) continue;
                var joined = new LinkedHashMap<>(row);
                if (edge.variable() != null) joined.put(edge.variable(), relation);
                if (target.variable() != null) joined.put(target.variable(), other);
                if (!matches(target, other, eval, joined)) continue;
                slots(eval, joined);
                if (where == null || eval.isTrue(where)) result.add(joined);
            }
        }
        QueryMemory.current().observe(result);
        return result;
    }

    private void expand(GraphDefinition graph, Entity start, Entity current, Map<String, Entity> initial, Stmt.EdgePat edge,
                         Stmt.NodePat target, Expr where, ExprEval eval, TransactionEngine.QueryScope scope,
                         BooleanSupplier canceled, int hop, int min, int max, Set<String> usedEdges, Set<Value> visited, String mode, List<Map<String, Entity>> result) {
        check(canceled);
        if (result.size() > 100000) throw new ClusterException("PATH_LIMIT", "路径展开结果超过安全上限");
        if (hop >= min) {
            var row = new LinkedHashMap<>(initial);
            if (target.variable() != null) row.put(target.variable(), current);
            if (matches(target, current, eval, row)) {
                slots(eval, row);
                if (where == null || eval.isTrue(where)) result.add(row);
            }
        }
        if (hop == max) return;
        for (Entity relation : adjacent(graph, current.getId(), edge, scope)) {
            if (mode.equalsIgnoreCase("TRAIL") && usedEdges.contains(identity(relation))) continue;
            Value next = relation.getSource().equals(current.getId()) ? relation.getTarget() : relation.getSource();
            boolean repeated = visited.contains(next);
            boolean closeStart = mode.equalsIgnoreCase("SIMPLE") && next.equals(start.getId());
            if (repeated && (mode.equalsIgnoreCase("ACYCLIC") || mode.equalsIgnoreCase("SIMPLE")) && !closeStart) continue;
            Entity other = node(graph, null, next, scope);
            if (other == null) continue;
            var bound = new LinkedHashMap<>(initial);
            if (edge.variable() != null) bound.put(edge.variable(), relation);
            if (edgeAccept(edge, relation, eval, initial, current, other)) {
                boolean newEdge = usedEdges.add(identity(relation)), newVertex = visited.add(next);
                expand(graph, start, other, bound, edge, target, where, eval, scope, canceled, hop + 1, min, closeStart ? hop + 1 : max, usedEdges, visited, mode, result);
                if (newEdge) usedEdges.remove(identity(relation));
                if (newVertex) visited.remove(next);
            }
        }
    }

    static boolean isAggregate(Expr expr) {
        return expr instanceof Expr.Call call && AGGREGATES.contains(call.name().toLowerCase(Locale.ROOT));
    }

    /**
     * 非聚合返回项都是分组键。GROUP BY 里的每一项都要对应一个返回项（返回项本身、它的别名，或者用变量 n 代替 id(n)），
     * 否则会按没返回的值分组、结果看不出分组依据。返回了但没写进 GROUP BY 的值照样参与分组，和以前的单机引擎一致。
     */
    static boolean groupsMatch(List<Stmt.Item> items, List<Expr> groupBy) {
        for (Expr group : groupBy) {
            boolean found = false;
            for (var item : items) if (!isAggregate(item.expr()) && sameGroup(item, group)) { found = true; break; }
            if (!found) return false;
        }
        return true;
    }

    private static boolean sameGroup(Stmt.Item item, Expr group) {
        if (item.expr().equals(group)) return true;
        if (!(group instanceof Expr.Variable variable)) return false;
        if (variable.name().equals(item.alias())) return true;
        return item.expr() instanceof Expr.Call call && call.name().equalsIgnoreCase("id")
                && call.args().size() == 1 && call.args().getFirst().equals(variable);
    }

    private QueryResult project(List<Map<String, Entity>> bindings, List<Stmt.Item> items, List<Stmt.Sort> sorts, boolean distinct,
                                Expr postFilter, Expr skip, Expr limit, ExprEval eval, BooleanSupplier canceled) {
        boolean aggregate = items.stream().anyMatch(item -> isAggregate(item.expr()));
        List<List<Object>> rows = new ArrayList<>(), keys = new ArrayList<>();
        String[] kinds = new String[items.size()];
        for (var row : bindings) {
            check(canceled); slots(eval, row);
            List<Object> projected = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                Expr expression = items.get(i).expr();
                if (aggregate && isAggregate(expression) && expression instanceof Expr.Call call) {
                    boolean star = call.args().isEmpty() || call.args().getFirst() instanceof Expr.Variable v && v.name().equals("*");
                    kinds[i] = star ? "star" : call.name().toLowerCase(Locale.ROOT);
                    projected.add(star ? 1L : eval.eval(call.args().getFirst()));
                } else { kinds[i] = "group"; projected.add(eval.eval(expression)); }
            }
            rows.add(projected);
            QueryMemory.current().observeRows(rows.size(), items.size());
            List<Object> sort = new ArrayList<>();
            if (!aggregate) {
                Map<String, ExprEval.Slot> aliases = new HashMap<>();
                for (int i = 0; i < items.size(); i++) if (items.get(i).alias() != null) {
                    aliases.put(items.get(i).alias(), ExprEval.Slot.scalar(projected.get(i)));
                }
                eval.aliases(aliases);
                for (var order : sorts) sort.add(eval.eval(order.expr()));
            }
            keys.add(sort);
        }
        if (aggregate) {
            for (int i = 0; i < kinds.length; i++) if (kinds[i] == null) {
                Expr expression = items.get(i).expr();
                kinds[i] = isAggregate(expression) && expression instanceof Expr.Call c ? (c.args().isEmpty() ? "star" : c.name().toLowerCase(Locale.ROOT)) : "group";
            }
            rows = DuckResult.aggregate(rows, kinds, duck, cancellation);
            keys = new ArrayList<>();
            for (List<Object> aggregated : rows) {
                List<Object> sort = new ArrayList<>();
                Map<String, ExprEval.Slot> aliases = new HashMap<>();
                for (int i = 0; i < items.size(); i++) if (items.get(i).alias() != null) aliases.put(items.get(i).alias(), ExprEval.Slot.scalar(aggregated.get(i)));
                eval.slots(aliases);
                for (Stmt.Sort order : sorts) {
                    int column = -1;
                    for (int i = 0; i < items.size(); i++) if (items.get(i).expr().equals(order.expr())) { column = i; break; }
                    sort.add(column >= 0 ? aggregated.get(column) : eval.eval(order.expr()));
                }
                keys.add(sort);
            }
        }
        List<String> columns = items.stream().map(item -> item.alias() == null ? item.text() : item.alias()).toList();
        if (postFilter != null) {
            List<List<Object>> keptRows = new ArrayList<>(), keptKeys = new ArrayList<>();
            for (int row = 0; row < rows.size(); row++) {
                check(canceled);
                Map<String, ExprEval.Slot> values = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) values.put(columns.get(i), ExprEval.Slot.scalar(rows.get(row).get(i)));
                eval.slots(values);
                if (eval.isTrue(postFilter)) { keptRows.add(rows.get(row)); keptKeys.add(keys.get(row)); }
            }
            rows = keptRows; keys = keptKeys;
        }
        Long offset = skip == null ? null : ((Number) eval.eval(skip)).longValue();
        Long max = limit == null ? null : ((Number) eval.eval(limit)).longValue();
        rows = DuckResult.arrange(rows, keys, sorts, distinct, offset, max, duck, cancellation);
        return new QueryResult(columns, rows, 0);
    }

    private static void addInsertNode(Stmt.NodePat pattern, ExprEval eval, List<Entity> entities) {
        if (pattern == null || pattern.typeName() == null) return;
        Object id = property(pattern.properties(), "id", eval);
        if (id == null) throw new ClusterException("INVALID_ARGUMENT", "节点必须提供 id");
        var entity = Entity.newBuilder().setNode(true).setType(pattern.typeName()).setId(EntityCodec.value(id));
        for (var prop : pattern.properties()) if (!prop.name().equals("id")) entity.putProperties(prop.name(), EntityCodec.value(eval.eval(prop.value())));
        entities.add(entity.build());
    }
    /** 点的 ID 全图唯一；边的 ID 只在边类型内唯一，边的身份要带上类型。 */
    private static String identity(Entity entity) {
        return entity.getNode() ? "true/" + entity.getId() : "false/" + entity.getType() + "/" + entity.getId();
    }
    /** 模式里写了标签就直接读这个类型的记录，一次点读；没写标签先查点目录。 */
    private Entity node(GraphDefinition graph, String typeName, Value id, TransactionEngine.QueryScope scope) {
        String type = typeName == null || typeName.isBlank() ? "" : typeName;
        NodeKey key = new NodeKey(type, id);
        Optional<Entity> cached = nodeCache.get(key);
        if (cached == null) {
            cached = Optional.ofNullable(reads.node(graph, type, id, scope));
            nodeCache.put(key, cached);
            cached.ifPresent(found -> nodeCache.putIfAbsent(new NodeKey(type.isEmpty() ? found.getType() : "", id), Optional.of(found)));
        }
        return cached.orElse(null);
    }
    /** 一个点在模式方向上的邻接边，同一条边只出现一次。 */
    private List<Entity> adjacent(GraphDefinition graph, Value vertex, Stmt.EdgePat.Direction direction, String edgeType,
                                  TransactionEngine.QueryScope scope) {
        List<Entity> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (boolean out : direction == Stmt.EdgePat.Direction.OUT ? List.of(true)
                : direction == Stmt.EdgePat.Direction.IN ? List.of(false) : List.of(true, false)) {
            AdjacencyKey key = new AdjacencyKey(vertex, out, edgeType == null ? "" : edgeType);
            List<Entity> edges = adjacencyCache.get(key);
            if (edges == null) {
                edges = List.copyOf(reads.neighbors(graph, vertex, out, edgeType, scope));
                adjacencyCache.put(key, edges);
            }
            for (Entity edge : edges) if (seen.add(identity(edge))) result.add(edge);
        }
        return result;
    }
    /** 按边模式取邻接边：标签表达式能列出类型时只读这些类型，否则读全部类型再按标签过滤。 */
    private List<Entity> adjacent(GraphDefinition graph, Value vertex, Stmt.EdgePat pattern, TransactionEngine.QueryScope scope) {
        List<String> types = pattern.typeNames();
        List<Entity> edges;
        if (types.size() <= 1) {
            edges = adjacent(graph, vertex, pattern.direction(), types.isEmpty() ? null : types.getFirst(), scope);
        } else {
            edges = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            for (String type : types) for (Entity edge : adjacent(graph, vertex, pattern.direction(), type, scope)) {
                if (seen.add(identity(edge))) edges.add(edge);
            }
        }
        if (pattern.label() == null) return edges;
        List<Entity> kept = new ArrayList<>(edges.size());
        for (Entity edge : edges) if (label(pattern.label(), edge.getType())) kept.add(edge);
        return kept;
    }

    /**
     * 一条边是否满足边模式：属性、边内 WHERE；量化路径模式的合成边还要满足组内两端的点模式和组的 WHERE。
     * {@code from}、{@code to} 是这一跳在模式里的左端和右端，不是边的起点和终点。
     */
    private boolean edgeAccept(Stmt.EdgePat pattern, Entity edge, ExprEval eval, Map<String, Entity> row, Entity from, Entity to) {
        if (pattern.label() != null && !label(pattern.label(), edge.getType())) return false;
        var bound = new LinkedHashMap<>(row);
        if (pattern.variable() != null) bound.put(pattern.variable(), edge);
        GroupInfo group = groups.get(pattern);
        if (group != null) {
            if (group.from().variable() != null) bound.put(group.from().variable(), from);
            if (group.to().variable() != null) bound.put(group.to().variable(), to);
            if (!matches(group.from(), from, eval, bound) || !matches(group.to(), to, eval, bound)) return false;
        }
        slots(eval, bound);
        for (var property : pattern.properties()) {
            Object actual = property.name().equals("id") ? EntityCodec.object(edge.getId())
                    : EntityCodec.object(edge.getPropertiesOrDefault(property.name(), Value.getDefaultInstance()));
            if (!eval.sameValue(actual, eval.eval(property.value()))) return false;
        }
        if (pattern.where() != null && !eval.isTrue(pattern.where())) return false;
        return group == null || group.where() == null || eval.isTrue(group.where());
    }

    private static Object property(List<Stmt.Prop> properties, String field, ExprEval eval) {
        for (var prop : properties) if (prop.name().equals(field)) return eval.eval(prop.value());
        return null;
    }
    private boolean matches(Stmt.NodePat pattern, Entity entity, ExprEval eval, Map<String, Entity> row) {
        if (pattern.typeName() != null && !pattern.typeName().equals(entity.getType())) return false;
        if (pattern.label() != null && !label(pattern.label(), entity.getType())) return false;
        slots(eval, row);
        for (var prop : pattern.properties()) {
            Object actual = prop.name().equals("id") ? EntityCodec.object(entity.getId()) : EntityCodec.object(entity.getPropertiesOrDefault(prop.name(), Value.getDefaultInstance()));
            if (!eval.sameValue(actual, eval.eval(prop.value()))) return false;
        }
        return pattern.where() == null || eval.isTrue(pattern.where());
    }
    private static boolean label(Stmt.Label label, String type) {
        return switch (label) {
            case Stmt.Label.One one -> one.name().equals(type);
            case Stmt.Label.Any ignored -> true;
            case Stmt.Label.Not not -> !label(not.inner(), type);
            case Stmt.Label.And and -> and.options().stream().allMatch(option -> label(option, type));
            case Stmt.Label.Or or -> or.options().stream().anyMatch(option -> label(option, type));
        };
    }
    private void bindMissing(Stmt.Clause.Match match, Map<String, Entity> bindings) {
        for (Stmt.Pattern pattern : match.patterns()) {
            bindMissing(pattern.elements(), bindings);
            for (List<Stmt.Element> branch : pattern.union()) bindMissing(branch, bindings);
            if (pattern.pathVariable() != null && !pattern.pathVariable().isBlank()) {
                bindings.computeIfAbsent(pattern.pathVariable(), ignored -> scalar(null));
            }
        }
    }
    private void bindMissing(List<Stmt.Element> elements, Map<String, Entity> bindings) {
        for (Stmt.Element element : elements) {
            String name = switch (element) {
                case Stmt.Element.Node node -> node.node().variable();
                case Stmt.Element.Rel rel -> rel.edge().variable();
                case Stmt.Element.Group group -> { bindMissing(group.elements(), bindings); yield null; }
            };
            if (name != null && !name.isBlank()) bindings.computeIfAbsent(name, ignored -> scalar(null));
        }
    }
    private void slots(ExprEval eval, Map<String, Entity> entities) {
        Map<String, ExprEval.Slot> slots = new HashMap<>();
        for (var entry : entities.entrySet()) {
            Entity entity = entry.getValue();
            if (scalarValues.containsKey(entity)) {
                slots.put(entry.getKey(), ExprEval.Slot.scalar(scalarValues.get(entity)));
                continue;
            }
            Map<String, Object> body = new LinkedHashMap<>(), props = new LinkedHashMap<>();
            entity.getPropertiesMap().forEach((key, value) -> props.put(key, EntityCodec.object(value)));
            body.put("id", EntityCodec.object(entity.getId())); body.put("type", entity.getType()); body.put("properties", props);
            if (!entity.getNode()) { body.put("src", EntityCodec.object(entity.getSource())); body.put("dst", EntityCodec.object(entity.getTarget())); }
            slots.put(entry.getKey(), ExprEval.Slot.scalar(body));
        }
        eval.slots(slots);
        // EXISTS { } 和 VALUE { } 在当前这一行的绑定上执行。
        Map<String, Entity> row = new LinkedHashMap<>(entities);
        GraphDefinition graph = currentGraph;
        Map<String, Object> params = currentParams;
        BooleanSupplier canceled = currentCanceled;
        eval.subquery(query -> nested(graph, query, row, params, canceled));
    }
    private Entity scalar(Object value) {
        Entity identity = Entity.newBuilder().setType("\u0000scalar").setState(UUID.randomUUID().toString()).build();
        scalarValues.put(identity, value);
        return identity;
    }
    private static void check(BooleanSupplier canceled) {
        if (Thread.currentThread().isInterrupted() || canceled.getAsBoolean()) throw new ClusterException("QUERY_CANCELED", "查询已取消");
    }
    private static long parseBudget(String value) {
        String text = value.toUpperCase(Locale.ROOT);
        long multiplier = 1;
        if (text.endsWith("GB")) { multiplier = 1024L * 1024 * 1024; text = text.substring(0, text.length() - 2); }
        else if (text.endsWith("MB")) { multiplier = 1024L * 1024; text = text.substring(0, text.length() - 2); }
        else if (text.endsWith("KB")) { multiplier = 1024; text = text.substring(0, text.length() - 2); }
        else if (text.endsWith("B")) text = text.substring(0, text.length() - 1);
        return Math.multiplyExact(Long.parseLong(text), multiplier);
    }
}
