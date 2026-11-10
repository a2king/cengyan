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
    private final DistributedCatalog catalog;
    private final DistributedGraph data;
    private final DuckSettings duck;
    private final StorageGateway storage;
    private TransactionEngine.QueryScope queryScope;
    private Stmt supplied;
    private final IdentityHashMap<Entity, Object> scalarValues = new IdentityHashMap<>();
    private List<Map<String, Entity>> incoming;
    private RemoteCancellation cancellation;
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
            if (replay != null) return QueryResult.changed(Math.toIntExact(replay.affected()));
            var planned = storage.get(0, 0, TransactionEngine.text("gql/plan/" + requestId));
            if (planned != null) {
                try {
                    GqlWritePlan plan = GqlWritePlan.parseFrom(planned);
                    return applyWritePlan(requestId, graphName, plan);
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "GQL 写入计划损坏", ex); }
            }
        }
        ExprEval eval = new ExprEval(params, false);
        if (statement instanceof Stmt.Select select && select.clauses().isEmpty() && (graphName == null || graphName.isBlank())) {
            return project(List.of(Map.of()), select.tail().items(), select.tail().orderBy(), select.tail().distinct(), select.tail().skip(), select.tail().limit(), eval, canceled);
        }
        if (statement instanceof Stmt.CreateGraph create) {
            catalog.create(requestId, create.name(), create.parts(), 1, IdType.INT64, IdType.INT64, create.comment());
            return QueryResult.changed(1);
        }
        if (statement instanceof Stmt.Show show && show.kind() == Stmt.Show.Kind.GRAPHS) {
            return new QueryResult(List.of("name", "comment"), catalog.graphs().stream().map(g -> List.<Object>of(g.getName(), g.getComment())).toList(), 0);
        }
        if (statement instanceof Stmt.Show show && show.kind() == Stmt.Show.Kind.ALIASES) return new QueryResult(List.of("alias", "graph"), catalog.aliases(), 0);
        if (statement instanceof Stmt.CreateAlias alias) { catalog.createAlias(requestId, alias.alias(), alias.graph()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.DropAlias alias) { catalog.dropAlias(requestId, alias.name()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.DropGraph drop) { catalog.dropGraph(requestId, drop.name()); return QueryResult.changed(1); }
        if (statement instanceof Stmt.Use use) graphName = use.name();
        if (graphName == null || graphName.isBlank()) throw new ClusterException("GRAPH_NOT_FOUND", "请求必须携带目标图");
        GraphDefinition graph = schemaVersion == 0 ? catalog.graph(graphName) : catalog.version(graphName, schemaVersion);
        // USE and Schema/catalog operations must not depend on Data Group availability.
        // Data-reading children of composite queries still establish/share their normal transaction view.
        if (statement instanceof Stmt.Use) return QueryResult.changed(0);
        if (statement instanceof Stmt.Select select && select.clauses().isEmpty() && incoming == null) {
            return project(List.of(Map.of()), select.tail().items(), select.tail().orderBy(), select.tail().distinct(), select.tail().skip(), select.tail().limit(), eval, canceled);
        }
        if (!(statement instanceof Stmt.Show || statement instanceof Stmt.Describe
                || statement instanceof Stmt.Explain || statement instanceof Stmt.ExplainQuery
                || statement instanceof Stmt.CreateType || statement instanceof Stmt.AlterType
                || statement instanceof Stmt.CreateIndex || statement instanceof Stmt.DropIndex
                || statement instanceof Stmt.DropType) && queryScope == null) {
            queryScope = data.readScope(graph.getId());
        }
        if (statement instanceof Stmt.Call call) return new RemoteProcedures(data, graph, queryScope, canceled).execute(call, eval);
        if (statement instanceof Stmt.Explain || statement instanceof Stmt.ExplainQuery) {
            return new QueryResult(List.of("plan"), List.of(List.of("ComputeCoordinator"), List.of("StorageLeaderScan / IndexLookup / PredicatePushdown"),
                    List.of("ComputeJoin / DuckDBAggregate / Sort / Limit")), 0);
        }
        if (statement instanceof Stmt.Composite composite) {
            QueryResult left = child(composite.left(), requestId, graphName, params, canceled, schemaVersion);
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
            QueryResult result = child(filter.query(), requestId, graphName, params, canceled, schemaVersion);
            List<List<Object>> kept = new ArrayList<>();
            for (var row : result.rows()) {
                var aliases = new HashMap<String, ExprEval.Slot>();
                for (int i = 0; i < result.columns().size(); i++) aliases.put(result.columns().get(i), ExprEval.Slot.scalar(row.get(i)));
                eval.slots(aliases);
                if (eval.isTrue(filter.condition())) kept.add(row);
            }
            return new QueryResult(result.columns(), kept, result.rowsAffected());
        }
        if (statement instanceof Stmt.InsertGraph insertion) {
            List<Entity> entities = insertElements(insertion.elements(), Map.of(), eval);
            return QueryResult.changed(Math.toIntExact(data.put(requestId, graphName, entities, false, Duration.ofMinutes(5), true).affected()));
        }
        if (statement instanceof Stmt.InsertUsing insertion) {
            List<Map<String, Entity>> rows = clauses(graph, insertion.clauses(), params, canceled);
            List<Entity> entities = new ArrayList<>();
            for (var bound : rows) {
                check(canceled); slots(eval, bound);
                entities.addAll(insertElements(insertion.elements(), bound, eval));
                QueryMemory.current().observeRows(entities.size(), 8);
            }
            return entities.isEmpty() ? QueryResult.changed(0)
                    : QueryResult.changed(Math.toIntExact(data.put(requestId, graphName, entities, false, Duration.ofMinutes(5)).affected()));
        }
        if (statement instanceof Stmt.CreateType type) {
            var definition = TypeDefinition.newBuilder().setName(type.name()).setNode(type.node()).setComment(type.comment() == null ? "" : type.comment());
            if (type.node() && type.idSpecified()) {
                definition.setIdType(type.idType().equalsIgnoreCase("STRING") ? IdType.STRING : IdType.INT64).setIdLength(type.idLength());
            }
            for (var field : type.fields()) definition.addProperties(PropertyDefinition.newBuilder().setName(field.name()).setType(field.valueType()).setNullable(field.nullable()).setComment(field.comment() == null ? "" : field.comment()));
            catalog.addType(requestId, graphName, definition.build()); return QueryResult.changed(1);
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
                        int idLength = t.getIdLength() == 0 ? (idType == IdType.STRING ? 4096 : 19) : t.getIdLength();
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
            return project(bindings, match.items(), match.orderBy(), match.distinct(), match.skip(), match.limit(), eval, canceled);
        }
        if (statement instanceof Stmt.Select select) {
            List<Map<String, Entity>> rows = clauses(graph, select.clauses(), params, canceled);
            List<Stmt.Item> items = select.tail().items();
            if (select.tail().star()) {
                Set<String> variables = new TreeSet<>(); rows.forEach(row -> variables.addAll(row.keySet()));
                items = variables.stream().map(name -> new Stmt.Item(new Expr.Variable(name), name, name)).toList();
            }
            QueryResult projected = project(rows, items, select.tail().orderBy(), select.tail().distinct(), select.tail().skip(), select.tail().limit(), eval, canceled);
            if (select.tail().postFilter() != null) {
                List<List<Object>> kept = new ArrayList<>();
                for (var row : projected.rows()) {
                    var values = new HashMap<String, ExprEval.Slot>();
                    for (int i = 0; i < projected.columns().size(); i++) values.put(projected.columns().get(i), ExprEval.Slot.scalar(row.get(i)));
                    eval.slots(values); if (eval.isTrue(select.tail().postFilter())) kept.add(row);
                }
                projected = new QueryResult(projected.columns(), kept, projected.rowsAffected());
            }
            if (!select.tail().groupBy().isEmpty()) {
                // 投影中非聚合项已经作为分组键；显式 GROUP BY 不允许悄悄忽略额外表达式。
                Set<Expr> projectedGroups = new HashSet<>();
                for (var item : items) if (!(item.expr() instanceof Expr.Call call && Set.of("count", "sum", "avg", "min", "max").contains(call.name().toLowerCase(Locale.ROOT)))) projectedGroups.add(item.expr());
                if (!projectedGroups.equals(new HashSet<>(select.tail().groupBy()))) throw new ClusterException("UNSUPPORTED", "显式 GROUP BY 必须与非聚合返回项一致");
            }
            return projected;
        }
        if (statement instanceof Stmt.Mutate mutate) {
            List<Map<String, Entity>> rows = clauses(graph, mutate.clauses(), params, canceled);
            LinkedHashMap<String, Entity> changes = new LinkedHashMap<>();
            LinkedHashMap<String, Entity> deletions = new LinkedHashMap<>();
            LinkedHashMap<String, Entity> expected = new LinkedHashMap<>();
            for (var row : rows) {
                row.values().forEach(entity -> { if (!scalarValues.containsKey(entity)) expected.put(entity.getNode() + "/" + entity.getId(), entity); });
                slots(eval, row);
                for (Stmt.Assign assignment : mutate.assignments()) {
                    Entity entity = row.get(assignment.variable());
                    if (entity == null || assignment.field().equals("id")) throw new ClusterException("INVALID_ARGUMENT", "不能修改 ID 或未绑定实体");
                    String key = entity.getNode() + "/" + entity.getId();
                    changes.put(key, changes.getOrDefault(key, entity).toBuilder().putProperties(assignment.field(), EntityCodec.value(eval.eval(assignment.value()))).build());
                }
                for (Stmt.Assign replacement : mutate.replacements()) {
                    Entity entity = row.get(replacement.variable());
                    if (entity == null) throw new ClusterException("INVALID_ARGUMENT", "未绑定实体");
                    Object value = eval.eval(replacement.value());
                    if (!(value instanceof Map<?, ?> properties)) throw new ClusterException("TYPE_MISMATCH", "完整属性替换必须使用属性 Map");
                    var changed = entity.toBuilder().clearProperties();
                    properties.forEach((key, property) -> changed.putProperties(String.valueOf(key), EntityCodec.value(property)));
                    changes.put(entity.getNode() + "/" + entity.getId(), changed.build());
                }
                for (String removal : mutate.removals()) {
                    int dot = removal.indexOf('.');
                    if (dot < 1) throw new ClusterException("UNSUPPORTED", "REMOVE 仅支持实体属性");
                    Entity entity = row.get(removal.substring(0, dot));
                    if (entity == null) throw new ClusterException("INVALID_ARGUMENT", "REMOVE 未绑定实体");
                    String key = entity.getNode() + "/" + entity.getId();
                    changes.put(key, changes.getOrDefault(key, entity).toBuilder().removeProperties(removal.substring(dot + 1)).build());
                }
                for (String variable : mutate.deletes()) {
                    Entity entity = row.get(variable);
                    if (entity != null) deletions.put(entity.getNode() + "/" + entity.getId(), entity);
                }
            }
            if (!changes.isEmpty() && !deletions.isEmpty()) throw new ClusterException("UNSUPPORTED", "第一版同一请求不能混合属性更新与级联删除");
            return freezeWritePlan(requestId, graphName, new ArrayList<>(changes.values()), new ArrayList<>(deletions.values()), new ArrayList<>(expected.values()));
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
                    String key = entity.getNode() + "/" + entity.getId();
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
                if (entity != null) entities.put(entity.getNode() + "/" + entity.getId(), entity);
            }
            return freezeWritePlan(requestId, graphName, List.of(), new ArrayList<>(entities.values()), new ArrayList<>(entities.values()));
        }
        throw new ClusterException("UNSUPPORTED", "该 GQL 算子尚未接入分布式执行器：" + statement.getClass().getSimpleName());
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
                else data.deleteEdge(sub, graph, entity.getId(), Duration.ofMinutes(5));
            }
            var key = TransactionEngine.text("gql/plan/" + requestId);
            try { storage.write(0, 0, StorageGateway.batch("gql.complete/" + requestId).addConditions(StorageGateway.condition(key, plan.toByteString()))
                    .addMutations(TransactionEngine.put(key, plan.toBuilder().setCompleted(true).build().toByteString())).build()); }
            catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        return QueryResult.changed(Math.toIntExact(plan.getAffected()));
    }
    private List<Entity> insertElements(List<Stmt.Element> elements, Map<String, Entity> bound, ExprEval eval) {
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
                entities.add(entity.build());
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
            entities.add(entity.build());
        }
        return entities;
    }
    private List<Map<String, Entity>> clauses(GraphDefinition graph, List<Stmt.Clause> clauses, Map<String, Object> params, BooleanSupplier canceled) {
        ExprEval eval = new ExprEval(params, false);
        List<Map<String, Entity>> rows = incoming == null ? new ArrayList<>(List.of(new LinkedHashMap<>())) : new ArrayList<>(incoming);
        for (Stmt.Clause clause : clauses) {
            check(canceled);
            if (clause instanceof Stmt.Clause.Filter filter) {
                rows = rows.stream().filter(row -> { slots(eval, row); return eval.isTrue(filter.expr()); }).toList();
            } else if (clause instanceof Stmt.Clause.Match match) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (Map<String, Entity> input : rows) {
                    List<Map<String, Entity>> matched = List.of(input);
                    for (Stmt.Pattern pattern : match.patterns()) {
                        List<Map<String, Entity>> combined = new ArrayList<>();
                        for (var seed : matched) {
                            if (match.pathVariable() != null || match.search() != null && !match.search().isBlank()) {
                                combined.addAll(pathPattern(graph, pattern.elements(), seed, match, params, canceled));
                            } else combined.addAll(pattern(graph, pattern.elements(), seed, params, canceled, match.mode()));
                        }
                        matched = combined;
                    }
                    int before = next.size();
                    for (var bound : matched) {
                        slots(eval, bound);
                        if (match.where() == null || eval.isTrue(match.where())) next.add(bound);
                    }
                    if (match.optional() && next.size() == before) {
                        var missing = new LinkedHashMap<>(input);
                        for (Stmt.Pattern pattern : match.patterns()) bindMissing(pattern.elements(), missing);
                        if (match.pathVariable() != null && !match.pathVariable().isBlank()) {
                            missing.computeIfAbsent(match.pathVariable(), ignored -> scalar(null));
                        }
                        next.add(missing);
                    }
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
                    if (!(collection instanceof Iterable<?> values)) throw new ClusterException("TYPE_MISMATCH", "FOR 必须展开列表");
                    for (Object value : values) {
                        check(canceled);
                        var bound = new LinkedHashMap<>(row); bound.put(loop.name(), scalar(value)); next.add(bound);
                        QueryMemory.current().observeRows(next.size(), Math.max(1, bound.size()));
                    }
                }
                rows = next;
            } else if (clause instanceof Stmt.Clause.Call call) {
                List<Map<String, Entity>> next = new ArrayList<>();
                for (var input : rows) {
                    slots(eval, input);
                    QueryResult result = new RemoteProcedures(data, graph, queryScope, canceled).execute(call.call(), eval);
                    for (List<Object> values : result.rows()) {
                        var bound = new LinkedHashMap<>(input);
                        for (int i = 0; i < result.columns().size(); i++) bound.put(result.columns().get(i), scalar(values.get(i)));
                        next.add(bound);
                    }
                }
                rows = next;
            } else throw new ClusterException("UNSUPPORTED", "该查询子句尚未接入远程执行：" + clause.getClass().getSimpleName());
            QueryMemory.current().observe(rows);
        }
        return rows;
    }
    private QueryResult child(Stmt statement, String requestId, String graph, Map<String, Object> params, BooleanSupplier canceled, long version) {
        RemoteQueryEngine engine = new RemoteQueryEngine(storage, duck.tempDirectory(), duck.memoryLimit(), duck.maxTempDirectorySize(), duck.threads());
        engine.queryScope = queryScope; engine.supplied = statement;
        return engine.execute(requestId, graph, "", params, canceled, version);
    }

    private List<Map<String, Entity>> pattern(GraphDefinition graph, List<Stmt.Element> elements, Map<String, Entity> input,
                                             Map<String, Object> params, BooleanSupplier canceled, String mode) {
        if (elements.isEmpty() || !(elements.getFirst() instanceof Stmt.Element.Node first)) throw new ClusterException("UNSUPPORTED", "远程模式必须以节点开始");
        var eval = new ExprEval(params, false);
        Stmt.NodePat source = constrain(first.node(), input);
        var scope = queryScope;
        if (elements.size() == 1) {
            List<Map<String, Entity>> result = new ArrayList<>();
            for (var found : bindings(graph, source, null, null, null, eval, scope, canceled, mode)) {
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
                for (var found : bindings(graph, constrain(a.node(), row), e.edge(), constrain(b.node(), row), null, eval, scope, canceled, mode)) {
                    var merged = mergeBindings(row, found);
                    if (merged != null) next.add(merged);
                }
            }
            rows = next;
            QueryMemory.current().observe(rows);
        }
        return rows;
    }
    private List<Map<String, Entity>> pathPattern(GraphDefinition graph, List<Stmt.Element> elements, Map<String, Entity> input,
                                                 Stmt.Clause.Match match, Map<String, Object> params, BooleanSupplier canceled) {
        if (elements.size() != 3 || !(elements.get(0) instanceof Stmt.Element.Node from)
                || !(elements.get(1) instanceof Stmt.Element.Rel rel) || !(elements.get(2) instanceof Stmt.Element.Node to)) {
            throw new ClusterException("UNSUPPORTED", "路径搜索第一版要求单个有界边模式");
        }
        ExprEval eval = new ExprEval(params, false);
        Stmt.NodePat source = constrain(from.node(), input), target = constrain(to.node(), input);
        Object sourceId = property(source.properties(), "id", eval);
        List<Entity> starts;
        if (sourceId == null) starts = data.scan(graph, true, source.typeName(), queryScope);
        else {
            Entity entity = data.get(graph, true, EntityCodec.value(sourceId), queryScope);
            starts = entity == null ? List.of() : List.of(entity);
        }
        int min = rel.edge().minHop() == null ? 1 : rel.edge().minHop();
        int max = rel.edge().maxHop() == null ? 1 : rel.edge().maxHop();
        if (max < 0 || max > 32 || min < 0 || min > max) throw new ClusterException("PATH_LIMIT", "路径搜索必须提供不超过 32 的跳数边界");
        List<Map<String, Entity>> output = new ArrayList<>();
        for (Entity start : starts) {
            var bound = new LinkedHashMap<>(input);
            if (source.variable() != null) bound.put(source.variable(), start);
            if (!matches(source, start, eval, bound)) continue;
            List<PathFound> paths = new ArrayList<>();
            List<Entity> vertices = new ArrayList<>(); vertices.add(start);
            walkPaths(graph, rel.edge(), target, bound, eval, canceled, match.mode() == null ? "WALK" : match.mode(),
                    min, max, vertices, new ArrayList<>(), paths, match.where(), match.pathVariable());
            paths.sort(Comparator.comparingInt(path -> path.edges().size()));
            Map<Value, List<PathFound>> byEnd = new LinkedHashMap<>();
            for (PathFound path : paths) byEnd.computeIfAbsent(path.vertices().getLast().getId(), ignored -> new ArrayList<>()).add(path);
            for (var choices : byEnd.values()) {
                int shortest = choices.getFirst().edges().size(), accepted = 0;
                for (PathFound path : choices) {
                    String search = match.search();
                    if (search != null && search.contains("SHORTEST") && path.edges().size() != shortest) break;
                    if (search != null && (search.equals("ANY_SHORTEST") || search.equals("ANY")) && accepted >= (match.searchCount() == null ? 1 : match.searchCount())) break;
                    if (search != null && search.equals("SHORTEST") && accepted >= (match.searchCount() == null ? 1 : match.searchCount())) break;
                    var row = new LinkedHashMap<>(bound);
                    if (target.variable() != null) row.put(target.variable(), path.vertices().getLast());
                    if (rel.edge().variable() != null) row.put(rel.edge().variable(), scalar(path.edges().stream().map(this::entityBody).toList()));
                    if (match.pathVariable() != null) row.put(match.pathVariable(), scalar(pathBody(path)));
                    output.add(row); accepted++;
                }
            }
        }
        return output;
    }
    private record PathFound(List<Entity> vertices, List<Entity> edges) { }
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
            var row = new LinkedHashMap<>(initial);
            if (target.variable() != null) row.put(target.variable(), current);
            if (pattern.variable() != null) row.put(pattern.variable(), scalar(edges.stream().map(this::entityBody).toList()));
            if (pathVariable != null) row.put(pathVariable, scalar(pathBody(new PathFound(vertices, edges))));
            if (matches(target, current, eval, row)) {
                slots(eval, row);
                if (where == null || eval.isTrue(where)) found.add(new PathFound(List.copyOf(vertices), List.copyOf(edges)));
            }
        }
        if (edges.size() == max) return;
        if (mode.equalsIgnoreCase("SIMPLE") && vertices.size() > 1 && current.getId().equals(vertices.getFirst().getId())) return;
        List<Entity> candidates = new ArrayList<>();
        if (pattern.direction() != Stmt.EdgePat.Direction.IN) candidates.addAll(data.neighbors(graph, current.getId(), true, pattern.typeName(), queryScope));
        if (pattern.direction() != Stmt.EdgePat.Direction.OUT) candidates.addAll(data.neighbors(graph, current.getId(), false, pattern.typeName(), queryScope));
        Set<Value> seen = new HashSet<>();
        for (Entity edge : candidates) {
            if (!seen.add(edge.getId()) || mode.equalsIgnoreCase("TRAIL") && edges.stream().anyMatch(existing -> existing.getId().equals(edge.getId()))) continue;
            Value id = edge.getSource().equals(current.getId()) ? edge.getTarget() : edge.getSource();
            boolean repeated = vertices.stream().anyMatch(vertex -> vertex.getId().equals(id));
            if (repeated && mode.equalsIgnoreCase("ACYCLIC")) continue;
            if (repeated && mode.equalsIgnoreCase("SIMPLE") && !id.equals(vertices.getFirst().getId())) continue;
            Entity next = data.get(graph, true, id, queryScope);
            if (next == null) continue;
            var row = new LinkedHashMap<>(initial);
            if (pattern.variable() != null) row.put(pattern.variable(), edge);
            slots(eval, row);
            boolean accepted = true;
            for (var property : pattern.properties()) {
                Object actual = property.name().equals("id") ? EntityCodec.object(edge.getId()) : EntityCodec.object(edge.getPropertiesOrDefault(property.name(), Value.getDefaultInstance()));
                if (!eval.sameValue(actual, eval.eval(property.value()))) accepted = false;
            }
            if (!accepted) continue;
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
        List<Map<String, Entity>> result = new ArrayList<>();
        Object id = property(source.properties(), "id", eval);
        List<Entity> starts;
        if (id != null) {
            Entity found = data.get(graph, true, EntityCodec.value(id), scope);
            starts = found == null ? List.of() : List.of(found);
        } else {
            Map<String, Value> filters = new HashMap<>();
            for (var prop : source.properties()) filters.put(prop.name(), EntityCodec.value(eval.eval(prop.value())));
            starts = filters.isEmpty() ? data.scan(graph, true, source.typeName(), scope) : data.lookup(graph, true, source.typeName(), filters, scope);
        }
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
            List<Entity> edges = new ArrayList<>();
            if (edge.direction() != Stmt.EdgePat.Direction.IN) edges.addAll(data.neighbors(graph, node.getId(), true, edge.typeName(), scope));
            if (edge.direction() != Stmt.EdgePat.Direction.OUT) edges.addAll(data.neighbors(graph, node.getId(), false, edge.typeName(), scope));
            Set<Value> seen = new HashSet<>();
            for (Entity relation : edges) {
                if (!seen.add(relation.getId())) continue;
                Value otherId = relation.getSource().equals(node.getId()) ? relation.getTarget() : relation.getSource();
                Entity other = data.get(graph, true, otherId, scope);
                if (other == null) continue;
                var joined = new LinkedHashMap<>(row);
                if (edge.variable() != null) joined.put(edge.variable(), relation);
                if (target.variable() != null) joined.put(target.variable(), other);
                if (!matches(target, other, eval, joined)) continue;
                slots(eval, joined);
                boolean edgeMatches = true;
                for (var prop : edge.properties()) {
                    Object actual = prop.name().equals("id") ? EntityCodec.object(relation.getId()) : EntityCodec.object(relation.getPropertiesOrDefault(prop.name(), Value.getDefaultInstance()));
                    if (!eval.sameValue(actual, eval.eval(prop.value()))) edgeMatches = false;
                }
                if (edgeMatches && (where == null || eval.isTrue(where))) result.add(joined);
            }
        }
        QueryMemory.current().observe(result);
        return result;
    }

    private void expand(GraphDefinition graph, Entity start, Entity current, Map<String, Entity> initial, Stmt.EdgePat edge,
                         Stmt.NodePat target, Expr where, ExprEval eval, TransactionEngine.QueryScope scope,
                         BooleanSupplier canceled, int hop, int min, int max, Set<Value> usedEdges, Set<Value> visited, String mode, List<Map<String, Entity>> result) {
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
        List<Entity> edges = new ArrayList<>();
        if (edge.direction() != Stmt.EdgePat.Direction.IN) edges.addAll(data.neighbors(graph, current.getId(), true, edge.typeName(), scope));
        if (edge.direction() != Stmt.EdgePat.Direction.OUT) edges.addAll(data.neighbors(graph, current.getId(), false, edge.typeName(), scope));
        Set<Value> seen = new HashSet<>();
        for (Entity relation : edges) {
            if (!seen.add(relation.getId())) continue;
            if (mode.equalsIgnoreCase("TRAIL") && usedEdges.contains(relation.getId())) continue;
            Value next = relation.getSource().equals(current.getId()) ? relation.getTarget() : relation.getSource();
            boolean repeated = visited.contains(next);
            boolean closeStart = mode.equalsIgnoreCase("SIMPLE") && next.equals(start.getId());
            if (repeated && (mode.equalsIgnoreCase("ACYCLIC") || mode.equalsIgnoreCase("SIMPLE")) && !closeStart) continue;
            Entity other = data.get(graph, true, next, scope);
            if (other == null) continue;
            var bound = new LinkedHashMap<>(initial);
            if (edge.variable() != null) bound.put(edge.variable(), relation);
            slots(eval, bound);
            boolean valid = true;
            for (var property : edge.properties()) {
                Object value = property.name().equals("id") ? EntityCodec.object(relation.getId()) : EntityCodec.object(relation.getPropertiesOrDefault(property.name(), Value.getDefaultInstance()));
                if (!eval.sameValue(value, eval.eval(property.value()))) valid = false;
            }
            if (valid) {
                boolean newEdge = usedEdges.add(relation.getId()), newVertex = visited.add(next);
                expand(graph, start, other, bound, edge, target, where, eval, scope, canceled, hop + 1, min, closeStart ? hop + 1 : max, usedEdges, visited, mode, result);
                if (newEdge) usedEdges.remove(relation.getId());
                if (newVertex) visited.remove(next);
            }
        }
    }

    private QueryResult project(List<Map<String, Entity>> bindings, List<Stmt.Item> items, List<Stmt.Sort> sorts, boolean distinct,
                                Expr skip, Expr limit, ExprEval eval, BooleanSupplier canceled) {
        boolean aggregate = items.stream().anyMatch(item -> item.expr() instanceof Expr.Call c && Set.of("count", "sum", "avg", "min", "max").contains(c.name().toLowerCase(Locale.ROOT)));
        List<List<Object>> rows = new ArrayList<>(), keys = new ArrayList<>();
        String[] kinds = new String[items.size()];
        for (var row : bindings) {
            check(canceled); slots(eval, row);
            List<Object> projected = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                Expr expression = items.get(i).expr();
                if (aggregate && expression instanceof Expr.Call call && Set.of("count", "sum", "avg", "min", "max").contains(call.name().toLowerCase(Locale.ROOT))) {
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
                kinds[i] = expression instanceof Expr.Call c ? (c.args().isEmpty() ? "star" : c.name().toLowerCase(Locale.ROOT)) : "group";
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
        Long offset = skip == null ? null : ((Number) eval.eval(skip)).longValue();
        Long max = limit == null ? null : ((Number) eval.eval(limit)).longValue();
        rows = DuckResult.arrange(rows, keys, sorts, distinct, offset, max, duck, cancellation);
        return new QueryResult(items.stream().map(item -> item.alias() == null ? item.text() : item.alias()).toList(), rows, 0);
    }

    private static void addInsertNode(Stmt.NodePat pattern, ExprEval eval, List<Entity> entities) {
        if (pattern == null || pattern.typeName() == null) return;
        Object id = property(pattern.properties(), "id", eval);
        if (id == null) throw new ClusterException("INVALID_ARGUMENT", "节点必须提供 id");
        var entity = Entity.newBuilder().setNode(true).setType(pattern.typeName()).setId(EntityCodec.value(id));
        for (var prop : pattern.properties()) if (!prop.name().equals("id")) entity.putProperties(prop.name(), EntityCodec.value(eval.eval(prop.value())));
        entities.add(entity.build());
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
