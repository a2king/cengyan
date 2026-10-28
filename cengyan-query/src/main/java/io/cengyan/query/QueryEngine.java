package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.AliasRow;
import io.cengyan.catalog.Auth;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.ClusterCodec;
import io.cengyan.catalog.GraphName;
import io.cengyan.catalog.Membership;
import io.cengyan.catalog.IndexDef;
import io.cengyan.catalog.NodeType;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.StatsRow;
import io.cengyan.catalog.ValueType;
import io.cengyan.catalog.VertexWrite;
import io.cengyan.parser.Gql;
import io.cengyan.parser.ParseException;
import io.cengyan.parser.Stmt;
import io.cengyan.store.Cf;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** 执行一条 GQL。同一会话上的语句串行，失败时不返回半截结果。 */
public final class QueryEngine {
    private final Catalog catalog;
    private final GraphStore store;
    private final Auth auth;
    private Membership membership;
    private final MatchExecutor match;
    private final MutateExecutor mutate;
    private final ProcedureExecutor procedures;

    public QueryEngine(Catalog catalog, GraphStore store) {
        this(catalog, store, "1GB", Path.of(System.getProperty("java.io.tmpdir"), "cengyan-duck"));
    }

    public QueryEngine(Catalog catalog, GraphStore store, String duckdbMemory, Path duckdbTempDirectory) {
        this(catalog, store, duckdbMemory, duckdbTempDirectory, 0.01d, 0.001d);
    }

    public QueryEngine(
            Catalog catalog,
            GraphStore store,
            String duckdbMemory,
            Path duckdbTempDirectory,
            double minEdgeRatio,
            double minAccumRatio) {
        this(catalog, store, duckdbMemory, duckdbTempDirectory, minEdgeRatio, minAccumRatio, null);
    }

    public QueryEngine(
            Catalog catalog,
            GraphStore store,
            String duckdbMemory,
            Path duckdbTempDirectory,
            double minEdgeRatio,
            double minAccumRatio,
            Auth auth) {
        if (catalog == null || store == null) {
            throw new IllegalArgumentException("目录和存储不能为空");
        }
        if (minEdgeRatio < 0d || minAccumRatio < 0d || !Double.isFinite(minEdgeRatio) || !Double.isFinite(minAccumRatio)) {
            throw new IllegalArgumentException("比例阈值不能为负");
        }
        this.catalog = catalog;
        this.store = store;
        this.auth = auth;
        DuckSettings duck = new DuckSettings(duckdbMemory, duckdbTempDirectory);
        this.match = new MatchExecutor(catalog, store, duck);
        this.mutate = new MutateExecutor(catalog, store);
        this.procedures = new ProcedureExecutor(catalog, store, minEdgeRatio, minAccumRatio);
    }

    public void bind(Membership bound) {
        if (bound == null) {
            throw new IllegalArgumentException("集群不能为空");
        }
        this.membership = bound;
    }

    public QueryResult execute(
            SessionState session,
            String gql,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        if (session == null) {
            throw new IllegalArgumentException("会话不能为空");
        }
        if (limits == null) {
            throw new IllegalArgumentException("查询上限不能为空");
        }
        synchronized (session) {
            int computeNode = bindCompute(session);
            try {
            Stmt stmt;
            try {
                stmt = Gql.parse(gql);
            } catch (ParseException ex) {
                throw new CengyanException(ErrorCode.SYNTAX, ex.getMessage(), ex);
            }
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            if (session.readOnly() && writes(stmt)) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "只读会话不能修改数据");
            }
            Map<String, Object> values = normalize(params);
            return switch (stmt) {
                case Stmt.CreateGraph create -> {
                    int created = catalog.createGraph(create.name());
                    if (membership != null) {
                        membership.ensureVotes(created);
                    }
                    yield QueryResult.changed(1);
                }
                case Stmt.Use use -> {
                    session.use(catalog.graph(use.name()));
                    yield QueryResult.changed(0);
                }
                case Stmt.CreateAlias alias -> {
                    catalog.createAlias(alias.alias(), alias.graph());
                    yield QueryResult.changed(1);
                }
                case Stmt.DropAlias drop -> {
                    catalog.dropAlias(drop.name());
                    yield QueryResult.changed(1);
                }
                case Stmt.DropGraph drop -> {
                    catalog.dropGraph(drop.name());
                    yield QueryResult.changed(1);
                }
                case Stmt.Show show -> show(session, show);
                case Stmt.Describe describe -> describe(session, describe);
                case Stmt.CreateType create -> {
                    createType(graphId(session), create);
                    yield QueryResult.changed(1);
                }
                case Stmt.CreateIndex create -> {
                    catalog.createIndex(
                            graphId(session), create.unique(), create.name(), create.node(),
                            create.typeName(), create.properties());
                    yield QueryResult.changed(1);
                }
                case Stmt.DropIndex drop -> {
                    catalog.dropIndex(graphId(session), drop.name());
                    yield QueryResult.changed(1);
                }
                case Stmt.AlterType alter -> {
                    alterType(graphId(session), alter);
                    yield QueryResult.changed(1);
                }
                case Stmt.Insert insert -> {
                    insert(graphId(session), insert, values);
                    yield QueryResult.changed(1);
                }
                case Stmt.Set set -> QueryResult.changed(mutate.set(set, graphId(session), values, limits, cancel));
                case Stmt.Delete delete -> QueryResult.changed(
                        mutate.delete(delete, graphId(session), values, limits, cancel));
                case Stmt.Match query -> match.run(query, graphId(session), values, limits, cancel);
                case Stmt.Call call -> procedures.call(call, graphId(session), values, limits, cancel);
            };
            } finally {
                if (computeNode != 0) {
                    membership.leave(computeNode);
                }
            }
        }
    }

    private int bindCompute(SessionState session) {
        if (membership == null) {
            return 0;
        }
        int chosen = membership.choose(session.computeNode());
        if (chosen == 0) {
            return 0;
        }
        if (session.computeNode() == 0) {
            session.pinCompute(chosen);
            membership.notePin(chosen);
        }
        membership.enter(chosen);
        return chosen;
    }

    private QueryResult show(SessionState session, Stmt.Show show) {
        return switch (show.kind()) {
            case GRAPHS -> {
                List<List<Object>> rows = new ArrayList<>();
                for (String name : catalog.graphs()) {
                    rows.add(List.of(name));
                }
                yield new QueryResult(List.of("name"), rows, 0);
            }
            case ALIASES -> {
                List<List<Object>> rows = new ArrayList<>();
                for (AliasRow alias : catalog.aliases()) {
                    rows.add(List.of(alias.alias(), alias.canonical(), alias.graphId()));
                }
                yield new QueryResult(List.of("alias", "canonical", "graphId"), rows, 0);
            }
            case NODE_TYPES -> types(true, graphId(session));
            case REL_TYPES -> types(false, graphId(session));
            case INDEXES -> indexes(graphId(session));
        };
    }

    private QueryResult indexes(int graphId) {
        List<List<Object>> rows = new ArrayList<>();
        for (IndexDef index : catalog.indexes(graphId)) {
            rows.add(List.of(
                    index.name(),
                    index.node() ? "NODE" : "REL",
                    typeName(graphId, index),
                    String.join(",", index.properties()),
                    index.unique(),
                    index.ready()));
        }
        return new QueryResult(List.of("name", "target", "type", "properties", "unique", "ready"), rows, 0);
    }

    private String typeName(int graphId, IndexDef index) {
        if (index.node()) {
            for (NodeType type : catalog.nodeTypes(graphId)) {
                if (type.id() == index.typeId()) {
                    return type.name();
                }
            }
        } else {
            for (RelType type : catalog.relTypes(graphId)) {
                if (type.id() == index.typeId()) {
                    return type.name();
                }
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "索引类型丢失：" + index.name());
    }

    public QueryResult login(SessionState session, String user, String password) {
        if (session == null) {
            throw new IllegalArgumentException("会话不能为空");
        }
        if (auth == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "还没有打开用户管理");
        }
        synchronized (session) {
            Auth.User found = auth.authenticate(user, password);
            if (found == null) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "用户或密码不正确");
            }
            session.bind(found.name(), found.role());
            String roleName = found.role() == Auth.ROLE_READ ? "read" : "write";
            return new QueryResult(List.of("user", "role"), List.of(List.of(found.name(), roleName)), 0);
        }
    }

    public QueryResult admin(SessionState session, String command, String index, CancelToken cancel) {
        return admin(session, command, index, "", "", "", cancel);
    }

    public QueryResult admin(
            SessionState session,
            String command,
            String index,
            String user,
            String password,
            String role,
            CancelToken cancel) {
        return admin(session, AdminCall.simple(command, index, user, password, role), cancel);
    }

    public QueryResult admin(SessionState session, AdminCall call, CancelToken cancel) {
        if (call == null) {
            throw new IllegalArgumentException("管理命令不能为空");
        }
        String command = call.command();
        String index = call.index();
        String user = call.user();
        String password = call.password();
        String role = call.role();
        if (session == null) {
            throw new IllegalArgumentException("会话不能为空");
        }
        if (command == null || command.isBlank()) {
            throw new CengyanException(ErrorCode.SYNTAX, "缺少 command");
        }
        synchronized (session) {
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            return switch (command) {
                case "rebuildIndex" -> {
                    requireWrite(session);
                    if (index == null || index.isBlank()) {
                        throw new CengyanException(ErrorCode.SYNTAX, "缺少索引名");
                    }
                    yield QueryResult.changed(catalog.rebuildIndex(graphId(session), index, cancel));
                }
                case "stats" -> {
                    List<List<Object>> rows = new ArrayList<>();
                    for (StatsRow row : catalog.stats(graphId(session), cancel)) {
                        rows.add(List.of(row.target(), row.name(), row.count(), row.maxDegree()));
                    }
                    yield new QueryResult(List.of("target", "name", "count", "maxDegree"), rows, 0);
                }
                case "storage" -> new QueryResult(
                        List.of("bytes"), List.of(List.of(store.storageSizeBytes())), 0);
                case "flush" -> {
                    requireWrite(session);
                    store.flush();
                    yield QueryResult.changed(0);
                }
                case "compact" -> {
                    requireWrite(session);
                    store.compact();
                    yield QueryResult.changed(0);
                }
                case "createUser" -> {
                    requireWrite(session);
                    requireAuth();
                    auth.createUser(user, password, parseRole(role));
                    yield QueryResult.changed(1);
                }
                case "setPassword" -> {
                    requireWrite(session);
                    requireAuth();
                    auth.setPassword(user, password);
                    yield QueryResult.changed(1);
                }
                case "setRole" -> {
                    requireWrite(session);
                    requireAuth();
                    auth.setRole(user, parseRole(role));
                    yield QueryResult.changed(1);
                }
                case "dropUser" -> {
                    requireWrite(session);
                    requireAuth();
                    auth.dropUser(user);
                    yield QueryResult.changed(1);
                }
                case "addStorage" -> {
                    requireWrite(session);
                    requireMembership();
                    yield nodeRow(membership.addStorage(call.address()));
                }
                case "drainStorage" -> {
                    requireWrite(session);
                    requireMembership();
                    membership.drainStorage(call.nodeId());
                    yield QueryResult.changed(1);
                }
                case "removeStorage" -> {
                    requireWrite(session);
                    requireMembership();
                    membership.removeStorage(call.nodeId());
                    yield QueryResult.changed(1);
                }
                case "addReplica" -> {
                    requireWrite(session);
                    requireMembership();
                    String graph = call.graph();
                    if (graph == null || graph.isBlank()) {
                        graph = session.current().canonical();
                    }
                    List<Integer> nodes = membership.addReplica(graph, call.replicas());
                    List<List<Object>> rows = new ArrayList<>();
                    for (int nodeId : nodes) {
                        rows.add(List.of(nodeId));
                    }
                    yield new QueryResult(List.of("nodeId"), rows, nodes.size());
                }
                case "addCompute" -> {
                    requireWrite(session);
                    requireMembership();
                    yield nodeRow(membership.addCompute(call.address()));
                }
                case "drainCompute" -> {
                    requireWrite(session);
                    requireMembership();
                    membership.drainCompute(call.nodeId());
                    yield QueryResult.changed(1);
                }
                case "removeCompute" -> {
                    requireWrite(session);
                    requireMembership();
                    membership.removeCompute(call.nodeId());
                    yield QueryResult.changed(1);
                }
                case "setPartCount" -> {
                    requireWrite(session);
                    int graphId = call.graph() == null || call.graph().isBlank()
                            ? graphId(session)
                            : catalog.graph(call.graph()).graphId();
                    catalog.setPartCount(graphId, call.parts());
                    if (membership != null) {
                        membership.ensureVotes(graphId);
                    }
                    yield QueryResult.changed(1);
                }
                case "showMembers" -> showMembers(session, call.graph());
                default -> throw new CengyanException(ErrorCode.SYNTAX, "无法识别的管理命令");
            };
        }
    }

    private void requireAuth() {
        if (auth == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "还没有打开用户管理");
        }
    }

    private void requireMembership() {
        if (membership == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "还没有打开集群管理");
        }
    }

    private static QueryResult nodeRow(int nodeId) {
        return new QueryResult(List.of("nodeId"), List.of(List.of(nodeId)), 1);
    }

    private QueryResult showMembers(SessionState session, String graph) {
        int graphId = graph == null || graph.isBlank()
                ? graphId(session)
                : catalog.graph(graph).graphId();
        List<List<Object>> rows = new ArrayList<>();
        try (ReadSession read = store.read()) {
            for (byte[] key : read.keysWithPrefix(Cf.CATALOG, KeyCodec.partVotePrefix(graphId))) {
                if (key.length != 7) {
                    continue;
                }
                byte[] value = read.get(Cf.CATALOG, key);
                if (value == null || ClusterCodec.voteKind(value) != ClusterCodec.VOTE_VOTER) {
                    continue;
                }
                rows.add(List.of(KeyCodec.voteNodeId(key), KeyCodec.votePartId(key), "voter"));
            }
        }
        rows.sort(java.util.Comparator
                .<List<Object>>comparingInt(row -> ((Number) row.get(1)).intValue())
                .thenComparingInt(row -> ((Number) row.get(0)).intValue()));
        return new QueryResult(List.of("nodeId", "partId", "vote"), rows, 0);
    }

    private static void requireWrite(SessionState session) {
        if (session.readOnly()) {
            throw new CengyanException(ErrorCode.FORBIDDEN, "只读会话不能修改数据");
        }
    }

    private static boolean writes(Stmt stmt) {
        return !(stmt instanceof Stmt.Use
                || stmt instanceof Stmt.Show
                || stmt instanceof Stmt.Describe
                || stmt instanceof Stmt.Match
                || stmt instanceof Stmt.Call);
    }

    private static int parseRole(String role) {
        if (role == null || role.isBlank()) {
            throw new CengyanException(ErrorCode.SYNTAX, "缺少角色");
        }
        return switch (role.toLowerCase(Locale.ROOT)) {
            case "read" -> Auth.ROLE_READ;
            case "write" -> Auth.ROLE_WRITE;
            default -> throw new CengyanException(ErrorCode.SYNTAX, "角色只能是 read 或 write");
        };
    }

    private QueryResult types(boolean node, int graphId) {
        List<List<Object>> rows = new ArrayList<>();
        if (node) {
            for (NodeType type : catalog.nodeTypes(graphId)) {
                rows.add(List.of(type.name(), type.id(), type.schemaVersion()));
            }
        } else {
            for (RelType type : catalog.relTypes(graphId)) {
                rows.add(List.of(type.name(), type.id(), type.schemaVersion()));
            }
        }
        return new QueryResult(List.of("name", "id", "schemaVersion"), rows, 0);
    }

    private QueryResult describe(SessionState session, Stmt.Describe describe) {
        int graphId = graphId(session);
        List<PropDef> properties = describe.node()
                ? catalog.nodeType(graphId, describe.name()).properties()
                : catalog.relType(graphId, describe.name()).properties();
        List<List<Object>> rows = new ArrayList<>();
        for (PropDef property : properties) {
            rows.add(List.of(property.name(), property.type().name(), property.nullable()));
        }
        return new QueryResult(List.of("property", "type", "nullable"), rows, 0);
    }

    private void createType(int graphId, Stmt.CreateType create) {
        List<PropDef> properties = new ArrayList<>();
        for (Stmt.Field field : create.fields()) {
            properties.add(new PropDef(field.name(), valueType(field.valueType()), field.nullable()));
        }
        if (create.node()) {
            catalog.createNodeType(graphId, create.name(), properties);
        } else {
            catalog.createRelType(graphId, create.name(), properties);
        }
    }

    private void alterType(int graphId, Stmt.AlterType alter) {
        PropDef property = new PropDef(alter.property(), valueType(alter.valueType()), true);
        if (alter.node()) {
            catalog.addNodeProperty(graphId, alter.typeName(), property);
        } else {
            catalog.addRelProperty(graphId, alter.typeName(), property);
        }
    }

    private void insert(int graphId, Stmt.Insert insert, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, true);
        if (insert.edge() == null) {
            catalog.insertVertex(graphId, requireType(insert.source()), idOf(insert.source(), eval),
                    properties(insert.source(), eval), false);
            return;
        }
        if (insert.edge().minHop() != null) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入边不能是多跳");
        }
        if (insert.edge().direction() == Stmt.EdgePat.Direction.ANY) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入边必须有方向");
        }
        if (insert.edge().typeNames().size() != 1) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入边必须写一个类型");
        }
        boolean outward = insert.edge().direction() == Stmt.EdgePat.Direction.OUT;
        Stmt.NodePat from = outward ? insert.source() : insert.target();
        Stmt.NodePat to = outward ? insert.target() : insert.source();
        long rank = insert.edge().rank() == null ? 0L : insert.edge().rank();
        catalog.insertRel(
                graphId,
                insert.edge().typeName(),
                rank,
                vertex(from, eval),
                vertex(to, eval),
                properties(insert.edge().properties(), eval),
                false);
    }

    private static VertexWrite vertex(Stmt.NodePat node, ExprEval eval) {
        long vid = idOf(node, eval);
        if (node.typeName() == null) {
            return VertexWrite.onlyId(vid);
        }
        return VertexWrite.of(node.typeName(), vid, properties(node, eval));
    }

    private static String requireType(Stmt.NodePat node) {
        if (node.typeName() == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入点需要节点类型");
        }
        return node.typeName();
    }

    private static long idOf(Stmt.NodePat node, ExprEval eval) {
        for (Stmt.Prop prop : node.properties()) {
            if (!"id".equals(prop.name())) {
                continue;
            }
            Object value = eval.eval(prop.value());
            if (!(value instanceof Long id)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "id 必须是整数");
            }
            return id;
        }
        throw new CengyanException(ErrorCode.SYNTAX, "插入需要 id");
    }

    private static Map<String, Object> properties(Stmt.NodePat node, ExprEval eval) {
        return properties(node.properties(), eval);
    }

    private static Map<String, Object> properties(List<Stmt.Prop> props, ExprEval eval) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Stmt.Prop prop : props) {
            if ("id".equals(prop.name())) {
                continue;
            }
            Object value = eval.eval(prop.value());
            if (value instanceof UtcTime time) {
                value = time.millis();
            }
            values.put(prop.name(), value);
        }
        return values;
    }

    private static int graphId(SessionState session) {
        GraphName graph = session.current();
        return graph.graphId();
    }

    static ValueType valueType(String text) {
        if (text == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        return switch (text.toUpperCase(Locale.ROOT)) {
            case "BOOL", "BOOLEAN" -> ValueType.BOOL;
            case "INT", "INTEGER", "INT64" -> ValueType.INT64;
            case "DOUBLE" -> ValueType.DOUBLE;
            case "STRING" -> ValueType.STRING;
            case "DATETIME" -> ValueType.DATETIME;
            default -> throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知列类型 " + text);
        };
    }

    static Map<String, Object> normalize(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            copy.put(entry.getKey(), number(entry.getValue()));
        }
        return copy;
    }

    private static Object number(Object value) {
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof Float) {
            return ((Float) value).doubleValue();
        }
        return value;
    }
}
