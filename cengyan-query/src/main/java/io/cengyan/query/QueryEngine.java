package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.AliasRow;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.GraphName;
import io.cengyan.catalog.NodeType;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.ValueType;
import io.cengyan.catalog.VertexWrite;
import io.cengyan.parser.Gql;
import io.cengyan.parser.ParseException;
import io.cengyan.parser.Stmt;
import io.cengyan.store.GraphStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** 执行一条 GQL。同一会话上的语句串行，失败时不返回半截结果。 */
public final class QueryEngine {
    private final Catalog catalog;
    private final MatchExecutor match;

    public QueryEngine(Catalog catalog, GraphStore store) {
        if (catalog == null || store == null) {
            throw new IllegalArgumentException("目录和存储不能为空");
        }
        this.catalog = catalog;
        this.match = new MatchExecutor(catalog, store);
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
            Stmt stmt;
            try {
                stmt = Gql.parse(gql);
            } catch (ParseException ex) {
                throw new CengyanException(ErrorCode.SYNTAX, ex.getMessage(), ex);
            }
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            Map<String, Object> values = normalize(params);
            return switch (stmt) {
                case Stmt.CreateGraph create -> {
                    catalog.createGraph(create.name());
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
                case Stmt.AlterType alter -> {
                    alterType(graphId(session), alter);
                    yield QueryResult.changed(1);
                }
                case Stmt.Insert insert -> {
                    insert(graphId(session), insert, values);
                    yield QueryResult.changed(1);
                }
                case Stmt.Match query -> match.run(
                        query, graphId(session), values, limits.maxEdgesPerVertex(), cancel);
            };
        }
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
        if (insert.edge().direction() == Stmt.EdgePat.Direction.ANY) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入边必须有方向");
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
