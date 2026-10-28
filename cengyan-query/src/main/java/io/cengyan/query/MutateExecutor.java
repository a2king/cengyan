package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.Mutation;
import io.cengyan.catalog.NodeType;
import io.cengyan.catalog.PropCodec;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.ValueType;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Stmt;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.VertexId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** SET 和 DELETE。匹配和条件都在写锁里完成，整句只提交一次。 */
final class MutateExecutor {
    private final Catalog catalog;
    private final GraphStore store;

    MutateExecutor(Catalog catalog, GraphStore store) {
        this.catalog = catalog;
        this.store = store;
    }

    int set(Stmt.Set statement, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        rejectQuantifier(statement.edge());
        return store.withWriteLock(() -> change(
                graphId, statement.source(), statement.edge(), statement.target(), statement.where(),
                statement.assignments(), null, params, limits.maxEdgesPerVertex(), cancel));
    }

    int delete(Stmt.Delete statement, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        rejectQuantifier(statement.edge());
        return store.withWriteLock(() -> change(
                graphId, statement.source(), statement.edge(), statement.target(), statement.where(),
                null, statement.variables(), params, limits.maxEdgesPerVertex(), cancel));
    }

    private int change(
            int graphId,
            Stmt.NodePat source,
            Stmt.EdgePat edge,
            Stmt.NodePat target,
            Expr where,
            List<Stmt.Assign> assignments,
            List<String> variables,
            Map<String, Object> params,
            int maxEdges,
            CancelToken cancel) {
        Bound bound = bind(graphId, source, edge, target, where, assignments, variables);
        ExprEval eval = new ExprEval(params, false);
        try (ReadSession session = store.read()) {
            List<Mutation> mutations = edge == null
                    ? nodes(session, graphId, bound, where, assignments, variables, eval, maxEdges, cancel)
                    : edges(session, graphId, bound, where, assignments, variables, eval, maxEdges, cancel);
            if (mutations.isEmpty()) {
                return 0;
            }
            return catalog.apply(graphId, mutations, maxEdges);
        }
    }

    private List<Mutation> nodes(
            ReadSession session,
            int graphId,
            Bound bound,
            Expr where,
            List<Stmt.Assign> assignments,
            List<String> variables,
            ExprEval eval,
            int maxEdges,
            CancelToken cancel) {
        List<Long> starts = starts(session, graphId, bound, eval, maxEdges, cancel);
        Map<Long, VertexUpdate> vertices = new LinkedHashMap<>();
        LinkedHashSet<Long> deleted = new LinkedHashSet<>();
        for (long vid : starts) {
            cancelled(cancel);
            Map<String, Object> props = bound.sourceType == null
                    ? Map.of()
                    : vertexProps(session, graphId, bound.sourceType, vid);
            if (bound.sourceType != null && props == null) {
                continue;
            }
            eval.slots(Map.of());
            if (!same(eval, withoutId(bound.source.properties()), props)) {
                continue;
            }
            slots(eval, bound, vid, vid, null, props, Map.of(), Map.of());
            if (where != null && !eval.isTrue(where)) {
                continue;
            }
            if (assignments != null) {
                assign(eval, bound, assignments, vid, vid, null, vertices, null);
            } else if (bound.source.variable() != null && variables.contains(bound.source.variable())) {
                deleted.add(vid);
            }
        }
        return mutations(vertices, Map.of(), deleted, List.of(), bound);
    }

    private List<Mutation> edges(
            ReadSession session,
            int graphId,
            Bound bound,
            Expr where,
            List<Stmt.Assign> assignments,
            List<String> variables,
            ExprEval eval,
            int maxEdges,
            CancelToken cancel) {
        List<Long> starts = starts(session, graphId, bound, eval, maxEdges, cancel);
        Long targetId = id(bound.target, eval);
        Map<Long, VertexUpdate> vertices = new LinkedHashMap<>();
        Map<EdgeRef, EdgeUpdate> edges = new LinkedHashMap<>();
        LinkedHashSet<Long> deletedVertices = new LinkedHashSet<>();
        LinkedHashSet<EdgeRef> deletedEdges = new LinkedHashSet<>();
        LinkedHashSet<EdgeRef> seen = new LinkedHashSet<>();
        java.util.function.Predicate<EdgeRef> rank = bound.edge.rank() == null
                ? null
                : edge -> edge.rank() == bound.edge.rank();
        if (bound.relType != null) {
            for (Direction direction : directions(bound.edge.direction())) {
                for (long start : starts) {
                    cancelled(cancel);
                    try (EdgeCursor cursor = session.scanEdges(
                            graphId, start, bound.relType.id(), direction, -1, maxEdges, rank, cancel)) {
                        take(session, graphId, bound, where, assignments, variables, eval, cursor, start, targetId,
                                vertices, edges, deletedVertices, deletedEdges, seen);
                    }
                }
            }
        } else {
            Set<Integer> typeIds = bound.openTypes ? null : typeIds(bound.rels);
            for (long start : starts) {
                cancelled(cancel);
                try (EdgeCursor cursor = session.scanEdges(
                        graphId, start, typeIds, directions(bound.edge.direction()), -1, maxEdges, rank, cancel)) {
                    take(session, graphId, bound, where, assignments, variables, eval, cursor, start, targetId,
                            vertices, edges, deletedVertices, deletedEdges, seen);
                }
            }
        }
        return mutations(vertices, edges, deletedVertices, deletedEdges, bound);
    }

    private void take(
            ReadSession session,
            int graphId,
            Bound bound,
            Expr where,
            List<Stmt.Assign> assignments,
            List<String> variables,
            ExprEval eval,
            EdgeCursor cursor,
            long start,
            Long targetId,
            Map<Long, VertexUpdate> vertices,
            Map<EdgeRef, EdgeUpdate> edges,
            LinkedHashSet<Long> deletedVertices,
            LinkedHashSet<EdgeRef> deletedEdges,
            LinkedHashSet<EdgeRef> seen) {
        while (cursor.hasNext()) {
            EdgeRef edge = cursor.next();
            if (!seen.add(edge)) {
                continue;
            }
            long neighbor = edge.outSrc() == start ? edge.outDst() : edge.outSrc();
            if (targetId != null && neighbor != targetId) {
                continue;
            }
            Map<String, Object> edgeProps = edgeProps(session, graphId, typeOf(bound, edge), edge);
            Map<String, Object> sourceProps = bound.sourceType == null
                    ? Map.of()
                    : vertexProps(session, graphId, bound.sourceType, start);
            Map<String, Object> targetProps = bound.targetType == null
                    ? Map.of()
                    : vertexProps(session, graphId, bound.targetType, neighbor);
            if (bound.sourceType != null && sourceProps == null) {
                continue;
            }
            if (bound.targetType != null && targetProps == null && !withoutId(bound.target.properties()).isEmpty()) {
                continue;
            }
            eval.slots(Map.of());
            if (!same(eval, bound.edge.properties(), edgeProps)) {
                continue;
            }
            if (!same(eval, withoutId(bound.source.properties()), sourceProps)) {
                continue;
            }
            if (!same(eval, withoutId(bound.target.properties()), targetProps == null ? Map.of() : targetProps)) {
                continue;
            }
            slots(eval, bound, start, neighbor, edge, sourceProps, targetProps, edgeProps);
            if (where != null && !eval.isTrue(where)) {
                continue;
            }
            if (assignments != null) {
                assign(eval, bound, assignments, start, neighbor, edge, vertices, edges);
            } else {
                deleteNames(bound, variables, start, neighbor, edge, deletedVertices, deletedEdges);
            }
        }
    }

    private void assign(
            ExprEval eval,
            Bound bound,
            List<Stmt.Assign> assignments,
            long start,
            long neighbor,
            EdgeRef edge,
            Map<Long, VertexUpdate> vertices,
            Map<EdgeRef, EdgeUpdate> edges) {
        for (Stmt.Assign assignment : assignments) {
            Object value = stored(eval.eval(assignment.value()));
            if (bound.edge != null && assignment.variable().equals(bound.edge.variable())) {
                RelType type = typeOf(bound, edge);
                if (!has(type.properties(), assignment.field())) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + assignment.field());
                }
                EdgeUpdate update = edges.computeIfAbsent(edge, ignored -> new EdgeUpdate(type.name(), edge));
                put(update.values, assignment.field(), value);
            } else if (assignment.variable().equals(bound.source.variable())) {
                VertexUpdate update = vertices.computeIfAbsent(start, ignored -> new VertexUpdate(bound.sourceType.name(), start));
                put(update.values, assignment.field(), value);
            } else if (bound.target != null && assignment.variable().equals(bound.target.variable())) {
                VertexUpdate update = vertices.computeIfAbsent(
                        neighbor, ignored -> new VertexUpdate(bound.targetType.name(), neighbor));
                put(update.values, assignment.field(), value);
            } else {
                throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + assignment.variable());
            }
        }
    }

    private static void deleteNames(
            Bound bound,
            List<String> variables,
            long start,
            long neighbor,
            EdgeRef edge,
            LinkedHashSet<Long> vertices,
            LinkedHashSet<EdgeRef> edges) {
        for (String variable : variables) {
            if (bound.edge.variable() != null && variable.equals(bound.edge.variable())) {
                edges.add(edge);
            } else if (bound.source.variable() != null && variable.equals(bound.source.variable())) {
                vertices.add(start);
            } else if (bound.target.variable() != null && variable.equals(bound.target.variable())) {
                vertices.add(neighbor);
            } else {
                throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
            }
        }
    }

    private static List<Mutation> mutations(
            Map<Long, VertexUpdate> vertices,
            Map<EdgeRef, EdgeUpdate> edges,
            LinkedHashSet<Long> deletedVertices,
            java.util.Collection<EdgeRef> deletedEdges,
            Bound bound) {
        List<Mutation> mutations = new ArrayList<>();
        if (!deletedVertices.isEmpty() || !deletedEdges.isEmpty()) {
            if (!deletedVertices.isEmpty()) {
                for (long vid : deletedVertices) {
                    mutations.add(new Mutation.DeleteVertex(vid));
                }
            } else {
                for (EdgeRef edge : deletedEdges) {
                    mutations.add(new Mutation.DeleteEdge(
                            typeOf(bound, edge).name(), edge.rank(), edge.outSrc(), edge.outDst()));
                }
            }
            return mutations;
        }
        for (VertexUpdate update : vertices.values()) {
            mutations.add(new Mutation.SetVertex(update.typeName, update.vid, update.values));
        }
        for (EdgeUpdate update : edges.values()) {
            mutations.add(new Mutation.SetEdge(
                    update.typeName, update.edge.rank(), update.edge.outSrc(), update.edge.outDst(), update.values));
        }
        return mutations;
    }

    private List<Long> starts(
            ReadSession session, int graphId, Bound bound, ExprEval eval, int maxEdges, CancelToken cancel) {
        Long sourceId = id(bound.source, eval);
        if (sourceId != null) {
            if (bound.sourceType != null && vertexProps(session, graphId, bound.sourceType, sourceId) == null) {
                return List.of();
            }
            return List.of(sourceId);
        }
        if (bound.sourceType == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "起点需要 id 或节点类型");
        }
        cancelled(cancel);
        List<VertexId> found = session.scanVertices(graphId, bound.sourceType.id(), -1, maxEdges, cancel);
        List<Long> ids = new ArrayList<>(found.size());
        for (VertexId vertex : found) {
            ids.add(vertex.vid());
        }
        return ids;
    }

    private Bound bind(
            int graphId,
            Stmt.NodePat source,
            Stmt.EdgePat edge,
            Stmt.NodePat target,
            Expr where,
            List<Stmt.Assign> assignments,
            List<String> variables) {
        NodeType sourceType = source.typeName() == null ? null : catalog.nodeType(graphId, source.typeName());
        NodeType targetType = target == null || target.typeName() == null ? null : catalog.nodeType(graphId, target.typeName());
        List<RelType> rels = List.of();
        boolean openTypes = false;
        RelType relType = null;
        if (edge != null) {
            if (edge.typeNames().isEmpty()) {
                openTypes = true;
                rels = List.copyOf(catalog.relTypes(graphId));
            } else {
                List<RelType> named = new ArrayList<>();
                for (String typeName : edge.typeNames()) {
                    named.add(catalog.relType(graphId, typeName));
                }
                rels = List.copyOf(named);
                if (rels.size() == 1) {
                    relType = rels.get(0);
                }
            }
        }
        if (sourceType != null) {
            checkProps(source.properties(), sourceType.properties(), "起点");
        } else if (!withoutId(source.properties()).isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "起点属性需要节点类型");
        }
        if (edge != null) {
            if (!openTypes) {
                for (RelType type : rels) {
                    checkProps(edge.properties(), type.properties(), "边");
                }
            }
            if (targetType != null) {
                checkProps(target.properties(), targetType.properties(), "终点");
            } else if (!withoutId(target.properties()).isEmpty()) {
                throw new CengyanException(ErrorCode.SYNTAX, "不知道终点的节点类型");
            }
        }
        Bound bound = new Bound(source, edge, target, sourceType, targetType, relType, rels, openTypes);
        if (assignments != null) {
            for (Stmt.Assign assignment : assignments) {
                requireField(bound, assignment.variable(), assignment.field());
                checkExpr(assignment.value(), bound);
            }
        } else {
            for (String variable : variables) {
                if (!known(bound, variable)) {
                    throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
                }
            }
        }
        if (where != null) {
            checkExpr(where, bound);
        }
        return bound;
    }

    private static void requireField(Bound bound, String variable, String field) {
        if (bound.edge != null && variable.equals(bound.edge.variable())) {
            if ("rank".equals(field) || !edgeField(bound, field)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + field);
            }
            return;
        }
        if (variable.equals(bound.source.variable())) {
            requireNodeField(field, bound.sourceType);
            return;
        }
        if (bound.target != null && variable.equals(bound.target.variable())) {
            requireNodeField(field, bound.targetType);
            return;
        }
        throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
    }

    private static void requireNodeField(String field, NodeType type) {
        if ("id".equals(field) || type == null || !has(type.properties(), field)) {
            if ("id".equals(field)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "id 不是普通属性");
            }
            if (type == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "不知道 " + field + " 属于哪个节点类型");
            }
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + field);
        }
    }

    private static boolean known(Bound bound, String variable) {
        return variable.equals(bound.source.variable())
                || (bound.edge != null && variable.equals(bound.edge.variable()))
                || (bound.target != null && variable.equals(bound.target.variable()));
    }

    private static void checkExpr(Expr expr, Bound bound) {
        switch (expr) {
            case Expr.Literal ignored -> {
            }
            case Expr.Param ignored -> {
            }
            case Expr.Variable variable -> {
                if (!known(bound, variable.name())) {
                    throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable.name());
                }
            }
            case Expr.Prop prop -> checkProperty(bound, prop.variable(), prop.field());
            case Expr.Call call -> {
                String name = call.name().toLowerCase(Locale.ROOT);
                if ("count".equals(name) || "sum".equals(name)) {
                    throw new CengyanException(ErrorCode.SYNTAX, "聚合只能写在 RETURN");
                }
                switch (name) {
                    case "id", "labels", "type", "properties", "startnode", "endnode", "coalesce", "size", "length" -> {
                    }
                    default -> throw new CengyanException(ErrorCode.SYNTAX, "未知函数 " + call.name());
                }
                for (Expr arg : call.args()) {
                    checkExpr(arg, bound);
                }
            }
            case Expr.Unary unary -> checkExpr(unary.inner(), bound);
            case Expr.Binary binary -> {
                checkExpr(binary.left(), bound);
                checkExpr(binary.right(), bound);
            }
            case Expr.In in -> {
                checkExpr(in.value(), bound);
                for (Expr option : in.options()) {
                    checkExpr(option, bound);
                }
            }
            case Expr.IsNull isNull -> checkExpr(isNull.value(), bound);
        }
    }

    private static void checkProperty(Bound bound, String variable, String field) {
        if (bound.edge != null && variable.equals(bound.edge.variable())) {
            if (!"rank".equals(field) && !edgeField(bound, field)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + field);
            }
            return;
        }
        if (variable.equals(bound.source.variable())) {
            if (!"id".equals(field)) {
                requireNodeField(field, bound.sourceType);
            }
            return;
        }
        if (bound.target != null && variable.equals(bound.target.variable())) {
            if (!"id".equals(field)) {
                requireNodeField(field, bound.targetType);
            }
            return;
        }
        throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
    }

    private static void slots(
            ExprEval eval,
            Bound bound,
            long start,
            long neighbor,
            EdgeRef edge,
            Map<String, Object> sourceProps,
            Map<String, Object> targetProps,
            Map<String, Object> edgeProps) {
        Map<String, ExprEval.Slot> slots = new LinkedHashMap<>();
        if (bound.source.variable() != null) {
            slots.put(bound.source.variable(), ExprEval.Slot.node(
                    start, bound.sourceType == null ? null : bound.sourceType.name(), sourceProps));
        }
        if (bound.target != null && bound.target.variable() != null) {
            slots.put(bound.target.variable(), ExprEval.Slot.node(
                    neighbor, bound.targetType == null ? null : bound.targetType.name(), targetProps));
        }
        if (bound.edge != null && bound.edge.variable() != null && edge != null) {
            slots.put(bound.edge.variable(), ExprEval.Slot.edge(
                    typeOf(bound, edge).name(), edge.rank(), edge.outSrc(), edge.outDst(), edgeProps));
        }
        eval.slots(slots);
    }

    private Map<String, Object> vertexProps(ReadSession session, int graphId, NodeType type, long vid) {
        byte[] raw = session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, catalog.partOf(graphId, vid), vid, type.id()));
        if (raw == null) {
            return null;
        }
        return typed(type.properties(), PropCodec.decode(type.properties(), type.schemaVersion(), raw));
    }

    private Map<String, Object> edgeProps(ReadSession session, int graphId, RelType type, EdgeRef edge) {
        byte[] raw = session.get(Cf.PROP, KeyCodec.propKey(graphId, type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        if (raw == null) {
            return Map.of();
        }
        return typed(type.properties(), PropCodec.decode(type.properties(), type.schemaVersion(), raw));
    }

    private static Map<String, Object> typed(List<PropDef> definitions, Map<String, Object> raw) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (PropDef definition : definitions) {
            Object value = raw.get(definition.name());
            if (value instanceof Long millis && definition.type() == ValueType.DATETIME) {
                values.put(definition.name(), new UtcTime(millis));
            } else {
                values.put(definition.name(), value);
            }
        }
        return values;
    }

    private static boolean same(ExprEval eval, List<Stmt.Prop> expected, Map<String, Object> actual) {
        if (expected.isEmpty()) {
            return true;
        }
        for (Stmt.Prop prop : expected) {
            Object found = actual == null ? null : actual.get(prop.name());
            if (!eval.sameValue(eval.eval(prop.value()), found)) {
                return false;
            }
        }
        return true;
    }

    private static Long id(Stmt.NodePat node, ExprEval eval) {
        if (node == null) {
            return null;
        }
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
        return null;
    }

    private static List<Stmt.Prop> withoutId(List<Stmt.Prop> props) {
        List<Stmt.Prop> rest = new ArrayList<>();
        for (Stmt.Prop prop : props) {
            if (!"id".equals(prop.name())) {
                rest.add(prop);
            }
        }
        return rest;
    }

    private static void checkProps(List<Stmt.Prop> props, List<PropDef> schema, String where) {
        for (Stmt.Prop prop : props) {
            if ("id".equals(prop.name())) {
                continue;
            }
            if (!has(schema, prop.name())) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, where + "没有属性 " + prop.name());
            }
        }
    }

    private static boolean has(List<PropDef> properties, String name) {
        for (PropDef property : properties) {
            if (property.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void put(Map<String, Object> values, String field, Object value) {
        if (values.containsKey(field) && !Objects.equals(values.get(field), value)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "同一个点写出了两份不同的属性");
        }
        values.put(field, value);
    }

    private static Object stored(Object value) {
        if (value instanceof UtcTime time) {
            return time.millis();
        }
        if (value == null || value instanceof Long || value instanceof Double
                || value instanceof String || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Integer number) {
            return number.longValue();
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这个值不能写入属性");
    }

    private static List<Direction> directions(Stmt.EdgePat.Direction direction) {
        return switch (direction) {
            case OUT -> List.of(Direction.OUT);
            case IN -> List.of(Direction.IN);
            case ANY -> List.of(Direction.OUT, Direction.IN);
        };
    }

    private static void rejectQuantifier(Stmt.EdgePat edge) {
        if (edge != null && edge.minHop() != null) {
            throw new CengyanException(ErrorCode.SYNTAX, "修改和删除只支持一跳");
        }
    }

    private static void cancelled(CancelToken cancel) {
        if (cancel != null && cancel.cancelled()) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
    }

    private record Bound(
            Stmt.NodePat source,
            Stmt.EdgePat edge,
            Stmt.NodePat target,
            NodeType sourceType,
            NodeType targetType,
            RelType relType,
            List<RelType> rels,
            boolean openTypes) {
    }

    private static boolean edgeField(Bound bound, String field) {
        if (bound.openTypes) {
            return true;
        }
        if (bound.rels.isEmpty()) {
            return false;
        }
        for (RelType type : bound.rels) {
            if (!has(type.properties(), field)) {
                return false;
            }
        }
        return true;
    }

    private static RelType typeOf(Bound bound, EdgeRef edge) {
        if (bound.relType != null && bound.relType.id() == edge.edgeTypeId()) {
            return bound.relType;
        }
        for (RelType type : bound.rels) {
            if (type.id() == edge.edgeTypeId()) {
                return type;
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
    }

    private static Set<Integer> typeIds(List<RelType> rels) {
        LinkedHashSet<Integer> ids = new LinkedHashSet<>();
        for (RelType type : rels) {
            ids.add(type.id());
        }
        return ids;
    }

    private static final class VertexUpdate {
        private final String typeName;
        private final long vid;
        private final Map<String, Object> values = new LinkedHashMap<>();

        private VertexUpdate(String typeName, long vid) {
            this.typeName = typeName;
            this.vid = vid;
        }
    }

    private static final class EdgeUpdate {
        private final String typeName;
        private final EdgeRef edge;
        private final Map<String, Object> values = new LinkedHashMap<>();

        private EdgeUpdate(String typeName, EdgeRef edge) {
            this.typeName = typeName;
            this.edge = edge;
        }
    }
}
