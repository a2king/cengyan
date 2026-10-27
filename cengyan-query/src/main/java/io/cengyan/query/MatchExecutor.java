package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;

/**
 * 一跳 MATCH。只涉及 rank 和终点 id 的条件在扫描时判断。
 * 属性在收满 LIMIT 之后一次取出。排序作用在这些已经收住的行上。
 */
final class MatchExecutor {
    private final Catalog catalog;
    private final GraphStore store;

    MatchExecutor(Catalog catalog, GraphStore store) {
        this.catalog = catalog;
        this.store = store;
    }

    QueryResult run(Stmt.Match match, int graphId, Map<String, Object> params, int maxEdges, CancelToken cancel) {
        Names names = names(match);
        RelType relType = catalog.relType(graphId, match.edge().typeName());
        NodeType sourceType = match.source().typeName() == null ? null : catalog.nodeType(graphId, match.source().typeName());
        NodeType targetType = match.target().typeName() == null ? null : catalog.nodeType(graphId, match.target().typeName());
        Needs needs = new Needs(match, names, sourceType, targetType, relType, sorts(match, names));
        ExprEval eval = new ExprEval(params, false);
        Long startId = constantId(match.source().properties(), eval);
        int window = window(match);
        Push push = push(match.where(), names, eval);
        LongPredicate rankFilter = push.rank;
        if (match.edge().rank() != null) {
            long exact = match.edge().rank();
            rankFilter = and(rankFilter, value -> value == exact);
        }
        LongPredicate rankOnly = rankFilter;
        LongPredicate neighborOnly = push.neighbor;
        List<Hop> hops = new ArrayList<>();
        try (ReadSession session = store.read()) {
            List<Long> starts = filterStarts(
                session, graphId, sourceType, eval, match.source(),
                starts(session, graphId, match.source(), sourceType, startId, maxEdges, cancel));
            for (Direction direction : directions(match.edge().direction())) {
                for (long start : starts) {
                    if (window >= 0 && hops.size() >= window) {
                        break;
                    }
                    cancelled(cancel);
                    int remaining = window < 0 ? -1 : window - hops.size();
                    try (EdgeCursor cursor = session.scanEdges(
                            graphId, start, relType.id(), direction, remaining, maxEdges,
                            edge -> rankOk(rankOnly, edge.rank()) && neighborOk(neighborOnly, other(edge, direction)),
                            cancel)) {
                        while (cursor.hasNext()) {
                            EdgeRef edge = cursor.next();
                            if (match.edge().direction() == Stmt.EdgePat.Direction.ANY && !names.seen.add(edge)) {
                                continue;
                            }
                            long other = other(edge, direction);
                            hops.add(new Hop(start, other, edge));
                        }
                    }
                }
            }
            List<Map<String, Object>> edgeProps = needs.edge ? edgeProps(session, graphId, relType, hops) : null;
            Map<Long, Map<String, Object>> sourceProps = needs.source
                    ? vertexProps(session, graphId, sourceType, hops, true) : Map.of();
            Map<Long, Map<String, Object>> targetProps = needs.target
                    ? vertexProps(session, graphId, targetType, hops, false) : Map.of();
            List<Hop> matched = new ArrayList<>();
            List<Map<String, Object>> matchedEdgeProps = needs.edge ? new ArrayList<>() : null;
            for (int i = 0; i < hops.size(); i++) {
                Hop hop = hops.get(i);
                if (!patternProps(eval, match, names, hop, edgeProps == null ? null : edgeProps.get(i),
                        sourceProps.get(hop.start()), targetProps.get(hop.neighbor()))) {
                    continue;
                }
                if (push.residual != null) {
                    eval.slots(slots(names, match, hop, sourceType, targetType,
                            edgeProps == null ? null : edgeProps.get(i),
                            sourceProps.get(hop.start()), targetProps.get(hop.neighbor())));
                    if (!eval.isTrue(push.residual)) {
                        continue;
                    }
                }
                matched.add(hop);
                if (matchedEdgeProps != null) {
                    matchedEdgeProps.add(edgeProps.get(i));
                }
            }
            List<String> columns = new ArrayList<>();
            for (Stmt.Item item : match.items()) {
                columns.add(item.alias() == null ? item.text() : item.alias());
            }
            List<List<Object>> rows = new ArrayList<>();
            List<List<Object>> sortKeys = new ArrayList<>();
            for (int i = 0; i < matched.size(); i++) {
                Hop hop = matched.get(i);
                eval.slots(slots(names, match, hop, sourceType, targetType,
                        matchedEdgeProps == null ? null : matchedEdgeProps.get(i),
                        sourceProps.get(hop.start()), targetProps.get(hop.neighbor())));
                List<Object> row = new ArrayList<>();
                for (Stmt.Item item : match.items()) {
                    row.add(DateTimes.plain(eval.eval(item.expr())));
                }
                rows.add(row);
                if (!match.orderBy().isEmpty()) {
                    List<Stmt.Sort> sorts = sorts(match, names);
                    List<Object> keys = new ArrayList<>();
                    for (Stmt.Sort sort : sorts) {
                        keys.add(DateTimes.plain(eval.eval(sort.expr())));
                    }
                    sortKeys.add(keys);
                }
            }
            order(rows, sortKeys, sorts(match, names));
            int skip = match.skip() == null ? 0 : Math.toIntExact(match.skip());
            if (skip > 0) {
                if (skip >= rows.size()) {
                    rows = List.of();
                } else {
                    rows = rows.subList(skip, rows.size());
                }
            }
            return new QueryResult(columns, rows, 0);
        }
    }

    private static List<Long> starts(
            ReadSession session,
            int graphId,
            Stmt.NodePat source,
            NodeType sourceType,
            Long startId,
            int maxEdges,
            CancelToken cancel) {
        if (startId != null) {
            if (sourceType != null) {
                byte[] raw = session.get(
                        Cf.VERTEX, KeyCodec.vertexKey(graphId, KeyCodec.partOf(startId), startId, sourceType.id()));
                if (raw == null) {
                    return List.of();
                }
            }
            return List.of(startId);
        }
        if (sourceType == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "起点需要 id 或节点类型");
        }
        cancelled(cancel);
        List<VertexId> found = session.scanVertices(graphId, sourceType.id(), -1, maxEdges, cancel);
        List<Long> ids = new ArrayList<>(found.size());
        for (VertexId vertex : found) {
            ids.add(vertex.vid());
        }
        return ids;
    }

    private List<Map<String, Object>> edgeProps(ReadSession session, int graphId, RelType type, List<Hop> hops) {
        List<byte[]> keys = new ArrayList<>(hops.size());
        for (Hop hop : hops) {
            EdgeRef edge = hop.edge();
            keys.add(KeyCodec.propKey(graphId, type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        List<byte[]> values = session.multiGet(Cf.PROP, keys);
        List<Map<String, Object>> props = new ArrayList<>(values.size());
        for (byte[] value : values) {
            props.add(value == null ? Map.of() : typed(type.properties(), PropCodec.decode(type.properties(), type.schemaVersion(), value)));
        }
        return props;
    }

    private Map<Long, Map<String, Object>> vertexProps(
            ReadSession session, int graphId, NodeType type, List<Hop> hops, boolean start) {
        if (type == null) {
            return Map.of();
        }
        Map<Long, Map<String, Object>> loaded = new HashMap<>();
        for (Hop hop : hops) {
            long vid = start ? hop.start() : hop.neighbor();
            if (loaded.containsKey(vid)) {
                continue;
            }
            byte[] raw = session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, KeyCodec.partOf(vid), vid, type.id()));
            loaded.put(vid, raw == null ? Map.of()
                    : typed(type.properties(), PropCodec.decode(type.properties(), type.schemaVersion(), raw)));
        }
        return loaded;
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

    private boolean patternProps(
            ExprEval eval,
            Stmt.Match match,
            Names names,
            Hop hop,
            Map<String, Object> edgeProps,
            Map<String, Object> sourceProps,
            Map<String, Object> targetProps) {
        eval.slots(Map.of());
        if (!same(eval, match.edge().properties(), edgeProps)) {
            return false;
        }
        if (sourceProps != null && !same(eval, withoutId(match.source().properties()), sourceProps)) {
            return false;
        }
        return targetProps == null || same(eval, withoutId(match.target().properties()), targetProps);
    }

    private static boolean same(ExprEval eval, List<Stmt.Prop> expected, Map<String, Object> actual) {
        for (Stmt.Prop prop : expected) {
            Object wanted = eval.eval(prop.value());
            Object found = actual == null ? null : actual.get(prop.name());
            if (!eval.sameValue(wanted, found)) {
                return false;
            }
        }
        return true;
    }

    private List<Long> filterStarts(
            ReadSession session,
            int graphId,
            NodeType sourceType,
            ExprEval eval,
            Stmt.NodePat source,
            List<Long> starts) {
        List<Stmt.Prop> expected = withoutId(source.properties());
        if (expected.isEmpty()) {
            return starts;
        }
        if (sourceType == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "起点属性需要节点类型");
        }
        List<Long> kept = new ArrayList<>();
        for (long vid : starts) {
            byte[] raw = session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, KeyCodec.partOf(vid), vid, sourceType.id()));
            Map<String, Object> props = raw == null
                    ? Map.of()
                    : typed(sourceType.properties(), PropCodec.decode(sourceType.properties(), sourceType.schemaVersion(), raw));
            if (same(eval, expected, props)) {
                kept.add(vid);
            }
        }
        return kept;
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

    private static Map<String, ExprEval.Slot> slots(
            Names names,
            Stmt.Match match,
            Hop hop,
            NodeType sourceType,
            NodeType targetType,
            Map<String, Object> edgeProps,
            Map<String, Object> sourceProps,
            Map<String, Object> targetProps) {
        Map<String, ExprEval.Slot> slots = new HashMap<>();
        if (names.source != null) {
            slots.put(names.source, ExprEval.Slot.node(hop.start(), typeName(sourceType), sourceProps));
        }
        if (names.target != null) {
            slots.put(names.target, ExprEval.Slot.node(hop.neighbor(), typeName(targetType), targetProps));
        }
        if (names.edge != null) {
            slots.put(names.edge, ExprEval.Slot.edge(
                    match.edge().typeName(), hop.edge().rank(), hop.edge().outSrc(), hop.edge().outDst(), edgeProps));
        }
        return slots;
    }

    private static String typeName(NodeType type) {
        return type == null ? null : type.name();
    }

    private static void order(List<List<Object>> rows, List<List<Object>> keys, List<Stmt.Sort> sorts) {
        if (sorts.isEmpty() || rows.size() < 2) {
            return;
        }
        Integer[] index = new Integer[rows.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        java.util.Arrays.sort(index, (left, right) -> compareRow(keys.get(left), keys.get(right), sorts));
        List<List<Object>> ordered = new ArrayList<>(rows.size());
        for (int position : index) {
            ordered.add(rows.get(position));
        }
        rows.clear();
        rows.addAll(ordered);
    }

    private static int compareRow(List<Object> left, List<Object> right, List<Stmt.Sort> sorts) {
        for (int i = 0; i < sorts.size(); i++) {
            int compared = compareValue(left.get(i), right.get(i));
            if (compared != 0) {
                return sorts.get(i).descending() ? -compared : compared;
            }
        }
        return 0;
    }

    private static int compareValue(Object left, Object right) {
        if (left == null || right == null) {
            if (left == right) {
                return 0;
            }
            return left == null ? 1 : -1;
        }
        if (left instanceof Number && right instanceof Number) {
            if (left instanceof Double || right instanceof Double) {
                return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
            }
            return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
        }
        if (left instanceof String text && right instanceof String other) {
            return java.util.Arrays.compareUnsigned(
                    text.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    other.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        if (left instanceof Boolean && right instanceof Boolean) {
            return Boolean.compare((Boolean) left, (Boolean) right);
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这两边的类型不能排序");
    }

    private static List<Stmt.Sort> sorts(Stmt.Match match, Names names) {
        if (match.orderBy().isEmpty()) {
            return List.of();
        }
        List<Stmt.Sort> sorts = new ArrayList<>();
        for (Stmt.Sort sort : match.orderBy()) {
            sorts.add(new Stmt.Sort(sortExpr(sort.expr(), match.items(), names), sort.descending()));
        }
        return sorts;
    }

    private static Expr sortExpr(Expr expr, List<Stmt.Item> items, Names names) {
        if (!(expr instanceof Expr.Variable variable)) {
            return expr;
        }
        if (variable.name().equals(names.source) || variable.name().equals(names.edge) || variable.name().equals(names.target)) {
            return expr;
        }
        for (Stmt.Item item : items) {
            if (variable.name().equals(item.alias())) {
                return item.expr();
            }
        }
        return expr;
    }

    private static int window(Stmt.Match match) {
        if (match.limit() == null && match.skip() == null) {
            return -1;
        }
        long limit = match.limit() == null ? Integer.MAX_VALUE : match.limit();
        long skip = match.skip() == null ? 0L : match.skip();
        if (limit < 0 || skip < 0 || limit > Integer.MAX_VALUE || skip > Integer.MAX_VALUE) {
            throw new CengyanException(ErrorCode.SYNTAX, "LIMIT 或 SKIP 超出范围");
        }
        long total;
        try {
            total = Math.addExact(limit, skip);
        } catch (ArithmeticException ex) {
            throw new CengyanException(ErrorCode.SYNTAX, "LIMIT 或 SKIP 超出范围");
        }
        if (total > Integer.MAX_VALUE) {
            throw new CengyanException(ErrorCode.SYNTAX, "LIMIT 或 SKIP 超出范围");
        }
        return (int) total;
    }

    private static Long constantId(List<Stmt.Prop> props, ExprEval eval) {
        for (Stmt.Prop prop : props) {
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

    private static Push push(Expr where, Names names, ExprEval eval) {
        if (where == null) {
            return new Push(null, null, null);
        }
        List<Expr> conjuncts = new ArrayList<>();
        collectAnd(where, conjuncts);
        LongPredicate neighbor = null;
        LongPredicate rank = null;
        List<Expr> residual = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            LongPredicate pushed = pushOne(conjunct, names, eval);
            if (pushed == null) {
                residual.add(conjunct);
                continue;
            }
            if (pushedKind(conjunct, names) == Kind.RANK) {
                rank = and(rank, pushed);
            } else {
                neighbor = and(neighbor, pushed);
            }
        }
        Expr rest = null;
        for (Expr expr : residual) {
            rest = rest == null ? expr : new Expr.Binary("AND", rest, expr);
        }
        return new Push(neighbor, rank, rest);
    }

    private static void collectAnd(Expr expr, List<Expr> conjuncts) {
        if (expr instanceof Expr.Binary binary && "AND".equals(binary.op())) {
            collectAnd(binary.left(), conjuncts);
            collectAnd(binary.right(), conjuncts);
            return;
        }
        conjuncts.add(expr);
    }

    private static LongPredicate pushOne(Expr expr, Names names, ExprEval eval) {
        if (!(expr instanceof Expr.Binary binary) || !isComparison(binary.op())) {
            return null;
        }
        Ref left = ref(binary.left(), names);
        Ref right = ref(binary.right(), names);
        if (left != null && right == null && constant(binary.right(), eval) instanceof Long number) {
            return compareLong(binary.op(), number);
        }
        if (right != null && left == null && constant(binary.left(), eval) instanceof Long number) {
            return compareLong(flip(binary.op()), number);
        }
        return null;
    }

    private static Kind pushedKind(Expr expr, Names names) {
        Expr.Binary binary = (Expr.Binary) expr;
        Ref left = ref(binary.left(), names);
        if (left != null) {
            return left.kind;
        }
        return ref(binary.right(), names).kind;
    }

    private static Ref ref(Expr expr, Names names) {
        if (!(expr instanceof Expr.Prop prop)) {
            return null;
        }
        if (names.target != null && names.target.equals(prop.variable()) && "id".equals(prop.field())) {
            return new Ref(Kind.NEIGHBOR);
        }
        if (names.edge != null && names.edge.equals(prop.variable()) && "rank".equals(prop.field())) {
            return new Ref(Kind.RANK);
        }
        return null;
    }

    private static Object constant(Expr expr, ExprEval eval) {
        if (expr instanceof Expr.Literal || expr instanceof Expr.Param) {
            return eval.eval(expr);
        }
        return null;
    }

    private static boolean isComparison(String op) {
        return "=".equals(op) || "<>".equals(op) || "<".equals(op) || "<=".equals(op) || ">".equals(op) || ">=".equals(op);
    }

    private static String flip(String op) {
        return switch (op) {
            case "<" -> ">";
            case "<=" -> ">=";
            case ">" -> "<";
            case ">=" -> "<=";
            default -> op;
        };
    }

    private static LongPredicate compareLong(String op, long literal) {
        return switch (op) {
            case "=" -> value -> value == literal;
            case "<>" -> value -> value != literal;
            case "<" -> value -> value < literal;
            case "<=" -> value -> value <= literal;
            case ">" -> value -> value > literal;
            case ">=" -> value -> value >= literal;
            default -> value -> false;
        };
    }

    private static LongPredicate and(LongPredicate left, LongPredicate right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return value -> left.test(value) && right.test(value);
    }

    private static boolean rankOk(LongPredicate rank, long value) {
        return rank == null || rank.test(value);
    }

    private static boolean neighborOk(LongPredicate neighbor, long value) {
        return neighbor == null || neighbor.test(value);
    }

    private static long other(EdgeRef edge, Direction direction) {
        return direction == Direction.OUT ? edge.outDst() : edge.outSrc();
    }

    private static List<Direction> directions(Stmt.EdgePat.Direction direction) {
        return switch (direction) {
            case OUT -> List.of(Direction.OUT);
            case IN -> List.of(Direction.IN);
            case ANY -> List.of(Direction.OUT, Direction.IN);
        };
    }

    private static void cancelled(CancelToken cancel) {
        if (cancel != null && cancel.cancelled()) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
    }

    private static Names names(Stmt.Match match) {
        Names names = new Names();
        names.source = unique(names, match.source().variable());
        names.edge = unique(names, match.edge().variable());
        names.target = unique(names, match.target().variable());
        return names;
    }

    private static String unique(Names names, String variable) {
        if (variable == null) {
            return null;
        }
        if (!names.declared.add(variable)) {
            throw new CengyanException(ErrorCode.SYNTAX, "变量重复：" + variable);
        }
        return variable;
    }

    private static final class Names {
        private String source;
        private String edge;
        private String target;
        private final java.util.Set<String> declared = new java.util.HashSet<>();
        private final java.util.Set<EdgeRef> seen = new LinkedHashSet<>();
    }

    private record Hop(long start, long neighbor, EdgeRef edge) {
    }

    private record Push(LongPredicate neighbor, LongPredicate rank, Expr residual) {
    }

    private record Ref(Kind kind) {
    }

    private enum Kind {
        NEIGHBOR, RANK
    }

    private static final class Needs {
        private final boolean edge;
        private final boolean source;
        private final boolean target;

        private Needs(
                Stmt.Match match,
                Names names,
                NodeType sourceType,
                NodeType targetType,
                RelType relType,
                List<Stmt.Sort> orderBy) {
            checkProps(match.edge().properties(), relType.properties(), false, "边");
            if (!withoutId(match.source().properties()).isEmpty() && sourceType == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "起点属性需要节点类型");
            }
            if (sourceType != null) {
                checkProps(match.source().properties(), sourceType.properties(), true, "起点");
            }
            if (!withoutId(match.target().properties()).isEmpty() && targetType == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "不知道终点的节点类型");
            }
            if (targetType != null) {
                checkProps(match.target().properties(), targetType.properties(), true, "终点");
            }
            boolean edge = !match.edge().properties().isEmpty();
            boolean source = !withoutId(match.source().properties()).isEmpty();
            boolean target = !withoutId(match.target().properties()).isEmpty();
            Walker walker = new Walker(names, sourceType, targetType, relType);
            if (match.where() != null) {
                walker.walk(match.where());
            }
            for (Stmt.Item item : match.items()) {
                walker.walk(item.expr());
            }
            for (Stmt.Sort sort : orderBy) {
                walker.walk(sort.expr());
            }
            this.edge = edge || walker.edge;
            this.source = source || walker.source;
            this.target = target || walker.target;
        }

        private static void checkProps(List<Stmt.Prop> props, List<PropDef> schema, boolean allowId, String where) {
            for (Stmt.Prop prop : props) {
                if (allowId && "id".equals(prop.name())) {
                    continue;
                }
                if (!Walker.has(schema, prop.name())) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, where + "没有属性 " + prop.name());
                }
            }
        }
    }

    private static final class Walker {
        private final Names names;
        private final NodeType sourceType;
        private final NodeType targetType;
        private final RelType relType;
        private boolean edge;
        private boolean source;
        private boolean target;

        private Walker(Names names, NodeType sourceType, NodeType targetType, RelType relType) {
            this.names = names;
            this.sourceType = sourceType;
            this.targetType = targetType;
            this.relType = relType;
        }

        private void walk(Expr expr) {
            switch (expr) {
                case Expr.Literal ignored -> {
                }
                case Expr.Param ignored -> {
                }
                case Expr.Variable variable -> variable(variable.name(), true);
                case Expr.Prop prop -> property(prop);
                case Expr.Call call -> {
                    checkFunction(call.name());
                    for (Expr arg : call.args()) {
                        walk(arg);
                    }
                }
                case Expr.Unary unary -> walk(unary.inner());
                case Expr.Binary binary -> {
                    walk(binary.left());
                    walk(binary.right());
                }
                case Expr.In in -> {
                    walk(in.value());
                    for (Expr option : in.options()) {
                        walk(option);
                    }
                }
                case Expr.IsNull isNull -> walk(isNull.value());
            }
        }

        private void property(Expr.Prop prop) {
            if (prop.variable().equals(names.edge)) {
                if (!"rank".equals(prop.field()) && !has(relType.properties(), prop.field())) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + prop.field());
                }
                if (!"rank".equals(prop.field())) {
                    edge = true;
                }
                return;
            }
            if (prop.variable().equals(names.source)) {
                nodeProperty(prop.field(), sourceType, true);
                return;
            }
            if (prop.variable().equals(names.target)) {
                nodeProperty(prop.field(), targetType, false);
                return;
            }
            throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + prop.variable());
        }

        private void nodeProperty(String field, NodeType type, boolean start) {
            if ("id".equals(field)) {
                return;
            }
            if (type == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "不知道 " + field + " 属于哪个节点类型");
            }
            if (!has(type.properties(), field)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + field);
            }
            if (start) {
                source = true;
            } else {
                target = true;
            }
        }

        private void variable(String name, boolean whole) {
            if (name.equals(names.edge)) {
                if (whole) {
                    edge = true;
                }
                return;
            }
            if (name.equals(names.source)) {
                if (whole && sourceType != null) {
                    source = true;
                }
                return;
            }
            if (name.equals(names.target)) {
                if (whole && targetType != null) {
                    target = true;
                }
                return;
            }
            throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + name);
        }

        private static boolean has(List<PropDef> properties, String name) {
            for (PropDef property : properties) {
                if (property.name().equals(name)) {
                    return true;
                }
            }
            return false;
        }

        private static void checkFunction(String name) {
            switch (name.toLowerCase(java.util.Locale.ROOT)) {
                case "id", "labels", "type", "properties", "startnode", "endnode", "coalesce", "size", "length" -> {
                }
                default -> throw new CengyanException(ErrorCode.SYNTAX, "未知函数 " + name);
            }
        }
    }
}
