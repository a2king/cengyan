package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.IndexDef;
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
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * 一跳 MATCH。只涉及 rank 和终点 id 的条件在扫描时判断。
 * 属性在收满 LIMIT 之后一次取出。排序、去重和聚合交给 DuckDB，作用在这些已经收住的行上。
 */
final class MatchExecutor {
    private final Catalog catalog;
    private final GraphStore store;
    private final DuckSettings duck;

    MatchExecutor(Catalog catalog, GraphStore store, DuckSettings duck) {
        this.catalog = catalog;
        this.store = store;
        this.duck = duck;
    }

    QueryResult run(Stmt.Match match, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        String[] aggregates = aggregateKinds(match);
        int maxEdges = limits.maxEdgesPerVertex();
        Names names = names(match);
        if (match.edge() == null) {
            return nodes(match, graphId, params, limits, cancel, aggregates, names);
        }
        boolean openTypes = match.edge().typeNames().isEmpty();
        List<RelType> rels = openTypes ? catalog.relTypes(graphId) : namedRels(graphId, match.edge().typeNames());
        RelType relType = !openTypes && rels.size() == 1 ? rels.get(0) : null;
        NodeType sourceType = match.source().typeName() == null ? null : catalog.nodeType(graphId, match.source().typeName());
        NodeType targetType = match.target().typeName() == null ? null : catalog.nodeType(graphId, match.target().typeName());
        Needs needs = new Needs(match, names, sourceType, targetType, openTypes ? null : rels, sorts(match, names));
        ExprEval eval = new ExprEval(params, false);
        Long startId = constantId(graphId, sourceType, match.source().properties(), eval);
        int window = window(match);
        Push push = push(match.where(), names, eval);
        LongPredicate rankFilter = push.rank;
        if (match.edge().rank() != null) {
            long exact = match.edge().rank();
            rankFilter = and(rankFilter, value -> value == exact);
        }
        LongPredicate rankOnly = rankFilter;
        LongPredicate neighborOnly = push.neighbor;
        Long targetId = constantId(graphId, targetType, match.target().properties(), eval);
        if (targetId != null) {
            neighborOnly = and(neighborOnly, value -> value == targetId);
        }
        LongPredicate neighborFilter = neighborOnly;
        if (match.edge().minHop() != null) {
            return multi(match, graphId, limits, cancel, names, relType, openTypes, rels, sourceType, targetType, needs, eval,
                    rankOnly, neighborFilter, aggregates);
        }
        Map<Integer, String> typeNames = new HashMap<>();
        if (relType == null) {
            for (RelType type : rels) {
                typeNames.put(type.id(), type.name());
            }
        }
        List<Hop> hops = new ArrayList<>();
        try (ReadSession session = store.read()) {
            List<IndexDef> indexDefs = catalog.indexes(graphId);
            List<EdgeRef> indexed = names.edge == null || relType == null
                    ? null
                    : IndexLookup.edges(
                            indexDefs, session, graphId, relType.id(), relType.properties(),
                            names.edge, match.where(), eval, cancel);
            if (indexed != null) {
                boolean restrict = false;
                List<Long> allowed = null;
                if (startId != null) {
                    allowed = List.of(startId);
                    restrict = true;
                } else if (sourceType != null && names.source != null) {
                    List<Long> fromVertex = IndexLookup.vertexIds(
                            indexDefs, session, graphId, sourceType.id(), sourceType.properties(),
                            names.source, match.where(), eval, cancel);
                    if (fromVertex != null) {
                        allowed = fromVertex;
                        restrict = true;
                    }
                }
                if (restrict) {
                    allowed = filterStarts(session, graphId, sourceType, eval, match.source(), allowed);
                }
                hops.addAll(hopsFromIndex(
                        indexed, match.edge().direction(), allowed, restrict, rankOnly, neighborFilter, window, cancel));
            } else {
                List<Long> indexedStarts = startId == null && sourceType != null && names.source != null
                        ? IndexLookup.vertexIds(
                                indexDefs, session, graphId, sourceType.id(), sourceType.properties(),
                                names.source, match.where(), eval, cancel)
                        : null;
                List<Long> starts = filterStarts(
                        session, graphId, sourceType, eval, match.source(),
                        indexedStarts != null
                                ? indexedStarts
                                : starts(session, graphId, match.source(), sourceType, startId, maxEdges, cancel));
                if (relType != null) {
                for (Direction direction : directions(match.edge().direction())) {
                    for (long start : starts) {
                        if (window >= 0 && hops.size() >= window) {
                            break;
                        }
                        cancelled(cancel);
                        int remaining = window < 0 ? -1 : window - hops.size();
                        try (EdgeCursor cursor = session.scanEdges(
                                graphId, start, relType.id(), direction, remaining, maxEdges,
                                edge -> rankOk(rankOnly, edge.rank()) && neighborOk(neighborFilter, other(edge, direction)),
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
                } else {
                Set<Integer> typeIds = openTypes ? null : typeIds(rels);
                List<Direction> wideDirections = directions(match.edge().direction());
                for (long start : starts) {
                    if (window >= 0 && hops.size() >= window) {
                        break;
                    }
                    cancelled(cancel);
                    int remaining = window < 0 ? -1 : window - hops.size();
                    try (EdgeCursor cursor = session.scanEdges(
                            graphId, start, typeIds, wideDirections, remaining, maxEdges,
                            edge -> rankOk(rankOnly, edge.rank()) && neighborOk(neighborFilter, endpoint(edge, start)),
                            cancel)) {
                        while (cursor.hasNext()) {
                            EdgeRef edge = cursor.next();
                            if (match.edge().direction() == Stmt.EdgePat.Direction.ANY && !names.seen.add(edge)) {
                                continue;
                            }
                            hops.add(new Hop(start, endpoint(edge, start), edge));
                        }
                    }
                }
                }
            }
            List<Map<String, Object>> edgeProps = needs.edge ? edgeProps(session, graphId, relType, rels, hops) : null;
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
                    eval.slots(slots(graphId, names, match, hop, sourceType, targetType,
                            edgeTypeName(relType, typeNames, hop.edge()),
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
            return project(match, names, sourceType, targetType, relType, typeNames, eval, matched, matchedEdgeProps,
                    sourceProps, targetProps, session, graphId, aggregates, false, cancel);
        }
    }

    private QueryResult nodes(
            Stmt.Match match,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel,
            String[] aggregates,
            Names names) {
        NodeType sourceType = match.source().typeName() == null
                ? null : catalog.nodeType(graphId, match.source().typeName());
        Needs needs = new Needs(match, names, sourceType, null, null, sorts(match, names));
        ExprEval eval = new ExprEval(params, false);
        Long startId = constantId(graphId, sourceType, match.source().properties(), eval);
        int window = window(match);
        Push push = push(match.where(), names, eval);
        IdConstraint idConstraint = idConstraint(graphId, sourceType, match.where(), names, eval);
        try (ReadSession session = store.read()) {
            List<Long> candidates = startId != null
                    ? startId == Catalog.MISSING_VERTEX_ID ? List.of() : List.of(startId)
                    : idConstraint != null
                    ? idConstraint.ids()
                    : null;
            List<Long> starts = filterStarts(
                    session, graphId, sourceType, eval, match.source(),
                    candidates != null
                            ? candidates
                            : indexedStartsOrScan(session, graphId, sourceType, match, names, eval, window, limits, cancel));
            List<Hop> hops = new ArrayList<>();
            for (long vid : starts) {
                if (window >= 0 && hops.size() >= window) {
                    break;
                }
                hops.add(new Hop(vid, vid, null));
            }
            Map<Long, Map<String, Object>> sourceProps = needs.source
                    ? vertexProps(session, graphId, sourceType, hops, true) : Map.of();
            List<Hop> matched = new ArrayList<>();
            for (Hop hop : hops) {
                if (push.residual != null) {
                    eval.slots(slots(graphId, names, match, hop, sourceType, null, null, null,
                            sourceProps.get(hop.start()), null));
                    if (!eval.isTrue(push.residual)) {
                        continue;
                    }
                }
                matched.add(hop);
            }
            return project(match, names, sourceType, null, null, Map.of(), eval, matched, null,
                    sourceProps, Map.of(), session, graphId, aggregates, false, cancel);
        }
    }

    private List<Long> indexedStartsOrScan(
            ReadSession session, int graphId, NodeType sourceType, Stmt.Match match,
            Names names, ExprEval eval, int window, QueryLimits limits, CancelToken cancel) {
        List<Long> indexed = sourceType == null || names.source == null
                ? null
                : IndexLookup.vertexIds(catalog.indexes(graphId), session, graphId, sourceType.id(),
                        sourceType.properties(), names.source, match.where(), eval, cancel);
        if (indexed != null) return indexed;
        boolean safeWindow = window >= 0 && match.where() == null
                && match.orderBy().isEmpty() && !match.distinct();
        return starts(session, graphId, match.source(), sourceType, null,
                safeWindow ? window : limits.maxEdgesPerVertex(), cancel);
    }

    private IdConstraint idConstraint(int graphId, NodeType type, Expr where, Names names, ExprEval eval) {
        if (where == null || names.source == null || type == null) return null;
        if (where instanceof Expr.Binary binary && "AND".equals(binary.op())) {
            IdConstraint left = idConstraint(graphId, type, binary.left(), names, eval);
            return left != null ? left : idConstraint(graphId, type, binary.right(), names, eval);
        }
        if (where instanceof Expr.Binary binary && "=".equals(binary.op())) {
            Expr constant = isIdExpr(binary.left(), names.source) ? binary.right()
                    : isIdExpr(binary.right(), names.source) ? binary.left() : null;
            Object value = evalConstant(constant, eval);
            if (constant != null && value != null) {
                long id = catalog.findVertexId(graphId, type, value);
                return id == Catalog.MISSING_VERTEX_ID ? new IdConstraint(List.of()) : new IdConstraint(List.of(id));
            }
        }
        if (where instanceof Expr.In in && !in.negated()) {
            if (isIdExpr(in.value(), names.source)) {
                List<Long> ids = new ArrayList<>();
                for (Expr option : in.options()) {
                    Object value = evalConstant(option, eval);
                    if (value == null) return null;
                    long id = catalog.findVertexId(graphId, type, value);
                    if (id != Catalog.MISSING_VERTEX_ID) ids.add(id);
                }
                return new IdConstraint(ids);
            }
        }
        return null;
    }

    private static boolean isIdExpr(Expr expr, String variable) {
        return expr instanceof Expr.Prop prop && variable.equals(prop.variable()) && "id".equals(prop.field())
                || expr instanceof Expr.Call call && "id".equalsIgnoreCase(call.name())
                && call.args().size() == 1 && call.args().get(0) instanceof Expr.Variable v && variable.equals(v.name());
    }

    private static Object evalConstant(Expr expr, ExprEval eval) {
        return expr instanceof Expr.Literal || expr instanceof Expr.Param ? eval.eval(expr) : null;
    }

    private record IdConstraint(List<Long> ids) { }

    private QueryResult multi(
            Stmt.Match match,
            int graphId,
            QueryLimits limits,
            CancelToken cancel,
            Names names,
            RelType relType,
            boolean openTypes,
            List<RelType> rels,
            NodeType sourceType,
            NodeType targetType,
            Needs needs,
            ExprEval eval,
            LongPredicate rank,
            LongPredicate neighbor,
            String[] aggregates) {
        int minHop = match.edge().minHop();
        int maxHop = match.edge().maxHop() == null ? minHop : match.edge().maxHop();
        Long startId = constantId(graphId, sourceType, match.source().properties(), eval);
        List<Reach> reached = new ArrayList<>();
        java.util.Set<Long> seen = new java.util.LinkedHashSet<>();
        try (ReadSession session = store.read()) {
            List<Long> starts = filterStarts(
                    session, graphId, sourceType, eval, match.source(),
                    starts(session, graphId, match.source(), sourceType, startId, limits.maxEdgesPerVertex(), cancel));
            for (long start : starts) {
                cancelled(cancel);
                java.util.Set<Long> vids = relType != null
                        ? HopLoop.expandRange(
                                session, graphId, relType.id(), directions(match.edge().direction()), start,
                                minHop, maxHop, limits.maxHops(), limits.maxEdgesPerVertex(),
                                limits.maxFrontierPerHop(), rank, neighbor, cancel)
                        : HopLoop.expandTypes(
                                session, graphId, openTypes ? null : typeIds(rels),
                                directions(match.edge().direction()), start,
                                minHop, maxHop, limits.maxHops(), limits.maxEdgesPerVertex(),
                                limits.maxFrontierPerHop(), rank, neighbor, cancel);
                for (long vid : vids) {
                    if (seen.add(vid)) {
                        reached.add(new Reach(start, vid));
                    }
                }
            }
            if (needs.edgeValue && reached.size() > limits.maxPaths()) {
                throw new CengyanException(ErrorCode.PATH_LIMIT_EXCEEDED,
                        "路径结果有 " + reached.size() + " 行，超过上限 " + limits.maxPaths());
            }
            if (needs.edge || needs.edgeValue || (match.where() != null && residual(match, names, eval))) {
                throw new CengyanException(ErrorCode.SYNTAX, "多跳查询的条件目前只能比较 rank 和终点 id");
            }
            Map<Long, Map<String, Object>> sourceProps = needs.source
                    ? vertexProps(session, graphId, sourceType, reachHops(reached, true), true) : Map.of();
            Map<Long, Map<String, Object>> targetProps = needs.target
                    ? vertexProps(session, graphId, targetType, reachHops(reached, false), false) : Map.of();
            List<Hop> hops = new ArrayList<>();
            for (Reach hop : reached) {
                hops.add(new Hop(hop.start(), hop.vid(), null));
            }
            return project(match, names, sourceType, targetType, relType, Map.of(), eval, hops, null,
                    sourceProps, targetProps, session, graphId, aggregates, true, cancel);
        }
    }

    private static boolean residual(Stmt.Match match, Names names, ExprEval eval) {
        return push(match.where(), names, eval).residual != null;
    }

    private static List<Hop> reachHops(List<Reach> reached, boolean start) {
        List<Hop> hops = new ArrayList<>();
        for (Reach reach : reached) {
            long vid = start ? reach.start() : reach.vid();
            hops.add(new Hop(vid, vid, null));
        }
        return hops;
    }

    private List<Long> starts(
            ReadSession session,
            int graphId,
            Stmt.NodePat source,
            NodeType sourceType,
            Long startId,
            int maxEdges,
            CancelToken cancel) {
        if (startId != null) {
            if (startId == Catalog.MISSING_VERTEX_ID) {
                return List.of();
            }
            if (sourceType != null) {
                byte[] raw = session.get(
                        Cf.VERTEX, KeyCodec.vertexKey(graphId, catalog.partOf(graphId, startId), startId, sourceType.id()));
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

    private List<Map<String, Object>> edgeProps(
            ReadSession session, int graphId, RelType type, List<RelType> rels, List<Hop> hops) {
        List<RelType> types = new ArrayList<>(hops.size());
        List<byte[]> keys = new ArrayList<>(hops.size());
        for (Hop hop : hops) {
            EdgeRef edge = hop.edge();
            RelType rel = type != null ? type : relOf(rels, edge.edgeTypeId());
            types.add(rel);
            keys.add(KeyCodec.propKey(graphId, rel.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        List<byte[]> values = session.multiGet(Cf.PROP, keys);
        List<Map<String, Object>> props = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            byte[] value = values.get(i);
            RelType rel = types.get(i);
            props.add(value == null ? Map.of() : typed(rel.properties(), PropCodec.decode(rel.properties(), rel.schemaVersion(), value)));
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
            byte[] raw = session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, catalog.partOf(graphId, vid), vid, type.id()));
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
            byte[] raw = session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, catalog.partOf(graphId, vid), vid, sourceType.id()));
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

    private Map<String, ExprEval.Slot> slots(
            int graphId,
            Names names,
            Stmt.Match match,
            Hop hop,
            NodeType sourceType,
            NodeType targetType,
            String edgeType,
            Map<String, Object> edgeProps,
            Map<String, Object> sourceProps,
            Map<String, Object> targetProps) {
        Map<String, ExprEval.Slot> slots = new HashMap<>();
        if (names.source != null) {
            slots.put(names.source, ExprEval.Slot.node(
                    hop.start(), catalog.externalVertexId(graphId, sourceType, hop.start()),
                    typeName(sourceType), sourceProps));
        }
        if (names.target != null) {
            slots.put(names.target, ExprEval.Slot.node(
                    hop.neighbor(), catalog.externalVertexId(graphId, targetType, hop.neighbor()),
                    typeName(targetType), targetProps));
        }
        if (names.edge != null && hop.edge() != null) {
            slots.put(names.edge, ExprEval.Slot.edge(
                    edgeType, hop.edge().rank(), hop.edge().outSrc(), hop.edge().outDst(),
                    catalog.externalVertexId(graphId, null, hop.edge().outSrc()),
                    catalog.externalVertexId(graphId, null, hop.edge().outDst()), edgeProps, null, null));
        }
        return slots;
    }

    private Map<String, ExprEval.Slot> slots(
            int graphId,
            Names names,
            Stmt.Match match,
            Hop hop,
            NodeType sourceType,
            NodeType targetType,
            String edgeType,
            Map<String, Object> edgeProps,
            Map<String, Object> sourceProps,
            Map<String, Object> targetProps,
            EndpointTypes ends) {
        Map<String, ExprEval.Slot> slots = new HashMap<>();
        String sourceName = ends.source != null ? ends.source : typeName(sourceType);
        String targetName = ends.target != null ? ends.target : typeName(targetType);
        if (names.source != null) {
            slots.put(names.source, ExprEval.Slot.node(
                    hop.start(), catalog.externalVertexId(graphId, sourceType, hop.start()), sourceName, sourceProps));
        }
        if (names.target != null) {
            slots.put(names.target, ExprEval.Slot.node(
                    hop.neighbor(), catalog.externalVertexId(graphId, targetType, hop.neighbor()), targetName, targetProps));
        }
        if (names.edge != null && hop.edge() != null) {
            slots.put(names.edge, ExprEval.Slot.edge(
                    edgeType, hop.edge().rank(), hop.edge().outSrc(), hop.edge().outDst(),
                    catalog.externalVertexId(graphId, null, hop.edge().outSrc()),
                    catalog.externalVertexId(graphId, null, hop.edge().outDst()), edgeProps,
                    ends.edgeSrc, ends.edgeDst));
        }
        return slots;
    }

    private String vertexType(ReadSession session, int graphId, long vid, Map<Long, String> cache) {
        String cached = cache.get(vid);
        if (cached != null) {
            return cached.isEmpty() ? null : cached;
        }
        byte[] prefix = KeyCodec.vertexPrefix(graphId, catalog.partOf(graphId, vid), vid);
        List<String> names = new ArrayList<>();
        for (byte[] key : session.keysWithPrefix(Cf.VERTEX, prefix)) {
            if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
                continue;
            }
            int tagId = KeyCodec.tagIdOf(key);
            for (NodeType type : catalog.nodeTypes(graphId)) {
                if (type.id() == tagId) {
                    names.add(type.name());
                }
            }
        }
        String name = String.join(", ", names);
        cache.put(vid, name);
        return name.isEmpty() ? null : name;
    }

    private String endpointType(
            long vid,
            Hop hop,
            String sourceName,
            String targetName,
            ReadSession session,
            int graphId,
            Map<Long, String> cache) {
        if (vid == hop.start() && sourceName != null) {
            return sourceName;
        }
        if (vid == hop.neighbor() && targetName != null) {
            return targetName;
        }
        return vertexType(session, graphId, vid, cache);
    }

    private record EndpointTypes(String source, String target, String edgeSrc, String edgeDst) {
    }

    private static String typeName(NodeType type) {
        return type == null ? null : type.name();
    }

    private QueryResult project(
            Stmt.Match match,
            Names names,
            NodeType sourceType,
            NodeType targetType,
            RelType relType,
            Map<Integer, String> typeNames,
            ExprEval eval,
            List<Hop> matched,
            List<Map<String, Object>> edgeProps,
            Map<Long, Map<String, Object>> sourceProps,
            Map<Long, Map<String, Object>> targetProps,
            ReadSession session,
            int graphId,
            String[] aggregates,
            boolean limitAfter,
            CancelToken cancel) {
        List<String> columns = new ArrayList<>();
        for (Stmt.Item item : match.items()) {
            columns.add(item.alias() == null ? item.text() : item.alias());
        }
        List<Stmt.Sort> sorts = aggregates == null ? sorts(match, names) : List.of();
        List<List<Object>> rows = new ArrayList<>();
        List<List<Object>> sortKeys = new ArrayList<>();
        Map<Long, String> typeCache = new HashMap<>();
        for (int i = 0; i < matched.size(); i++) {
            Hop hop = matched.get(i);
            String sourceName = sourceType != null
                    ? sourceType.name() : vertexType(session, graphId, hop.start(), typeCache);
            String targetName = targetType != null
                    ? targetType.name() : vertexType(session, graphId, hop.neighbor(), typeCache);
            String edgeSrc = hop.edge() == null ? null : endpointType(
                    hop.edge().outSrc(), hop, sourceName, targetName, session, graphId, typeCache);
            String edgeDst = hop.edge() == null ? null : endpointType(
                    hop.edge().outDst(), hop, sourceName, targetName, session, graphId, typeCache);
            eval.slots(slots(graphId, names, match, hop, sourceType, targetType,
                    edgeTypeName(relType, typeNames, hop.edge()),
                    edgeProps == null ? null : edgeProps.get(i),
                    sourceProps.get(hop.start()), targetProps.get(hop.neighbor()),
                    new EndpointTypes(sourceName, targetName, edgeSrc, edgeDst)));
            List<Object> row = new ArrayList<>();
            for (int column = 0; column < match.items().size(); column++) {
                row.add(cell(eval, match.items().get(column), aggregates == null ? null : aggregates[column]));
            }
            rows.add(row);
            if (!sorts.isEmpty()) {
                List<Object> keys = new ArrayList<>();
                for (Stmt.Sort sort : sorts) {
                    keys.add(DateTimes.plain(eval.eval(sort.expr())));
                }
                sortKeys.add(keys);
            }
        }
        return finish(match, columns, rows, sortKeys, sorts, aggregates, limitAfter, cancel);
    }

    private QueryResult finish(
            Stmt.Match match,
            List<String> columns,
            List<List<Object>> rows,
            List<List<Object>> sortKeys,
            List<Stmt.Sort> sorts,
            String[] aggregates,
            boolean limitAfter,
            CancelToken cancel) {
        Long skip = match.skip();
        Long limit = limitAfter ? match.limit() : null;
        boolean reshape = aggregates != null || match.distinct() || !sorts.isEmpty() || (skip != null && skip > 0);
        if (!reshape) {
            if (limit != null && limit < rows.size()) {
                rows = new ArrayList<>(rows.subList(0, limit.intValue()));
            }
            return new QueryResult(columns, rows, 0);
        }
        List<List<Object>> shaped = aggregates != null
                ? page(DuckResult.aggregate(rows, aggregates, duck, cancel), skip, limit)
                : DuckResult.arrange(rows, sortKeys, sorts, match.distinct(), skip, limit, duck, cancel);
        return new QueryResult(columns, shaped, 0);
    }

    private static List<List<Object>> page(List<List<Object>> rows, Long skip, Long limit) {
        int from = skip == null ? 0 : Math.toIntExact(skip);
        if (from >= rows.size()) {
            return List.of();
        }
        int to = rows.size();
        if (limit != null) {
            to = (int) Math.min(to, (long) from + limit);
        }
        if (from == 0 && to == rows.size()) {
            return rows;
        }
        return new ArrayList<>(rows.subList(from, to));
    }

    private static Object cell(ExprEval eval, Stmt.Item item, String kind) {
        if (kind == null) {
            return DateTimes.plain(eval.eval(item.expr()));
        }
        if ("star".equals(kind)) {
            return 1L;
        }
        Expr.Call call = (Expr.Call) item.expr();
        return DateTimes.plain(eval.eval(call.args().get(0)));
    }

    private static String[] aggregateKinds(Stmt.Match match) {
        String[] kinds = new String[match.items().size()];
        boolean aggregate = false;
        boolean plain = false;
        for (int i = 0; i < match.items().size(); i++) {
            Expr expr = match.items().get(i).expr();
            if (expr instanceof Expr.Call call && aggregateName(call.name())) {
                aggregate = true;
                boolean star = call.args().size() == 1
                        && call.args().get(0) instanceof Expr.Literal literal
                        && "*".equals(literal.value());
                String name = call.name().toLowerCase(java.util.Locale.ROOT);
                if (star && !"count".equals(name)) {
                    throw new CengyanException(ErrorCode.SYNTAX, name + " 不能写星号");
                }
                kinds[i] = star ? "star" : name;
            } else {
                plain = true;
            }
        }
        if (!aggregate) {
            return null;
        }
        if (plain) {
            throw new CengyanException(ErrorCode.SYNTAX, "聚合不能和普通列混在一起");
        }
        if (match.distinct()) {
            throw new CengyanException(ErrorCode.SYNTAX, "聚合不能再写 DISTINCT");
        }
        if (!match.orderBy().isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "聚合结果不能再排序");
        }
        return kinds;
    }

    private static boolean aggregateName(String name) {
        return switch (name.toLowerCase(java.util.Locale.ROOT)) {
            case "count", "sum" -> true;
            default -> false;
        };
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

    private Long constantId(int graphId, NodeType type, List<Stmt.Prop> props, ExprEval eval) {
        for (Stmt.Prop prop : props) {
            if (!"id".equals(prop.name())) {
                continue;
            }
            return catalog.findVertexId(graphId, type, eval.eval(prop.value()));
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

    private static List<Hop> hopsFromIndex(
            List<EdgeRef> edges,
            Stmt.EdgePat.Direction direction,
            List<Long> allowed,
            boolean restrict,
            LongPredicate rankOnly,
            LongPredicate neighborOnly,
            int window,
            CancelToken cancel) {
        java.util.Set<Long> allow = restrict ? new java.util.HashSet<>(allowed) : null;
        List<Hop> hops = new ArrayList<>();
        int seen = 0;
        for (EdgeRef edge : edges) {
            if ((seen++ & 63) == 0) {
                cancelled(cancel);
            }
            if (!rankOk(rankOnly, edge.rank())) {
                continue;
            }
            boolean room = switch (direction) {
                case OUT -> allow == null || allow.contains(edge.outSrc())
                        ? addHop(hops, edge.outSrc(), edge.outDst(), edge, neighborOnly, window)
                        : true;
                case IN -> allow == null || allow.contains(edge.outDst())
                        ? addHop(hops, edge.outDst(), edge.outSrc(), edge, neighborOnly, window)
                        : true;
                case ANY -> {
                    if (allow == null || allow.contains(edge.outSrc())) {
                        yield addHop(hops, edge.outSrc(), edge.outDst(), edge, neighborOnly, window);
                    }
                    if (allow.contains(edge.outDst())) {
                        yield addHop(hops, edge.outDst(), edge.outSrc(), edge, neighborOnly, window);
                    }
                    yield true;
                }
            };
            if (!room) {
                break;
            }
        }
        return hops;
    }

    /** @return 窗口还没满 */
    private static boolean addHop(
            List<Hop> hops, long start, long neighbor, EdgeRef edge, LongPredicate neighborOnly, int window) {
        if (window >= 0 && hops.size() >= window) {
            return false;
        }
        if (!neighborOk(neighborOnly, neighbor)) {
            return true;
        }
        hops.add(new Hop(start, neighbor, edge));
        return window < 0 || hops.size() < window;
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
        if (match.edge() != null) {
            names.edge = unique(names, match.edge().variable());
            names.target = unique(names, match.target().variable());
        }
        return names;
    }

    private List<RelType> namedRels(int graphId, List<String> typeNames) {
        List<RelType> rels = new ArrayList<>(typeNames.size());
        for (String typeName : typeNames) {
            rels.add(catalog.relType(graphId, typeName));
        }
        return rels;
    }

    private static Set<Integer> typeIds(List<RelType> rels) {
        LinkedHashSet<Integer> ids = new LinkedHashSet<>();
        for (RelType type : rels) {
            ids.add(type.id());
        }
        return ids;
    }

    private static RelType relOf(List<RelType> rels, int edgeTypeId) {
        for (RelType type : rels) {
            if (type.id() == edgeTypeId) {
                return type;
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
    }

    private static String edgeTypeName(RelType only, Map<Integer, String> typeNames, EdgeRef edge) {
        if (edge == null) {
            return null;
        }
        if (only != null) {
            return only.name();
        }
        String name = typeNames.get(edge.edgeTypeId());
        if (name == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
        }
        return name;
    }

    private static long endpoint(EdgeRef edge, long start) {
        return edge.outSrc() == start ? edge.outDst() : edge.outSrc();
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

    private record Reach(long start, long vid) {
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
        private final boolean edgeValue;

        private Needs(
                Stmt.Match match,
                Names names,
                NodeType sourceType,
                NodeType targetType,
                List<RelType> edgeTypes,
                List<Stmt.Sort> orderBy) {
            if (match.edge() != null && edgeTypes != null) {
                for (RelType type : edgeTypes) {
                    checkProps(match.edge().properties(), type.properties(), false, "边");
                }
            }
            if (!withoutId(match.source().properties()).isEmpty() && sourceType == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "起点属性需要节点类型");
            }
            if (sourceType != null) {
                checkProps(match.source().properties(), sourceType.properties(), true, "起点");
            }
            if (match.target() != null && !withoutId(match.target().properties()).isEmpty() && targetType == null) {
                throw new CengyanException(ErrorCode.SYNTAX, "不知道终点的节点类型");
            }
            if (targetType != null) {
                checkProps(match.target().properties(), targetType.properties(), true, "终点");
            }
            boolean edge = match.edge() != null && !match.edge().properties().isEmpty();
            boolean source = !withoutId(match.source().properties()).isEmpty();
            boolean target = match.target() != null && !withoutId(match.target().properties()).isEmpty();
            Walker walker = new Walker(names, sourceType, targetType, edgeTypes);
            walker.place = 0;
            if (match.where() != null) {
                walker.walk(match.where());
            }
            walker.place = 1;
            for (Stmt.Item item : match.items()) {
                walker.walk(item.expr());
            }
            walker.place = 2;
            for (Stmt.Sort sort : orderBy) {
                walker.walk(sort.expr());
            }
            this.edge = edge || walker.edge;
            this.source = source || walker.source;
            this.target = target || walker.target;
            this.edgeValue = walker.edge;
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
        private final List<RelType> edgeTypes;
        private boolean edge;
        private boolean source;
        private boolean target;
        private int place;

        private Walker(Names names, NodeType sourceType, NodeType targetType, List<RelType> edgeTypes) {
            this.names = names;
            this.sourceType = sourceType;
            this.targetType = targetType;
            this.edgeTypes = edgeTypes;
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
                    if (aggregateName(call.name())) {
                        if (place != 1) {
                            throw new CengyanException(ErrorCode.SYNTAX, "聚合只能写在 RETURN");
                        }
                        boolean star = call.args().size() == 1
                                && call.args().get(0) instanceof Expr.Literal literal
                                && "*".equals(literal.value());
                        String lower = call.name().toLowerCase(java.util.Locale.ROOT);
                        if (star && !"count".equals(lower)) {
                            throw new CengyanException(ErrorCode.SYNTAX, lower + " 不能写星号");
                        }
                        if (!star && call.args().size() != 1) {
                            throw new CengyanException(ErrorCode.SYNTAX, call.name() + " 的参数个数不对");
                        }
                    }
                    for (Expr arg : call.args()) {
                        if (arg instanceof Expr.Literal literal && "*".equals(literal.value())) {
                            continue;
                        }
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
            if (names.edge != null && prop.variable().equals(names.edge)) {
                if (!"rank".equals(prop.field())) {
                    if (edgeTypes != null) {
                        for (RelType type : edgeTypes) {
                            if (!has(type.properties(), prop.field())) {
                                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + prop.field());
                            }
                        }
                    }
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
                case "id", "labels", "type", "properties", "startnode", "endnode", "coalesce", "size", "length", "count", "sum" -> {
                }
                default -> throw new CengyanException(ErrorCode.SYNTAX, "未知函数 " + name);
            }
        }
    }
}
