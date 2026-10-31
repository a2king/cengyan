package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.ValueType;
import io.cengyan.parser.Stmt;
import io.cengyan.store.Direction;
import io.cengyan.store.GraphStore;
import io.cengyan.store.ReadSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** {@code CALL ... YIELD}。最短路、全部路径和穿透持股。 */
final class ProcedureExecutor {
    private static final Set<String> SHORTEST_ARGS = Set.of("start", "end", "rel", "hops");
    private static final Set<String> ALL_ARGS = Set.of("start", "end", "rel", "min", "max", "direction", "limit");
    private static final Set<String> OWNERSHIP_ARGS = Set.of("start", "rel", "ratioProp", "hops");
    private static final Set<String> SUBGRAPH_ARGS = Set.of("start", "rel", "hops");
    private static final Set<String> SHORTEST_YIELD = Set.of("length", "vids");
    private static final Set<String> ALL_YIELD = Set.of("length", "path");
    private static final Set<String> OWNERSHIP_YIELD = Set.of("vid", "ratio");
    private static final Set<String> SUBGRAPH_YIELD = Set.of("vid");

    private final Catalog catalog;
    private final GraphStore store;

    ProcedureExecutor(Catalog catalog, GraphStore store) {
        this.catalog = catalog;
        this.store = store;
    }

    QueryResult call(Stmt.Call call, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        Map<String, Object> args = arguments(call, params);
        String name = call.name().toLowerCase(Locale.ROOT);
        return switch (name) {
            case "path.shortest" -> shortest(graphId, args, call.yield(), limits, cancel);
            case "path.all" -> all(graphId, args, call.yield(), limits, cancel);
            case "ownership.largest_holder" -> ownership(graphId, args, call.yield(), limits, cancel);
            case "subgraph.bounded" -> subgraph(graphId, args, call.yield(), limits, cancel);
            default -> throw new CengyanException(ErrorCode.SYNTAX, "未知过程 " + call.name());
        };
    }

    private QueryResult shortest(
            int graphId,
            Map<String, Object> args,
            List<String> yield,
            QueryLimits limits,
            CancelToken cancel) {
        known(args, SHORTEST_ARGS);
        List<String> columns = columns(yield, SHORTEST_YIELD);
        long start = requiredVertexId(graphId, args, "start");
        long end = requiredVertexId(graphId, args, "end");
        String rel = requiredString(args, "rel");
        int hops = hops(requiredLong(args, "hops"), limits.maxHops());
        RelType type = catalog.relType(graphId, rel);
        if (start == Catalog.MISSING_VERTEX_ID || end == Catalog.MISSING_VERTEX_ID) {
            return new QueryResult(columns, List.of(), 0);
        }
        ShortestPath.Found found;
        try (ReadSession session = store.read()) {
            found = ShortestPath.find(
                    session, graphId, type.id(), start, end, hops, limits.maxHops(),
                    limits.maxEdgesPerVertex(), limits.maxFrontierPerHop(), cancel);
        }
        if (found == null) {
            return new QueryResult(columns, List.of(), 0);
        }
        List<Object> external = new ArrayList<>(found.vids().size());
        for (long vid : found.vids()) {
            external.add(catalog.externalVertexId(graphId, null, vid));
        }
        Map<String, Object> values = Map.of("length", (long) found.length(), "vids", external);
        return new QueryResult(columns, List.of(row(columns, values)), 0);
    }

    private QueryResult all(
            int graphId,
            Map<String, Object> args,
            List<String> yield,
            QueryLimits limits,
            CancelToken cancel) {
        known(args, ALL_ARGS);
        List<String> columns = columns(yield, ALL_YIELD);
        long start = requiredVertexId(graphId, args, "start");
        Long end = args.containsKey("end") ? requiredVertexId(graphId, args, "end") : null;
        int minHop = args.containsKey("min") ? hops(requiredLong(args, "min"), limits.maxHops()) : 1;
        int maxHop = hops(requiredLong(args, "max"), limits.maxHops());
        int limit = pathLimit(requiredLong(args, "limit"), limits.maxPaths());
        String direction = args.containsKey("direction") ? requiredString(args, "direction") : "out";
        if (start == Catalog.MISSING_VERTEX_ID || (end != null && end == Catalog.MISSING_VERTEX_ID)) {
            return new QueryResult(columns, List.of(), 0);
        }
        List<PathAll.Found> found;
        try (ReadSession session = store.read()) {
            found = PathAll.find(
                    session,
                    graphId,
                    edgeTypes(graphId, args),
                    directions(direction),
                    start,
                    end,
                    minHop,
                    maxHop,
                    limits.maxHops(),
                    limits.maxEdgesPerVertex(),
                    limit,
                    null,
                    cancel);
        }
        Map<Integer, String> typeNames = PathValue.typeNames(catalog, graphId);
        List<List<Object>> rows = new ArrayList<>(found.size());
        for (PathAll.Found path : found) {
            Map<String, Object> body = PathValue.of(catalog, graphId, typeNames, path);
            Map<String, Object> values = Map.of(
                    "length", (long) path.length(),
                    "path", body);
            rows.add(row(columns, values));
        }
        return new QueryResult(columns, rows, 0);
    }

    private QueryResult ownership(
            int graphId,
            Map<String, Object> args,
            List<String> yield,
            QueryLimits limits,
            CancelToken cancel) {
        known(args, OWNERSHIP_ARGS);
        List<String> columns = columns(yield, OWNERSHIP_YIELD);
        long start = requiredVertexId(graphId, args, "start");
        String rel = requiredString(args, "rel");
        String ratioProp = requiredString(args, "ratioProp");
        int hops = hops(requiredLong(args, "hops"), limits.maxHops());
        RelType type = catalog.relType(graphId, rel);
        requireRatio(type, ratioProp);
        OwnershipExecutor.Result result;
        try (ReadSession session = store.read()) {
            result = OwnershipExecutor.largest(
                    session, graphId, type, ratioProp, start, hops, cancel);
        }
        if (result.vid() == null || start == Catalog.MISSING_VERTEX_ID) {
            return new QueryResult(columns, List.of(), 0);
        }
        Map<String, Object> values = Map.of(
                "vid", catalog.externalVertexId(graphId, null, result.vid()), "ratio", result.ratio());
        return new QueryResult(columns, List.of(row(columns, values)), 0);
    }

    /** 从起点沿这种边的两个方向扩展。起点算在结果里，只返回 vid。 */
    private QueryResult subgraph(
            int graphId,
            Map<String, Object> args,
            List<String> yield,
            QueryLimits limits,
            CancelToken cancel) {
        known(args, SUBGRAPH_ARGS);
        List<String> columns = columns(yield, SUBGRAPH_YIELD);
        long start = requiredVertexId(graphId, args, "start");
        String rel = requiredString(args, "rel");
        int hops = hops(requiredLong(args, "hops"), limits.maxHops());
        RelType type = catalog.relType(graphId, rel);
        List<Long> vids = new ArrayList<>();
        if (start == Catalog.MISSING_VERTEX_ID) {
            return new QueryResult(columns, List.of(), 0);
        }
        vids.add(start);
        if (hops > 0) {
            try (ReadSession session = store.read()) {
                vids.addAll(HopLoop.expandRange(
                        session, graphId, type.id(), List.of(Direction.OUT, Direction.IN),
                        start, 1, hops, limits.maxHops(), limits.maxEdgesPerVertex(),
                        limits.maxFrontierPerHop(), null, null, cancel));
            }
        }
        List<List<Object>> rows = new ArrayList<>();
        for (long vid : vids) {
            rows.add(row(columns, Map.<String, Object>of(
                    "vid", catalog.externalVertexId(graphId, null, vid))));
        }
        return new QueryResult(columns, rows, 0);
    }

    private static void requireRatio(RelType type, String ratioProp) {
        PropDef found = null;
        for (PropDef property : type.properties()) {
            if (property.name().equals(ratioProp)) {
                found = property;
                break;
            }
        }
        if (found == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性");
        }
        if (found.type() != ValueType.DOUBLE && found.type() != ValueType.INT64) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "比例必须是数字");
        }
    }

    private static Map<String, Object> arguments(Stmt.Call call, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, false);
        Map<String, Object> values = new LinkedHashMap<>();
        for (Stmt.Arg arg : call.args()) {
            values.put(arg.name(), eval.eval(arg.value()));
        }
        return values;
    }

    private static void known(Map<String, Object> args, Set<String> allowed) {
        for (String key : args.keySet()) {
            if (!allowed.contains(key)) {
                throw new CengyanException(ErrorCode.SYNTAX, "未知参数 " + key);
            }
        }
    }

    private static List<String> columns(List<String> yield, Set<String> allowed) {
        if (yield.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "YIELD 不能为空");
        }
        Set<String> seen = new HashSet<>();
        for (String name : yield) {
            if (!allowed.contains(name)) {
                throw new CengyanException(ErrorCode.SYNTAX, "不能返回 " + name);
            }
            if (!seen.add(name)) {
                throw new CengyanException(ErrorCode.SYNTAX, "返回列 " + name + " 重复了");
            }
        }
        return yield;
    }

    private static List<Object> row(List<String> columns, Map<String, Object> values) {
        List<Object> row = new ArrayList<>(columns.size());
        for (String column : columns) {
            row.add(values.get(column));
        }
        return row;
    }

    private static Object required(Map<String, Object> args, String key) {
        if (!args.containsKey(key)) {
            throw new CengyanException(ErrorCode.SYNTAX, "缺少参数 " + key);
        }
        return args.get(key);
    }

    private static long requiredLong(Map<String, Object> args, String key) {
        Object value = required(args, key);
        if (value instanceof Long number) {
            return number;
        }
        if (value instanceof Integer number) {
            return number.longValue();
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, key + " 必须是整数");
    }

    private long requiredVertexId(int graphId, Map<String, Object> args, String key) {
        Object value = required(args, key);
        if (!(value instanceof Number) && !(value instanceof String)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, key + " 必须是整数或字符串");
        }
        return catalog.findVertexId(graphId, null, value);
    }

    private static String requiredString(Map<String, Object> args, String key) {
        Object value = required(args, key);
        if (value instanceof String text) {
            return text;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, key + " 必须是字符串");
    }

    private Set<Integer> edgeTypes(int graphId, Map<String, Object> args) {
        if (!args.containsKey("rel")) {
            return null;
        }
        String text = requiredString(args, "rel");
        if (text.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "rel 不能为空");
        }
        Set<Integer> ids = new LinkedHashSet<>();
        for (String part : text.split("\\|", -1)) {
            if (part.isEmpty()) {
                throw new CengyanException(ErrorCode.SYNTAX, "rel 里有空的边类型");
            }
            RelType type = catalog.relType(graphId, part);
            if (!ids.add(type.id())) {
                throw new CengyanException(ErrorCode.SYNTAX, "边类型重复：" + part);
            }
        }
        return ids;
    }

    private static List<Direction> directions(String text) {
        return switch (text.toLowerCase(Locale.ROOT)) {
            case "out" -> List.of(Direction.OUT);
            case "in" -> List.of(Direction.IN);
            case "both" -> List.of(Direction.OUT, Direction.IN);
            default -> throw new CengyanException(ErrorCode.SYNTAX, "方向只能是 out、in 或 both");
        };
    }

    private static int pathLimit(long value, int maxPaths) {
        if (value < 1 || value > Integer.MAX_VALUE) {
            throw new CengyanException(ErrorCode.SYNTAX, "路径条数超出范围");
        }
        int limit = (int) value;
        if (limit > maxPaths) {
            throw new CengyanException(ErrorCode.SYNTAX, "路径条数 " + limit + " 超过上限 " + maxPaths);
        }
        return limit;
    }

    private static int hops(long value, int maxHops) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数超出范围");
        }
        int hops = (int) value;
        if (hops > maxHops) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数 " + hops + " 超过上限 " + maxHops);
        }
        return hops;
    }
}
