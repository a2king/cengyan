package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.ValueType;
import io.cengyan.parser.Stmt;
import io.cengyan.store.GraphStore;
import io.cengyan.store.ReadSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** {@code CALL ... YIELD}。最短路和穿透持股不走普通 MATCH。 */
final class ProcedureExecutor {
    private static final Set<String> SHORTEST_ARGS = Set.of("start", "end", "rel", "hops");
    private static final Set<String> OWNERSHIP_ARGS = Set.of("start", "rel", "ratioProp", "hops");
    private static final Set<String> SHORTEST_YIELD = Set.of("length", "vids");
    private static final Set<String> OWNERSHIP_YIELD = Set.of("vid", "ratio");

    private final Catalog catalog;
    private final GraphStore store;
    private final double minEdgeRatio;
    private final double minAccumRatio;

    ProcedureExecutor(Catalog catalog, GraphStore store, double minEdgeRatio, double minAccumRatio) {
        this.catalog = catalog;
        this.store = store;
        this.minEdgeRatio = minEdgeRatio;
        this.minAccumRatio = minAccumRatio;
    }

    QueryResult call(Stmt.Call call, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        Map<String, Object> args = arguments(call, params);
        String name = call.name().toLowerCase(Locale.ROOT);
        return switch (name) {
            case "path.shortest" -> shortest(graphId, args, call.yield(), limits, cancel);
            case "ownership.largest_holder" -> ownership(graphId, args, call.yield(), limits, cancel);
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
        long start = requiredLong(args, "start");
        long end = requiredLong(args, "end");
        String rel = requiredString(args, "rel");
        int hops = hops(requiredLong(args, "hops"), limits.maxHops());
        RelType type = catalog.relType(graphId, rel);
        ShortestPath.Found found;
        try (ReadSession session = store.read()) {
            found = ShortestPath.find(
                    session, graphId, type.id(), start, end, hops, limits.maxHops(),
                    limits.maxEdgesPerVertex(), limits.maxFrontierPerHop(), cancel);
        }
        if (found == null) {
            return new QueryResult(columns, List.of(), 0);
        }
        Map<String, Object> values = Map.of("length", (long) found.length(), "vids", found.vids());
        return new QueryResult(columns, List.of(row(columns, values)), 0);
    }

    private QueryResult ownership(
            int graphId,
            Map<String, Object> args,
            List<String> yield,
            QueryLimits limits,
            CancelToken cancel) {
        known(args, OWNERSHIP_ARGS);
        List<String> columns = columns(yield, OWNERSHIP_YIELD);
        long start = requiredLong(args, "start");
        String rel = requiredString(args, "rel");
        String ratioProp = requiredString(args, "ratioProp");
        int hops = hops(requiredLong(args, "hops"), limits.maxHops());
        RelType type = catalog.relType(graphId, rel);
        requireRatio(type, ratioProp);
        OwnershipExecutor.Result result;
        try (ReadSession session = store.read()) {
            result = OwnershipExecutor.largest(
                    session, graphId, type, ratioProp, start, hops, minEdgeRatio, minAccumRatio, cancel);
        }
        if (result.vid() == null) {
            return new QueryResult(columns, List.of(), 0);
        }
        Map<String, Object> values = Map.of("vid", result.vid(), "ratio", result.ratio());
        return new QueryResult(columns, List.of(row(columns, values)), 0);
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

    private static String requiredString(Map<String, Object> args, String key) {
        Object value = required(args, key);
        if (value instanceof String text) {
            return text;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, key + " 必须是字符串");
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
