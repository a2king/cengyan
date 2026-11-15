package io.cengyan.query;

import io.cengyan.distributed.*;
import io.cengyan.parser.Stmt;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.util.*;
import java.util.function.BooleanSupplier;

/** 过程的分布式邻接访问版本，frontier 和最终聚合在执行 Compute。 */
final class RemoteProcedures {
    private final DistributedGraph data;
    private final GraphDefinition graph;
    private final TransactionEngine.QueryScope scope;
    private final BooleanSupplier canceled;
    RemoteProcedures(DistributedGraph data, GraphDefinition graph, TransactionEngine.QueryScope scope, BooleanSupplier canceled) {
        this.data = data; this.graph = graph; this.scope = scope; this.canceled = canceled;
    }
    QueryResult execute(Stmt.Call call, ExprEval eval) {
        Map<String, Object> args = new LinkedHashMap<>();
        for (Stmt.Arg arg : call.args()) {
            Object value = eval.eval(arg.value());
            if (arg.name() == null && value instanceof Map<?, ?> map) map.forEach((key, val) -> args.put(String.valueOf(key), val));
            else args.put(arg.name(), value);
        }
        String name = call.name().toLowerCase(Locale.ROOT);
        Value start = EntityCodec.value(required(args, "start"));
        String rel = String.valueOf(required(args, "rel"));
        int max = integer(args, name.equals("path.all") ? "max" : "hops", 10);
        if (max < 0 || max > 32) throw new ClusterException("PATH_LIMIT", "过程跳数必须在 0 到 32 之间");
        if (data.get(graph, true, start, scope) == null) return new QueryResult(columns(call, name), List.of(), 0);
        List<Map<String, Object>> records = switch (name) {
            case "subgraph.bounded" -> subgraph(start, rel, max);
            case "path.shortest" -> shortest(start, EntityCodec.value(required(args, "end")), rel, max);
            case "path.all" -> all(start, args.containsKey("end") ? EntityCodec.value(args.get("end")) : null, rel,
                    integer(args, "min", 1), max, integer(args, "limit", 100), String.valueOf(args.getOrDefault("direction", "out")));
            case "ownership.largest_holder" -> ownership(start, rel, String.valueOf(required(args, "ratioProp")), max);
            default -> throw new ClusterException("SYNTAX", "未知过程：" + call.name());
        };
        List<String> columns = columns(call, name);
        return new QueryResult(columns, records.stream().map(record -> columns.stream().map(record::get).toList()).toList(), 0);
    }
    private List<Map<String, Object>> subgraph(Value start, String rel, int max) {
        LinkedHashSet<Value> visited = new LinkedHashSet<>(); visited.add(start);
        List<Value> frontier = List.of(start);
        for (int hop = 0; hop < max && !frontier.isEmpty(); hop++) {
            List<Value> next = new ArrayList<>();
            for (Value current : frontier) for (Entity edge : adjacency(current, rel, "both")) {
                Value vertex = other(edge, current);
                if (visited.add(vertex)) next.add(vertex);
                bounded(visited.size());
            }
            frontier = next;
        }
        return visited.stream().map(id -> Map.<String, Object>of("vid", EntityCodec.object(id))).toList();
    }
    private List<Map<String, Object>> shortest(Value start, Value end, String rel, int max) {
        Map<Value, Value> previous = new HashMap<>(); previous.put(start, start);
        List<Value> frontier = List.of(start);
        for (int hop = 0; hop <= max && !frontier.isEmpty(); hop++) {
            if (previous.containsKey(end)) {
                List<Object> path = new ArrayList<>(); Value current = end;
                while (!current.equals(start)) { path.add(EntityCodec.object(current)); current = previous.get(current); }
                path.add(EntityCodec.object(start)); Collections.reverse(path);
                return List.of(Map.of("length", (long) path.size() - 1, "vids", path));
            }
            if (hop == max) break;
            List<Value> next = new ArrayList<>();
            for (Value current : frontier) for (Entity edge : adjacency(current, rel, "out")) {
                Value target = edge.getTarget();
                if (!previous.containsKey(target)) { previous.put(target, current); next.add(target); }
                bounded(previous.size());
            }
            frontier = next;
        }
        return List.of();
    }
    private List<Map<String, Object>> all(Value start, Value end, String rel, int min, int max, int limit, String direction) {
        if (min < 0 || min > max || limit < 1 || limit > 100000) throw new ClusterException("INVALID_ARGUMENT", "路径数和跳数边界无效");
        List<Map<String, Object>> result = new ArrayList<>();
        paths(start, end, rel, min, max, limit, direction, new ArrayList<>(List.of(start)), new ArrayList<>(), result);
        return result;
    }
    private void paths(Value current, Value end, String rel, int min, int max, int limit, String direction,
                       List<Value> vertices, List<Entity> edges, List<Map<String, Object>> result) {
        check();
        if (result.size() == limit) return;
        int length = edges.size();
        if (length >= min && (end == null || end.equals(current))) {
            var body = new LinkedHashMap<String, Object>();
            body.put("length", (long) length);
            body.put("nodes", vertices.stream().map(id -> entity(data.get(graph, true, id, scope))).toList());
            body.put("edges", edges.stream().map(RemoteProcedures::entity).toList());
            result.add(Map.of("length", (long) length, "path", body));
        }
        if (length == max || result.size() == limit) return;
        for (Entity edge : adjacency(current, rel, direction)) {
            Value next = other(edge, current);
            edges.add(edge); vertices.add(next);
            paths(next, end, rel, min, max, limit, direction, vertices, edges, result);
            vertices.removeLast(); edges.removeLast();
            if (result.size() == limit) return;
        }
    }
    private List<Map<String, Object>> ownership(Value start, String rel, String property, int max) {
        record Step(Value vertex, double ratio, Set<Value> path) { }
        List<Step> frontier = List.of(new Step(start, 1.0, Set.of(start)));
        Map<Value, Double> sums = new HashMap<>();
        for (int hop = 0; hop < max && !frontier.isEmpty(); hop++) {
            List<Step> next = new ArrayList<>();
            for (Step current : frontier) for (Entity edge : adjacency(current.vertex(), rel, "out")) {
                Object raw = EntityCodec.object(edge.getPropertiesOrDefault(property, Value.getDefaultInstance()));
                if (!(raw instanceof Number number)) continue;
                double ratio = number.doubleValue(), accumulated = current.ratio() * ratio;
                if (!Double.isFinite(ratio) || ratio < 0 || ratio > 1) throw new ClusterException("TYPE_MISMATCH", "持股比例必须位于 0 与 1 之间");
                if (ratio < 0.01 || accumulated < 0.001 || current.path().contains(edge.getTarget())) continue;
                sums.merge(edge.getTarget(), accumulated, Double::sum);
                Set<Value> path = new HashSet<>(current.path()); path.add(edge.getTarget());
                next.add(new Step(edge.getTarget(), accumulated, path)); bounded(next.size());
            }
            frontier = next;
        }
        return sums.entrySet().stream().max(Comparator.<Map.Entry<Value, Double>>comparingDouble(Map.Entry::getValue)
                        .thenComparing(entry -> entry.getKey().toString()))
                .map(entry -> List.of(Map.<String, Object>of("vid", EntityCodec.object(entry.getKey()), "ratio", entry.getValue()))).orElse(List.of());
    }
    private List<Entity> adjacency(Value vertex, String rel, String direction) {
        check(); List<Entity> result = new ArrayList<>();
        if (!direction.equalsIgnoreCase("in")) result.addAll(data.neighbors(graph, vertex, true, rel, scope));
        if (!direction.equalsIgnoreCase("out")) result.addAll(data.neighbors(graph, vertex, false, rel, scope));
        LinkedHashMap<Value, Entity> unique = new LinkedHashMap<>(); result.forEach(edge -> unique.put(edge.getId(), edge));
        bounded(unique.size()); return new ArrayList<>(unique.values());
    }
    private void check() { if (canceled.getAsBoolean() || Thread.currentThread().isInterrupted()) throw new ClusterException("QUERY_CANCELED", "过程已取消"); }
    private void bounded(int size) { check(); if (size > 100000) throw new ClusterException("FRONTIER_EXCEEDED", "过程展开超过安全上限"); }
    private static Value other(Entity edge, Value vertex) { return edge.getSource().equals(vertex) ? edge.getTarget() : edge.getSource(); }
    private static Object required(Map<String, Object> args, String name) {
        Object value = args.get(name); if (value == null) throw new ClusterException("INVALID_ARGUMENT", "过程缺少参数：" + name); return value;
    }
    private static int integer(Map<String, Object> args, String name, int fallback) {
        Object value = args.getOrDefault(name, fallback); if (!(value instanceof Number number)) throw new ClusterException("TYPE_MISMATCH", "过程参数必须为整数：" + name); return Math.toIntExact(number.longValue());
    }
    private static List<String> columns(Stmt.Call call, String name) {
        List<String> supported = switch (name) {
            case "path.shortest" -> List.of("length", "vids");
            case "path.all" -> List.of("length", "path");
            case "ownership.largest_holder" -> List.of("vid", "ratio");
            case "subgraph.bounded" -> List.of("vid");
            default -> throw new ClusterException("SYNTAX", "未知过程：" + name);
        };
        if (call.yield().isEmpty()) return supported;
        if (!supported.containsAll(call.yield())) throw new ClusterException("SYNTAX", "过程 YIELD 列不合法");
        return call.yield();
    }
    private static Map<String, Object> entity(Entity entity) {
        if (entity == null) return Map.of();
        Map<String, Object> body = new LinkedHashMap<>(), properties = new LinkedHashMap<>();
        body.put("id", EntityCodec.object(entity.getId())); body.put("type", entity.getType());
        entity.getPropertiesMap().forEach((key, value) -> properties.put(key, EntityCodec.object(value))); body.put("properties", properties);
        if (!entity.getNode()) { body.put("src", EntityCodec.object(entity.getSource())); body.put("dst", EntityCodec.object(entity.getTarget())); }
        return body;
    }
}
