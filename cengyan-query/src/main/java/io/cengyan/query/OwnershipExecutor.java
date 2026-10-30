package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.PropCodec;
import io.cengyan.catalog.RelType;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * 穿透持股。按当前路径做深度优先，不拼路径，也不受每点和每跳点数上限。
 * 环只挡住当前这条路径，另一条路径仍可以把比例加进同一个点。
 */
public final class OwnershipExecutor {
    private OwnershipExecutor() {
    }

    /** 一条出边和它的持股比例。 */
    public record Edge(long dst, double ratio) {
    }

    /** {@code best} 不含起点。没有可达点时 {@code vid} 为空。 */
    public record Result(Map<Long, Double> best, Long vid, Double ratio) {
    }

    /**
     * 用内存里的假边算。扫描器接上之前先用这张图对比例。
     */
    public static Result largest(
            Map<Long, List<Edge>> edges,
            long start,
            int hops,
            CancelToken cancel) {
        if (edges == null) {
            throw new IllegalArgumentException("边不能为空");
        }
        return compute(start, hops, cancel,
                vid -> edges.getOrDefault(vid, List.of()));
    }

    /** 从快照读出边。每点边数和每跳点数上限在这里关掉，取消仍每 64 条检查一次。 */
    public static Result largest(
            ReadSession session,
            int graphId,
            RelType type,
            String ratioProp,
            long start,
            int hops,
            CancelToken cancel) {
        if (session == null || type == null) {
            throw new IllegalArgumentException("快照和边类型不能为空");
        }
        return compute(start, hops, cancel,
                vid -> load(session, graphId, type, ratioProp, vid, cancel));
    }

    private static Result compute(
            long start,
            int hops,
            CancelToken cancel,
            LongFunction<List<Edge>> edges) {
        if (hops < 0) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数不能为负");
        }
        Map<Long, Double> best = new HashMap<>();
        ArrayDeque<Long> stack = new ArrayDeque<>();
        HashSet<Long> onStack = new HashSet<>();
        stack.push(start);
        onStack.add(start);
        dfs(start, 1.0d, 0, hops, edges, best, stack, onStack, cancel);
        best.remove(start);
        Long chosen = null;
        double chosenRatio = 0d;
        for (Map.Entry<Long, Double> entry : best.entrySet()) {
            double ratio = entry.getValue();
            if (chosen == null || ratio > chosenRatio || (ratio == chosenRatio && entry.getKey() < chosen)) {
                chosen = entry.getKey();
                chosenRatio = ratio;
            }
        }
        Map<Long, Double> frozen = Map.copyOf(best);
        if (chosen == null) {
            return new Result(frozen, null, null);
        }
        return new Result(frozen, chosen, chosenRatio);
    }

    private static void dfs(
            long vid,
            double accum,
            int depth,
            int hops,
            LongFunction<List<Edge>> edges,
            Map<Long, Double> best,
            ArrayDeque<Long> stack,
            HashSet<Long> onStack,
            CancelToken cancel) {
        if (cancel != null && cancel.cancelled()) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
        if (depth == hops) {
            return;
        }
        List<Edge> outgoing = edges.apply(vid);
        int seen = 0;
        for (Edge edge : outgoing) {
            if ((seen++ & 63) == 0 && cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            double nxt = accum * edge.ratio();
            if (onStack.contains(edge.dst())) {
                continue;
            }
            best.merge(edge.dst(), nxt, Double::sum);
            stack.push(edge.dst());
            onStack.add(edge.dst());
            dfs(edge.dst(), nxt, depth + 1, hops, edges, best, stack, onStack, cancel);
            stack.pop();
            onStack.remove(edge.dst());
        }
    }

    private static List<Edge> load(
            ReadSession session,
            int graphId,
            RelType type,
            String ratioProp,
            long vid,
            CancelToken cancel) {
        List<EdgeRef> refs = new ArrayList<>();
        try (EdgeCursor cursor = session.scanEdges(
                graphId, vid, type.id(), Direction.OUT, -1, -1, null, cancel)) {
            while (cursor.hasNext()) {
                refs.add(cursor.next());
            }
        }
        if (refs.isEmpty()) {
            return List.of();
        }
        List<byte[]> keys = new ArrayList<>(refs.size());
        for (EdgeRef edge : refs) {
            keys.add(KeyCodec.propKey(graphId, type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        List<byte[]> values = session.multiGet(Cf.PROP, keys);
        List<Edge> edges = new ArrayList<>(refs.size());
        for (int i = 0; i < refs.size(); i++) {
            Double ratio = ratio(values.get(i), type, ratioProp);
            if (ratio != null) {
                edges.add(new Edge(refs.get(i).outDst(), ratio));
            }
        }
        return edges;
    }

    private static Double ratio(byte[] raw, RelType type, String ratioProp) {
        if (raw == null) {
            return null;
        }
        Object value = PropCodec.decode(type.properties(), type.schemaVersion(), raw).get(ratioProp);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "比例必须是数字");
        }
        double ratio = number.doubleValue();
        if (!Double.isFinite(ratio)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "比例必须是有限数字");
        }
        return ratio;
    }
}
