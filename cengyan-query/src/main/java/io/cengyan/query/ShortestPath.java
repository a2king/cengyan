package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.ReadSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 两点之间的最短路。从起点沿出边、从终点沿入边交替扩展。
 * 每一侧都受每点边数和每跳点数上限。跳数用完还没相遇就返回空，不改成全图搜索。
 */
public final class ShortestPath {
    private ShortestPath() {
    }

    /** 一条最短路。{@code length} 是边数，{@code vids} 从起点排到终点。 */
    public record Found(int length, List<Long> vids) {
    }

    public static Found find(
            ReadSession session,
            int graphId,
            int edgeTypeId,
            long start,
            long end,
            int hops,
            int maxHops,
            int maxEdgesPerVertex,
            int maxFrontierPerHop,
            CancelToken cancel) {
        if (session == null) {
            throw new IllegalArgumentException("快照不能为空");
        }
        if (hops < 0) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数不能为负");
        }
        if (hops > maxHops) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数 " + hops + " 超过上限 " + maxHops);
        }
        if (start == end) {
            return new Found(0, List.of(start));
        }
        Side forward = new Side(start);
        Side backward = new Side(end);
        int depthStart = 0;
        int depthEnd = 0;
        boolean expandStart = true;
        while (depthStart + depthEnd < hops) {
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            Found found = expandStart
                    ? expand(session, graphId, edgeTypeId, forward, backward, true,
                    depthStart + 1, maxEdgesPerVertex, maxFrontierPerHop, cancel)
                    : expand(session, graphId, edgeTypeId, backward, forward, false,
                    depthEnd + 1, maxEdgesPerVertex, maxFrontierPerHop, cancel);
            if (found != null) {
                return found;
            }
            if (expandStart) {
                depthStart++;
            } else {
                depthEnd++;
            }
            expandStart = !expandStart;
        }
        return null;
    }

    private static Found expand(
            ReadSession session,
            int graphId,
            int edgeTypeId,
            Side side,
            Side other,
            boolean fromStart,
            int hop,
            int maxEdgesPerVertex,
            int maxFrontierPerHop,
            CancelToken cancel) {
        if (side.frontier.isEmpty()) {
            return null;
        }
        if (side.frontier.size() > maxFrontierPerHop) {
            throw new CengyanException(
                    ErrorCode.FRONTIER_EXCEEDED,
                    "第 " + hop + " 跳的起点集合大小为 " + side.frontier.size() + "，超过上限 " + maxFrontierPerHop);
        }
        List<Long> layer = new ArrayList<>(side.frontier);
        side.frontier.clear();
        Direction direction = fromStart ? Direction.OUT : Direction.IN;
        Found best = null;
        for (long vid : layer) {
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            try (EdgeCursor cursor = session.scanEdges(
                    graphId, vid, edgeTypeId, direction, -1, maxEdgesPerVertex, null, cancel)) {
                while (cursor.hasNext()) {
                    EdgeRef edge = cursor.next();
                    long neighbor = fromStart ? edge.outDst() : edge.outSrc();
                    if (side.depth.containsKey(neighbor)) {
                        continue;
                    }
                    if (other.depth.containsKey(neighbor)) {
                        long startVertex = fromStart ? vid : neighbor;
                        long endVertex = fromStart ? neighbor : vid;
                        Found candidate = path(side, other, fromStart, startVertex, endVertex);
                        if (best == null || candidate.length() < best.length()) {
                            best = candidate;
                        }
                        continue;
                    }
                    side.parent.put(neighbor, vid);
                    side.depth.put(neighbor, side.depth.get(vid) + 1);
                    side.frontier.add(neighbor);
                    if (side.frontier.size() > maxFrontierPerHop) {
                        throw new CengyanException(
                                ErrorCode.FRONTIER_EXCEEDED,
                                "第 " + hop + " 跳起点 " + vid + " 的新点超过上限 " + maxFrontierPerHop);
                    }
                }
            } catch (CengyanException ex) {
                if (ex.code() == ErrorCode.TRUNCATED) {
                    throw new CengyanException(
                            ErrorCode.TRUNCATED,
                            "第 " + hop + " 跳起点 " + vid + "：" + ex.getMessage(),
                            ex);
                }
                throw ex;
            }
        }
        return best;
    }

    private static Found path(Side expanded, Side other, boolean fromStart, long startVertex, long endVertex) {
        Side startSide = fromStart ? expanded : other;
        Side endSide = fromStart ? other : expanded;
        List<Long> left = fromOrigin(startSide, startVertex);
        List<Long> right = fromOrigin(endSide, endVertex);
        Collections.reverse(right);
        List<Long> vids = new ArrayList<>(left.size() + right.size());
        vids.addAll(left);
        vids.addAll(right);
        return new Found(vids.size() - 1, List.copyOf(vids));
    }

    /** 从这一侧的根走到 {@code vid}。父指针指向搜索起点。 */
    private static List<Long> fromOrigin(Side side, long vid) {
        List<Long> chain = new ArrayList<>();
        long cursor = vid;
        chain.add(cursor);
        while (side.depth.get(cursor) != 0) {
            Long previous = side.parent.get(cursor);
            if (previous == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "最短路的父指针断了");
            }
            cursor = previous;
            chain.add(cursor);
        }
        Collections.reverse(chain);
        return chain;
    }

    private static final class Side {
        private final Map<Long, Long> parent = new HashMap<>();
        private final Map<Long, Integer> depth = new HashMap<>();
        private final List<Long> frontier = new ArrayList<>();

        private Side(long origin) {
            depth.put(origin, 0);
            frontier.add(origin);
        }
    }
}
