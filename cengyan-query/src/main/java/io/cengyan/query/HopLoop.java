package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.ReadSession;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * 普通多跳。全局 visited，点进过就不再进。超过每跳点数或每点边数就失败，不把截断当成成功。
 */
public final class HopLoop {
    private HopLoop() {
    }

    public static Set<Long> expand(
            ReadSession session,
            int graphId,
            int edgeTypeId,
            Direction direction,
            long start,
            int hops,
            int maxHops,
            int maxEdgesPerVertex,
            int maxFrontierPerHop,
            CancelToken cancel) {
        if (session == null) {
            throw new IllegalArgumentException("快照不能为空");
        }
        if (hops < 1) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数至少为 1");
        }
        if (hops > maxHops) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数 " + hops + " 超过上限 " + maxHops);
        }
        return expandRange(
                session, graphId, edgeTypeId, List.of(direction), start, hops, hops,
                maxHops, maxEdgesPerVertex, maxFrontierPerHop, null, null, cancel);
    }

    /**
     * {@code minHop} 到 {@code maxHop} 新到达的点。定长时两者相等，结果就是那一跳的 frontier。
     * 点一旦进入 visited 就不再进入后面的跳。
     */
    public static Set<Long> expandRange(
            ReadSession session,
            int graphId,
            int edgeTypeId,
            List<Direction> directions,
            long start,
            int minHop,
            int maxHop,
            int maxHops,
            int maxEdgesPerVertex,
            int maxFrontierPerHop,
            LongPredicate rank,
            LongPredicate neighbor,
            CancelToken cancel) {
        if (session == null) {
            throw new IllegalArgumentException("快照不能为空");
        }
        if (directions == null || directions.isEmpty()) {
            throw new IllegalArgumentException("方向不能为空");
        }
        if (minHop < 1 || maxHop < minHop) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数范围不正确");
        }
        if (maxHop > maxHops) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数 " + maxHop + " 超过上限 " + maxHops);
        }
        LinkedHashSet<Long> visited = new LinkedHashSet<>();
        visited.add(start);
        LinkedHashSet<Long> frontier = new LinkedHashSet<>();
        frontier.add(start);
        LinkedHashSet<Long> reached = new LinkedHashSet<>();
        for (int hop = 1; hop <= maxHop; hop++) {
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            if (frontier.size() > maxFrontierPerHop) {
                throw new CengyanException(
                        ErrorCode.FRONTIER_EXCEEDED,
                        "第 " + hop + " 跳的起点集合大小为 " + frontier.size() + "，超过上限 " + maxFrontierPerHop);
            }
            LinkedHashSet<Long> next = new LinkedHashSet<>();
            for (long vid : frontier) {
                for (Direction direction : directions) {
                    try (EdgeCursor cursor = session.scanEdges(
                            graphId, vid, edgeTypeId, direction, -1, maxEdgesPerVertex,
                            edge -> rankOk(rank, edge.rank()) && neighborOk(neighbor, other(edge, vid)),
                            cancel)) {
                        while (cursor.hasNext()) {
                            EdgeRef edge = cursor.next();
                            long other = other(edge, vid);
                            if (!visited.add(other)) {
                                continue;
                            }
                            next.add(other);
                            if (next.size() > maxFrontierPerHop) {
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
            }
            if (hop >= minHop) {
                reached.addAll(next);
            }
            frontier = next;
        }
        return reached;
    }

    /**
     * 和 {@link #expandRange} 一样，但一条边上可以是多种类型。{@code edgeTypeIds} 为空表示任何类型。
     * 每个起点只扫一次，两个方向放在同一次扫描里。
     */
    public static Set<Long> expandTypes(
            ReadSession session,
            int graphId,
            Set<Integer> edgeTypeIds,
            List<Direction> directions,
            long start,
            int minHop,
            int maxHop,
            int maxHops,
            int maxEdgesPerVertex,
            int maxFrontierPerHop,
            LongPredicate rank,
            LongPredicate neighbor,
            CancelToken cancel) {
        if (session == null) {
            throw new IllegalArgumentException("快照不能为空");
        }
        if (directions == null || directions.isEmpty()) {
            throw new IllegalArgumentException("方向不能为空");
        }
        if (minHop < 1 || maxHop < minHop) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数范围不正确");
        }
        if (maxHop > maxHops) {
            throw new CengyanException(ErrorCode.SYNTAX, "跳数 " + maxHop + " 超过上限 " + maxHops);
        }
        LinkedHashSet<Long> visited = new LinkedHashSet<>();
        visited.add(start);
        LinkedHashSet<Long> frontier = new LinkedHashSet<>();
        frontier.add(start);
        LinkedHashSet<Long> reached = new LinkedHashSet<>();
        for (int hop = 1; hop <= maxHop; hop++) {
            if (cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            if (frontier.size() > maxFrontierPerHop) {
                throw new CengyanException(
                        ErrorCode.FRONTIER_EXCEEDED,
                        "第 " + hop + " 跳的起点集合大小为 " + frontier.size() + "，超过上限 " + maxFrontierPerHop);
            }
            LinkedHashSet<Long> next = new LinkedHashSet<>();
            for (long vid : frontier) {
                try (EdgeCursor cursor = session.scanEdges(
                        graphId, vid, edgeTypeIds, directions, -1, maxEdgesPerVertex,
                        edge -> rankOk(rank, edge.rank()) && neighborOk(neighbor, otherEnd(edge, vid)),
                        cancel)) {
                    while (cursor.hasNext()) {
                        EdgeRef edge = cursor.next();
                        long other = otherEnd(edge, vid);
                        if (!visited.add(other)) {
                            continue;
                        }
                        next.add(other);
                        if (next.size() > maxFrontierPerHop) {
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
            if (hop >= minHop) {
                reached.addAll(next);
            }
            frontier = next;
        }
        return reached;
    }

    private static long otherEnd(EdgeRef edge, long vid) {
        return edge.outSrc() == vid ? edge.outDst() : edge.outSrc();
    }

    private static long other(EdgeRef edge, long vid) {
        return edge.outSrc() == vid ? edge.outDst() : edge.outSrc();
    }

    private static boolean rankOk(LongPredicate rank, long value) {
        return rank == null || rank.test(value);
    }

    private static boolean neighborOk(LongPredicate neighbor, long value) {
        return neighbor == null || neighbor.test(value);
    }
}
