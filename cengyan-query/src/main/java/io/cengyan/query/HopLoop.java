package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.ReadSession;

import java.util.LinkedHashSet;
import java.util.Set;

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
        LinkedHashSet<Long> visited = new LinkedHashSet<>();
        visited.add(start);
        LinkedHashSet<Long> frontier = new LinkedHashSet<>();
        frontier.add(start);
        for (int hop = 1; hop <= hops; hop++) {
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
                        graphId, vid, edgeTypeId, direction, -1, maxEdgesPerVertex, cancel)) {
                    while (cursor.hasNext()) {
                        EdgeRef edge = cursor.next();
                        long other = edge.outSrc() == vid ? edge.outDst() : edge.outSrc();
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
            frontier = next;
        }
        return frontier;
    }
}
