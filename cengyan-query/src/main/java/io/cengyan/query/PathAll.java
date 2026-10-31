package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.ReadSession;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * 全部简单路径。一条路上的点不重复；下一条边如果回到这条路上，就丢掉这条边。
 * {@code end} 为空时返回跳数在范围内的每一条路。有终点时只返回正好停在终点上的路，并且不再往下走。
 */
public final class PathAll {
    private PathAll() {
    }

    /** {@code length} 是边数。{@code vids} 从起点排到当前终点，{@code edges} 与每一跳对齐。 */
    public record Found(int length, List<Long> vids, List<EdgeRef> edges) {
    }

    public static List<Found> find(
            ReadSession session,
            int graphId,
            Set<Integer> edgeTypeIds,
            List<Direction> directions,
            long start,
            Long end,
            int minHop,
            int maxHop,
            int maxHops,
            int maxEdgesPerVertex,
            int limit,
            LongPredicate rank,
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
        if (limit < 1) {
            throw new CengyanException(ErrorCode.SYNTAX, "路径条数至少为 1");
        }
        List<Found> found = new ArrayList<>();
        ArrayDeque<Frame> stack = new ArrayDeque<>();
        ArrayList<Long> vids = new ArrayList<>();
        ArrayList<EdgeRef> walked = new ArrayList<>();
        Set<Long> onPath = new HashSet<>();
        vids.add(start);
        onPath.add(start);
        stack.push(new Frame(start));
        while (!stack.isEmpty()) {
            cancelled(cancel);
            Frame frame = stack.peek();
            if (!frame.opened) {
                frame.opened = true;
                int depth = stack.size() - 1;
                if (depth >= minHop && (end == null || frame.vid == end)) {
                    if (found.size() == limit) {
                        throw new CengyanException(
                                ErrorCode.PATH_LIMIT_EXCEEDED,
                                "路径结果有 " + (limit + 1) + " 条，超过上限 " + limit);
                    }
                    found.add(new Found(depth, List.copyOf(vids), List.copyOf(walked)));
                }
                boolean terminal = depth == maxHop || (end != null && frame.vid == end);
                frame.edges = terminal
                        ? List.of()
                        : edgesOf(session, graphId, edgeTypeIds, directions, frame.vid, maxEdgesPerVertex, rank, cancel);
            }
            if (frame.index >= frame.edges.size()) {
                Frame done = stack.pop();
                onPath.remove(done.vid);
                vids.remove(vids.size() - 1);
                if (!walked.isEmpty()) {
                    walked.remove(walked.size() - 1);
                }
                continue;
            }
            EdgeRef edge = frame.edges.get(frame.index++);
            long next = edge.outSrc() == frame.vid ? edge.outDst() : edge.outSrc();
            if (!onPath.add(next)) {
                continue;
            }
            vids.add(next);
            walked.add(edge);
            stack.push(new Frame(next));
        }
        return found;
    }

    private static List<EdgeRef> edgesOf(
            ReadSession session,
            int graphId,
            Set<Integer> edgeTypeIds,
            List<Direction> directions,
            long vid,
            int maxEdgesPerVertex,
            LongPredicate rank,
            CancelToken cancel) {
        List<EdgeRef> edges = new ArrayList<>();
        try (EdgeCursor cursor = session.scanEdges(
                graphId, vid, edgeTypeIds, directions, -1, maxEdgesPerVertex,
                rank == null ? null : edge -> rank.test(edge.rank()), cancel)) {
            while (cursor.hasNext()) {
                edges.add(cursor.next());
            }
        } catch (CengyanException ex) {
            if (ex.code() == ErrorCode.TRUNCATED) {
                throw new CengyanException(ErrorCode.TRUNCATED, "起点 " + vid + "：" + ex.getMessage(), ex);
            }
            throw ex;
        }
        return edges;
    }

    private static void cancelled(CancelToken cancel) {
        if (cancel != null && cancel.cancelled()) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
    }

    private static final class Frame {
        private final long vid;
        private List<EdgeRef> edges = List.of();
        private int index;
        private boolean opened;

        private Frame(long vid) {
            this.vid = vid;
        }
    }
}
