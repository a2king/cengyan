package io.cengyan.store;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * 按边类型把多次扫描接起来。每点上限和语句上限在整条链上共用。
 */
final class ChainEdgeCursor implements EdgeCursor {
    @FunctionalInterface
    interface Opener {
        EdgeCursor open(int examined, int matched);
    }

    private final List<Opener> openers;
    private final int queryLimit;
    private final int maxEdgesPerVertex;
    private final long vid;
    private int nextOpener;
    private int examined;
    private int matched;
    private EdgeCursor current;

    ChainEdgeCursor(List<Opener> openers, int queryLimit, int maxEdgesPerVertex, long vid) {
        this.openers = openers;
        this.queryLimit = queryLimit;
        this.maxEdgesPerVertex = maxEdgesPerVertex;
        this.vid = vid;
    }

    static EdgeCursor empty() {
        return new ChainEdgeCursor(List.of(), 0, -1, 0L);
    }

    @Override
    public synchronized boolean hasNext() {
        while (true) {
            if (current != null) {
                if (current.hasNext()) {
                    return true;
                }
                examined += current.examined();
                current.close();
                current = null;
            }
            if (queryLimit >= 0 && matched >= queryLimit) {
                return false;
            }
            if (maxEdgesPerVertex >= 0 && examined >= maxEdgesPerVertex) {
                throw new CengyanException(
                        ErrorCode.TRUNCATED,
                        "点 " + vid + " 已检查 " + examined + " 条边，超过每点上限 " + maxEdgesPerVertex);
            }
            if (nextOpener >= openers.size()) {
                return false;
            }
            EdgeCursor opened = openers.get(nextOpener++).open(examined, matched);
            if (opened == null) {
                continue;
            }
            current = opened;
        }
    }

    @Override
    public synchronized EdgeRef next() {
        if (!hasNext()) {
            throw new NoSuchElementException("没有下一条边");
        }
        matched++;
        return current.next();
    }

    @Override
    public synchronized int examined() {
        return examined + (current == null ? 0 : current.examined());
    }

    @Override
    public synchronized void close() {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
