package io.cengyan.server;

import io.cengyan.query.QueryHandle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 运行中的查询。超时线程每 100 毫秒扫一次到期的句柄。 */
final class QueryRegistry {
    private final ConcurrentMap<String, QueryHandle> running = new ConcurrentHashMap<>();

    void register(QueryHandle handle) {
        running.put(handle.id(), handle);
    }

    void unregister(String id) {
        running.remove(id);
    }

    boolean cancel(String queryId, String reason) {
        QueryHandle handle = running.get(queryId);
        if (handle == null) {
            return false;
        }
        handle.cancel(reason);
        return true;
    }

    /** 只返回带语句的执行。耗时按调用这一刻计算，长的排在前面。 */
    List<RunningQuery> list() {
        long now = System.currentTimeMillis();
        List<RunningQuery> rows = new ArrayList<>();
        for (QueryHandle handle : running.values()) {
            if (handle.statement().isBlank()) {
                continue;
            }
            rows.add(new RunningQuery(
                    handle.id(),
                    handle.user(),
                    Math.max(0, now - handle.startedMillis()),
                    handle.statement()));
        }
        rows.sort(Comparator.comparingLong(RunningQuery::elapsedMs).reversed());
        return rows;
    }

    void sweep() {
        try {
            long now = System.nanoTime();
            for (QueryHandle handle : running.values()) {
                if (now - handle.deadlineNanos() >= 0) {
                    handle.cancel("超时");
                }
            }
        } catch (RuntimeException ex) {
            // 定时任务抛出去会被调度器停掉，这里吞掉只记在调用方日志里。
            throw ex;
        }
    }
}
