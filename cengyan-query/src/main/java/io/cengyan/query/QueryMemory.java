package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.util.List;
import java.util.Map;

/**
 * 查询级的托管内存预算。
 *
 * <p>Java 不提供按查询回收/限制整个 JVM 堆的能力，因此这里统计查询执行器持有的
 * 行、值和中间结果估算值。DuckDB 的排序/聚合同时使用同一个预算作为 memory_limit，
 * 超出后由 DuckDB 写入临时目录，而不是继续无限扩大进程内存。</p>
 */
final class QueryMemory {
    private static final ThreadLocal<QueryMemory> CURRENT = new ThreadLocal<>();

    private final long limit;
    private long observed;

    private QueryMemory(long limit) {
        this.limit = limit <= 0 ? Long.MAX_VALUE : limit;
    }

    static Scope open(long limit) {
        QueryMemory previous = CURRENT.get();
        QueryMemory current = new QueryMemory(limit);
        CURRENT.set(current);
        return new Scope(previous);
    }

    static QueryMemory current() {
        QueryMemory found = CURRENT.get();
        return found == null ? UNLIMITED : found;
    }

    private static final QueryMemory UNLIMITED = new QueryMemory(Long.MAX_VALUE);

    long limit() {
        return limit == Long.MAX_VALUE ? 0L : limit;
    }

    /** 以峰值方式观察一批行，避免同一批数据在多个阶段被重复计费。 */
    void observeRows(List<? extends List<?>> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        long estimate = 32L;
        for (List<?> row : rows) {
            estimate = add(estimate, 32L);
            if (row == null) {
                continue;
            }
            for (Object value : row) {
                estimate = add(estimate, estimateValue(value, 0));
            }
        }
        observe(estimate);
    }

    void observeRows(int rowCount, int width) {
        if (rowCount <= 0 || width <= 0) {
            return;
        }
        long perRow = add(32L, (long) width * 24L);
        observe(add(32L, multiply(rowCount, perRow)));
    }

    void observe(Object value) {
        observe(estimateValue(value, 0));
    }

    void observe(long bytes) {
        if (bytes <= observed) {
            return;
        }
        if (bytes > limit) {
            throw new CengyanException(
                    ErrorCode.QUERY_MEMORY_EXCEEDED,
                    "查询托管内存预算为 " + limit + " 字节，估算需要 " + bytes + " 字节");
        }
        observed = bytes;
    }

    private static long estimateValue(Object value, int depth) {
        if (value == null) {
            return 8L;
        }
        if (depth > 3) {
            return 64L;
        }
        if (value instanceof CharSequence text) {
            return add(24L, text.length() * 2L);
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return 24L;
        }
        if (value instanceof List<?> list) {
            long total = 32L;
            for (Object child : list) {
                total = add(total, estimateValue(child, depth + 1));
            }
            return total;
        }
        if (value instanceof Map<?, ?> map) {
            long total = 48L;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                total = add(total, estimateValue(entry.getKey(), depth + 1));
                total = add(total, estimateValue(entry.getValue(), depth + 1));
            }
            return total;
        }
        return 64L;
    }

    private static long add(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long multiply(long left, long right) {
        if (left <= 0 || right <= 0 || left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    static final class Scope implements AutoCloseable {
        private final QueryMemory previous;

        private Scope(QueryMemory previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
