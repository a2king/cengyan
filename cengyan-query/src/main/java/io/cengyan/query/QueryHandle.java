package io.cengyan.query;

import io.cengyan.CancelToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一条正在执行的查询。超时、断连、手动终止都只调用 {@link #cancel(String)}。
 */
public final class QueryHandle implements CancelToken {
    private static final Logger log = LoggerFactory.getLogger(QueryHandle.class);

    private final String id;
    private final long deadlineNanos;
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final CopyOnWriteArrayList<Runnable> hooks = new CopyOnWriteArrayList<>();
    private volatile String reason = "";

    private QueryHandle(String id, long deadlineNanos) {
        this.id = id;
        this.deadlineNanos = deadlineNanos;
    }

    /** 从现在起 {@code timeoutMs} 毫秒后到期。 */
    public static QueryHandle start(long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs 不能为负");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        return new QueryHandle(UUID.randomUUID().toString(), deadline);
    }

    public String id() {
        return id;
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    public String reason() {
        return reason;
    }

    @Override
    public boolean cancelled() {
        return canceled.get();
    }

    /**
     * 置位取消标志并执行钩子。重复调用保留第一次的原因。
     */
    public void cancel(String reason) {
        if (!canceled.compareAndSet(false, true)) {
            return;
        }
        this.reason = reason == null ? "" : reason;
        for (Runnable hook : hooks) {
            runHook(hook);
        }
    }

    @Override
    public void onCancel(Runnable hook) {
        if (hook == null) {
            throw new IllegalArgumentException("取消钩子不能为空");
        }
        hooks.add(hook);
        if (canceled.get()) {
            runHook(hook);
        }
    }

    private static void runHook(Runnable hook) {
        try {
            hook.run();
        } catch (RuntimeException ex) {
            log.warn("取消钩子执行失败：{}", ex.toString());
        }
    }
}
