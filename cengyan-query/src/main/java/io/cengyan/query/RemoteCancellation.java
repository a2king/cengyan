package io.cengyan.query;

import io.cengyan.CancelToken;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** 将入口取消/硬截止时间接到现有 DuckDB Statement.cancel 钩子。 */
final class RemoteCancellation implements CancelToken, AutoCloseable {
    private static final ScheduledExecutorService WATCHER = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "remote-query-cancellation"); thread.setDaemon(true); return thread;
    });
    private final BooleanSupplier condition;
    private final Set<Runnable> hooks = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final ScheduledFuture<?> watch;
    RemoteCancellation(BooleanSupplier condition) {
        this.condition = condition;
        watch = WATCHER.scheduleWithFixedDelay(this::check, 50, 50, TimeUnit.MILLISECONDS);
    }
    private void check() {
        boolean requested;
        try { requested = condition.getAsBoolean(); } catch (RuntimeException ex) { requested = true; }
        if (requested && canceled.compareAndSet(false, true)) {
            for (Runnable hook : hooks) {
                try { hook.run(); } catch (RuntimeException ignored) { }
            }
            hooks.clear();
        }
    }
    @Override public boolean cancelled() { check(); return canceled.get(); }
    @Override public void onCancel(Runnable hook) {
        hooks.add(hook);
        if (cancelled() && hooks.remove(hook)) hook.run();
    }
    @Override public void forget(Runnable hook) { hooks.remove(hook); }
    @Override public void close() { watch.cancel(false); hooks.clear(); }
}
