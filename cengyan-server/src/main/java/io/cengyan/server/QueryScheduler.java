package io.cengyan.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 把一条语句送进对应的线程池。
 * 保留名额只给登录和元数据，重查询用单独的池。
 * 同一会话同时只跑一条语句，排队发生在领线程之前。
 */
final class QueryScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(QueryScheduler.class);

    private static final Set<String> METADATA = Set.of("SHOW", "USE", "DESCRIBE");
    private static final Set<String> HEAVY_ADMIN = Set.of(
            "stats",
            "storage",
            "rebuildIndex",
            "flush",
            "compact",
            "addReplica",
            "addStorage",
            "drainStorage",
            "removeStorage",
            "setPartCount",
            "addCompute",
            "drainCompute",
            "removeCompute");

    enum Lane {
        RESERVED,
        GENERAL,
        HEAVY
    }

    @FunctionalInterface
    interface Busy {
        void rejected(String message);
    }

    private final long queueMs;
    private final int reservedSize;
    private final ExecutorService admission;
    private final ExecutorService interactivePool;
    private final ExecutorService heavyPool;
    private final Semaphore reserved;
    private final Semaphore general;
    private final Semaphore heavy;
    private final ConcurrentMap<String, ReentrantLock> turns = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private QueryScheduler(
            long queueMs,
            int reservedSize,
            ExecutorService admission,
            ExecutorService interactivePool,
            ExecutorService heavyPool,
            Semaphore reserved,
            Semaphore general,
            Semaphore heavy) {
        this.queueMs = queueMs;
        this.reservedSize = reservedSize;
        this.admission = admission;
        this.interactivePool = interactivePool;
        this.heavyPool = heavyPool;
        this.reserved = reserved;
        this.general = general;
        this.heavy = heavy;
    }

    static QueryScheduler open(ServerConfig config) {
        int reservedSize = config.interactiveReserved();
        int generalSize = config.queryThreads() - reservedSize;
        ExecutorService interactivePool = Executors.newFixedThreadPool(config.queryThreads(), runnable -> {
            Thread thread = new Thread(runnable, "cengyan-query");
            thread.setDaemon(false);
            return thread;
        });
        ExecutorService heavyPool = Executors.newFixedThreadPool(config.heavyQueryThreads(), runnable -> {
            Thread thread = new Thread(runnable, "cengyan-heavy");
            thread.setDaemon(false);
            return thread;
        });
        ExecutorService admission = Executors.newVirtualThreadPerTaskExecutor();
        log.info(
                "查询准入：交互线程 {}（保留 {}），重查询线程 {}，排队 {} 毫秒",
                config.queryThreads(),
                reservedSize,
                config.heavyQueryThreads(),
                config.queryQueueMs());
        return new QueryScheduler(
                config.queryQueueMs(),
                reservedSize,
                admission,
                interactivePool,
                heavyPool,
                new Semaphore(reservedSize, true),
                new Semaphore(generalSize, true),
                new Semaphore(config.heavyQueryThreads(), true));
    }

    static Lane laneForQuery(String query) {
        String keyword = firstKeyword(query);
        if (METADATA.contains(keyword)) {
            return Lane.RESERVED;
        }
        return Lane.GENERAL;
    }

    static Lane laneForAdmin(String command) {
        if (command != null && HEAVY_ADMIN.contains(command)) {
            return Lane.HEAVY;
        }
        return Lane.GENERAL;
    }

    /** 重查询超时。{@code capMs <= 0} 表示不额外封顶。请求为 0 时用封顶值。 */
    static long capTimeout(long requestedMs, long capMs) {
        if (capMs <= 0) {
            return requestedMs;
        }
        if (requestedMs <= 0) {
            return capMs;
        }
        return Math.min(requestedMs, capMs);
    }

    /**
     * @param sessionWaitMs 等同会话上一条语句还没结束时，最多再等多久。这段等待不占查询线程。
     */
    void submit(Lane lane, String sessionId, long sessionWaitMs, Runnable task, Busy busy) {
        if (closed.get()) {
            reject(busy, "查询线程已满");
            return;
        }
        try {
            admission.execute(() -> admit(lane, sessionId, Math.max(0, sessionWaitMs), task, busy));
        } catch (RejectedExecutionException ex) {
            reject(busy, "查询线程已满");
        }
    }

    private void admit(Lane lane, String sessionId, long sessionWaitMs, Runnable task, Busy busy) {
        Lane effective = effective(lane);
        ReentrantLock turn = turns.computeIfAbsent(sessionId, id -> new ReentrantLock(true));
        boolean locked = false;
        boolean permitted = false;
        Semaphore permits = permits(effective);
        try {
            locked = turn.tryLock(sessionWaitMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                reject(busy, "会话正忙");
                return;
            }
            permitted = permits.tryAcquire(queueMs, TimeUnit.MILLISECONDS);
            if (!permitted) {
                reject(busy, "查询线程已满");
                return;
            }
            CompletableFuture<Void> done = new CompletableFuture<>();
            ExecutorService pool = effective == Lane.HEAVY ? heavyPool : interactivePool;
            try {
                pool.execute(() -> {
                    try {
                        task.run();
                        done.complete(null);
                    } catch (Throwable ex) {
                        done.completeExceptionally(ex);
                    }
                });
            } catch (RejectedExecutionException ex) {
                reject(busy, "查询线程已满");
                return;
            }
            try {
                done.join();
            } catch (CompletionException ex) {
                log.warn("查询线程异常退出：{}", ex.getCause() == null ? ex.toString() : ex.getCause().toString());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            reject(busy, "查询线程已满");
        } finally {
            if (permitted) {
                permits.release();
            }
            if (locked) {
                turn.unlock();
            }
        }
    }

    private Lane effective(Lane lane) {
        if (lane == Lane.RESERVED && reservedSize == 0) {
            return Lane.GENERAL;
        }
        return lane;
    }

    private Semaphore permits(Lane lane) {
        return switch (lane) {
            case RESERVED -> reserved;
            case GENERAL -> general;
            case HEAVY -> heavy;
        };
    }

    private static void reject(Busy busy, String message) {
        try {
            busy.rejected(message);
        } catch (RuntimeException ex) {
            log.warn("拒绝查询失败：{}", ex.toString());
        }
    }

    static String firstKeyword(String query) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        int i = 0;
        int n = query.length();
        while (i < n) {
            while (i < n && Character.isWhitespace(query.charAt(i))) {
                i++;
            }
            if (i + 1 < n && query.charAt(i) == '-' && query.charAt(i + 1) == '-') {
                while (i < n && query.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (i + 1 < n && query.charAt(i) == '/' && query.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < n && !(query.charAt(i) == '*' && query.charAt(i + 1) == '/')) {
                    i++;
                }
                i = Math.min(n, i + 2);
                continue;
            }
            break;
        }
        int start = i;
        while (i < n && (Character.isLetter(query.charAt(i)) || query.charAt(i) == '_')) {
            i++;
        }
        return query.substring(start, i).toUpperCase(Locale.ROOT);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        admission.shutdownNow();
        interactivePool.shutdownNow();
        heavyPool.shutdownNow();
    }
}
