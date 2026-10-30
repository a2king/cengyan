package io.cengyan.server;

import io.cengyan.query.SessionState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存里的会话，按会话号查找，不绑在某条 TCP 上。
 * 连接断开后仍然保留，直到收到 logout，或空闲超过 8 小时。
 */
final class Sessions {
    private static final long IDLE_NANOS = TimeUnit.HOURS.toNanos(8);

    private final ConcurrentMap<String, Entry> sessions = new ConcurrentHashMap<>();

    SessionState open(String id) {
        Entry entry = sessions.computeIfAbsent(id, ignored -> new Entry(new SessionState()));
        entry.lastAccess.set(System.nanoTime());
        return entry.state;
    }

    void close(String id) {
        if (id != null && !id.isBlank()) {
            sessions.remove(id);
        }
    }

    void sweep() {
        long now = System.nanoTime();
        sessions.entrySet().removeIf(entry -> now - entry.getValue().lastAccess.get() >= IDLE_NANOS);
    }

    private static final class Entry {
        private final SessionState state;
        private final AtomicLong lastAccess = new AtomicLong();

        private Entry(SessionState state) {
            this.state = state;
        }
    }
}
