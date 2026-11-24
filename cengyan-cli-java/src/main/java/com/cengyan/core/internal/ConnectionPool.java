package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.exception.ConnectionException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 按地址的连接池。一条连接同一时间只借给一个请求；不阻塞：空闲连接用完时新建，
 * 归还时每个地址最多保留 {@code maxIdlePerAddress} 条空闲连接，多余的直接关闭。
 *
 * <p>出过错、超时或被关闭的连接不会回到池里；空闲超过 {@code idleTimeoutMillis} 的连接在下次借用时关闭。
 * 锁只保护登记表，不覆盖网络往返。
 */
public final class ConnectionPool {
    private static final class Idle {
        final Connection connection;
        final long since;

        Idle(Connection connection, long since) {
            this.connection = connection;
            this.since = since;
        }
    }

    private final Connection.Factory factory;
    private final int maxIdlePerAddress;
    private final long idleTimeoutNanos;
    private final Map<Address, Deque<Idle>> idle = new HashMap<Address, Deque<Idle>>();
    private final Set<Connection> borrowed = Collections.newSetFromMap(new IdentityHashMap<Connection, Boolean>());
    /** null 表示所有地址都有效；地址刷新后只保留新地址的连接。 */
    private Set<Address> valid;
    private boolean closed;

    public ConnectionPool(Connection.Factory factory, int maxIdlePerAddress, long idleTimeoutMillis) {
        if (maxIdlePerAddress < 0) throw new IllegalArgumentException("maxIdlePerAddress 不能为负数");
        if (idleTimeoutMillis <= 0) throw new IllegalArgumentException("idleTimeout 必须为正");
        this.factory = factory;
        this.maxIdlePerAddress = maxIdlePerAddress;
        this.idleTimeoutNanos = idleTimeoutMillis * 1_000_000L;
    }

    /** 借一条到 address 的连接（可能尚未建连）。池已关闭时抛 {@link ConnectionException}。 */
    public Connection borrow(Address address) {
        List<Connection> expired = new ArrayList<Connection>();
        Connection chosen = null;
        synchronized (this) {
            if (closed) throw new ConnectionException("连接池已关闭", "CONNECTION_ERROR", null);
            Deque<Idle> queue = idle.get(address);
            long now = System.nanoTime();
            while (queue != null && !queue.isEmpty()) {
                Idle candidate = queue.pollFirst();
                if (now - candidate.since < idleTimeoutNanos && candidate.connection.reusable()) {
                    chosen = candidate.connection;
                    break;
                }
                expired.add(candidate.connection);
            }
            if (chosen != null) borrowed.add(chosen);
        }
        closeAll(expired);
        if (chosen != null) return chosen;
        Connection created = factory.open(address);
        synchronized (this) {
            if (!closed) {
                borrowed.add(created);
                return created;
            }
        }
        created.close();
        throw new ConnectionException("连接池已关闭", "CONNECTION_ERROR", null);
    }

    /** 归还连接。可复用的放回池中（LIFO，最近用过的先借出），其余关闭。 */
    public void release(Connection connection) {
        boolean keep = false;
        synchronized (this) {
            boolean mine = borrowed.remove(connection);
            Address address = connection.address();
            if (mine && !closed && connection.reusable() && (valid == null || valid.contains(address))) {
                Deque<Idle> queue = idle.get(address);
                if (queue == null) {
                    queue = new ArrayDeque<Idle>();
                    idle.put(address, queue);
                }
                if (queue.size() < maxIdlePerAddress) {
                    queue.addFirst(new Idle(connection, System.nanoTime()));
                    keep = true;
                }
            }
        }
        if (!keep) connection.close();
    }

    /** 只保留这些地址的连接：其余地址的空闲连接立即关闭，借出的在归还时关闭。 */
    public void retainOnly(Collection<Address> addresses) {
        List<Connection> stale = new ArrayList<Connection>();
        synchronized (this) {
            valid = new HashSet<Address>(addresses);
            for (Iterator<Map.Entry<Address, Deque<Idle>>> it = idle.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Address, Deque<Idle>> entry = it.next();
                if (!valid.contains(entry.getKey())) {
                    for (Idle item : entry.getValue()) stale.add(item.connection);
                    it.remove();
                }
            }
        }
        closeAll(stale);
    }

    /** 关闭全部连接，包括正在使用的（进行中的请求会以连接错误结束）。 */
    public void close() {
        List<Connection> all = new ArrayList<Connection>();
        synchronized (this) {
            if (closed) return;
            closed = true;
            for (Deque<Idle> queue : idle.values()) for (Idle item : queue) all.add(item.connection);
            idle.clear();
            all.addAll(borrowed);
            borrowed.clear();
        }
        closeAll(all);
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    /** 当前空闲连接数（测试与诊断用）。 */
    public synchronized int idleCount() {
        int count = 0;
        for (Deque<Idle> queue : idle.values()) count += queue.size();
        return count;
    }

    /** 当前借出的连接数（测试与诊断用）。 */
    public synchronized int borrowedCount() {
        return borrowed.size();
    }

    private static void closeAll(List<Connection> connections) {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (RuntimeException ignored) {
                // 关闭失败不影响其他连接
            }
        }
    }
}
