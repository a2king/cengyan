package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.exception.ConnectionException;
import com.cengyan.core.exception.ReadTimeoutException;
import com.cengyan.core.exception.TransportException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 分布式 Compute 的多地址传输，语义与 pycengyan 的 {@code ComputeTransport} 一致：
 * <ul>
 *   <li>不自动判断 GQL 是读还是写，由调用方显式传 {@code write}；</li>
 *   <li>写请求断线或读超时时，用同一 requestId 在其他节点重试，首次发送加最多 3 次重试；
 *       超过原始截止时间或重试用尽时返回 {@code code=FAILED}（提交状态未知），不抛异常；</li>
 *   <li>读请求断线不重发，但默认入口会离开失败的节点；</li>
 *   <li>服务端业务错误原样返回，不重试；</li>
 *   <li>指定地址失败只影响本次请求，不挪动默认入口。</li>
 * </ul>
 * 线程安全：锁只保护地址表，网络往返在锁外。
 */
public final class ComputeTransport implements Transport {
    private static final Logger LOG = Logger.getLogger("com.cengyan.core");
    /** 首次发送 + 最多三次自动重试。 */
    static final int WRITE_ATTEMPTS = 4;

    private final long timeoutMillis;
    private final ConnectionPool pool;
    private final Random random;
    private final Object lock = new Object();
    private List<Address> addresses;
    private int index;
    private volatile boolean closed;

    public ComputeTransport(List<Address> addresses, long timeoutMillis, ConnectionPool pool, Random random) {
        List<Address> values = validate(addresses, "至少配置一个计算节点地址");
        if (values.size() != addresses.size()) throw new IllegalArgumentException("计算节点地址不能重复");
        if (timeoutMillis <= 0) throw new IllegalArgumentException("Compute 请求必须有正数超时");
        this.addresses = values;
        this.timeoutMillis = timeoutMillis;
        this.pool = pool;
        this.random = random;
        this.index = random.nextInt(values.size());
    }

    private static List<Address> validate(List<Address> addresses, String emptyMessage) {
        if (addresses == null || addresses.isEmpty()) throw new IllegalArgumentException(emptyMessage);
        LinkedHashSet<Address> unique = new LinkedHashSet<Address>();
        for (Address address : addresses) {
            if (address == null) throw new IllegalArgumentException("计算节点地址不能为 null");
            unique.add(address);
        }
        return Collections.unmodifiableList(new ArrayList<Address>(unique));
    }

    /** 当前默认入口。 */
    public Address address() {
        synchronized (lock) {
            return addresses.get(index);
        }
    }

    public List<Address> addresses() {
        synchronized (lock) {
            return addresses;
        }
    }

    public ConnectionPool pool() {
        return pool;
    }

    @Override
    public Map<String, Object> request(Map<String, Object> payload, Long timeout, boolean write, Address preferred) {
        if (closed) throw new ConnectionException("Compute 传输已关闭", "CONNECTION_ERROR", null);
        long budget = timeout == null ? timeoutMillis : timeout;
        if (budget <= 0) throw new IllegalArgumentException("Compute 请求必须有正数超时");
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budget);
        // 只改顶层 requestId / remainingTimeoutMs，浅拷贝即可；批量写入的 rows 不再整份复制。
        Map<String, Object> request = new LinkedHashMap<String, Object>(payload);
        int attempts = 1;
        if (write) {
            Object id = request.get("requestId");
            if (id == null || String.valueOf(id).isEmpty()) request.put("requestId", UUID.randomUUID().toString());
            attempts = WRITE_ATTEMPTS;
        }
        Address target = preferred;
        for (int attempt = 0; ; attempt++) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0) {
                if (write) return uncertain(request, "写请求已到达原始截止时间，提交状态无法确认，数据可能已经提交", null);
                throw new ReadTimeoutException("请求已到达截止时间", "READ_TIMEOUT", null);
            }
            Address current = target != null ? target : address();
            request.put("remainingTimeoutMs", Math.max(1L, remaining));
            Connection connection;
            try {
                connection = pool.borrow(current);
            } catch (ConnectionException error) {
                // 只有池已关闭才会走到这里。之前的尝试可能已送达，写请求按提交状态未知返回。
                if (write && attempt > 0) return uncertain(request, "写请求传输已关闭，提交状态无法确认，数据可能已经提交：", error);
                throw error;
            }
            try {
                return connection.request(request, remaining);
            } catch (ConnectionException | ReadTimeoutException error) {
                // 读不重试，但下次请求也不能再连回已失败的节点；写继续在别的节点重试。
                switchFrom(current);
                target = null;
                if (!write) {
                    LOG.log(Level.FINE, "读请求在 {0} 失败，不自动重发：{1}", new Object[] {current, error.getMessage()});
                    throw error;
                }
                if (closed) return uncertain(request, "写请求传输已关闭，提交状态无法确认，数据可能已经提交：", error);
                if (attempt == attempts - 1) {
                    return uncertain(request, "写请求重试 3 次后仍未获得成功确认，提交状态无法确认，数据可能已经提交：", error);
                }
                LOG.log(Level.FINE, "写请求 {0} 在 {1} 失败，第 {2} 次重试：{3}",
                        new Object[] {request.get("requestId"), current, attempt + 1, error.getMessage()});
            } finally {
                pool.release(connection);
            }
        }
    }

    private static Map<String, Object> uncertain(Map<String, Object> request, String message, TransportException error) {
        Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("code", "FAILED");
        response.put("requestId", request.get("requestId"));
        response.put("message", error == null ? message : message + error.getMessage());
        return response;
    }

    /** 只有失败的正是当前入口时才换：指定地址失败不影响全局入口。 */
    private void switchFrom(Address failed) {
        synchronized (lock) {
            if (addresses.size() > 1 && addresses.get(index).equals(failed)) {
                // 不选刚刚失败的入口；多轮允许回到已恢复的节点。
                int offset = 1 + random.nextInt(addresses.size() - 1);
                index = (index + offset) % addresses.size();
            }
        }
    }

    /** 显式刷新发现的 Compute 外部地址，不自动重发任何当前读请求。 */
    @Override
    public void updateAddresses(List<Address> discovered) {
        List<Address> values = validate(discovered, "发现的计算节点地址不能为空");
        synchronized (lock) {
            Address current = addresses.get(index);
            addresses = values;
            int position = values.indexOf(current);
            index = position >= 0 ? position : random.nextInt(values.size());
        }
        pool.retainOnly(values);
    }

    @Override
    public void close() {
        closed = true;
        pool.close();
    }
}
