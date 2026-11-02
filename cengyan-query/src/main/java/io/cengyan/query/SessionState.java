package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Auth;
import io.cengyan.catalog.GraphName;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/** 一个连接上的当前图和登录用户。进程退出后消失。 */
public final class SessionState {
    private GraphName current;
    private volatile String user;
    private volatile int role;
    private volatile int computeNode;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /** 读查询可以共享；USE、登录和写入通过写锁独占会话状态。 */
    public <T> T withReadLock(Supplier<T> action) {
        if (action == null) {
            throw new IllegalArgumentException("会话动作不能为空");
        }
        lock.readLock().lock();
        try {
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public <T> T withWriteLock(Supplier<T> action) {
        if (action == null) {
            throw new IllegalArgumentException("会话动作不能为空");
        }
        lock.writeLock().lock();
        try {
            return action.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void use(GraphName graph) {
        if (graph == null) {
            throw new IllegalArgumentException("图不能为空");
        }
        this.current = graph;
    }

    public GraphName current() {
        if (current == null) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "还没有选择图");
        }
        return current;
    }

    public void bind(String name, int role) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("用户不能为空");
        }
        if (role != Auth.ROLE_READ && role != Auth.ROLE_WRITE) {
            throw new IllegalArgumentException("角色无效");
        }
        this.role = role;
        this.user = name;
    }

    /** 进程内测试没有登录用户，服务协议必须先登录。 */
    public boolean authenticated() {
        return user != null;
    }

    public boolean readOnly() {
        return authenticated() && role == Auth.ROLE_READ;
    }

    public String user() {
        return user;
    }

    public int role() {
        return role;
    }

    public int computeNode() {
        return computeNode;
    }

    /** 第一次 execute 绑上计算节点后，后面的语句还在这台。 */
    public synchronized void pinCompute(int nodeId) {
        if (computeNode == 0 && nodeId != 0) {
            computeNode = nodeId;
        }
    }
}
