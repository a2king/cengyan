package io.cengyan.store;

/**
 * 多个投票成员时的写入。有 3 个投票成员以后，先把批次写入多数派的提交日志，再写本地库。
 * 不实现 Raft。
 */
public interface WriteGate {
    void replicate(GraphStore local, StoreBatch batch);
}
