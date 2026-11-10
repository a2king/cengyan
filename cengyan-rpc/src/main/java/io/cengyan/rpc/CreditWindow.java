package io.cengyan.rpc;

/** 单流双额度窗口。总接收预算应在调用 grant 前由节点预算分配器预留。 */
public final class CreditWindow {
    private final long maxBytes;
    private final int maxBatches;
    private long bytes;
    private int batches;
    private boolean closed;

    public CreditWindow(long maxBytes, int maxBatches) {
        if (maxBytes <= 0 || maxBatches <= 0) throw new IllegalArgumentException("流额度上限必须为正数");
        this.maxBytes = maxBytes;
        this.maxBatches = maxBatches;
    }

    public synchronized void grant(long moreBytes, int moreBatches) {
        if (closed) throw new IllegalStateException("流已关闭");
        if (moreBytes < 0 || moreBatches < 0 || moreBytes > maxBytes - bytes || moreBatches > maxBatches - batches) {
            throw new IllegalArgumentException("流额度超出窗口限制");
        }
        bytes += moreBytes;
        batches += moreBatches;
    }

    public synchronized boolean tryConsume(int batchBytes) {
        if (batchBytes < 0 || batchBytes > maxBytes) throw new IllegalArgumentException("批次大小超出窗口限制");
        if (closed || batches == 0 || bytes < batchBytes) return false;
        bytes -= batchBytes;
        batches--;
        return true;
    }

    public synchronized void close() { closed = true; bytes = 0; batches = 0; }
    public synchronized long availableBytes() { return bytes; }
    public synchronized int availableBatches() { return batches; }
}
