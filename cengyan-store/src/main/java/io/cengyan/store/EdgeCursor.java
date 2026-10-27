package io.cengyan.store;

/** 一次前缀扫描。用完必须关闭，取消和截断在 {@link #hasNext()} 或 {@link #next()} 里抛出。 */
public interface EdgeCursor extends AutoCloseable {
    boolean hasNext();

    EdgeRef next();

    /** 已经检查过的边数，包含还没返回给调用方的当前条。 */
    int examined();

    @Override
    void close();
}
