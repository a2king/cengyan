package io.cengyan;

/**
 * 扫描和执行循环观察的取消标志。超时、断连和手动终止都只调用 {@code cancel}。
 */
public interface CancelToken {
    boolean cancelled();

    /**
     * 取消已经发生时立刻执行，否则等 {@code cancel} 时执行。
     * 钩子可能跑在超时线程上，不能在里面做长时间阻塞。
     */
    void onCancel(Runnable hook);

    /** 扫描正常结束后摘掉钩子，避免同一次查询里钩子越积越多。 */
    default void forget(Runnable hook) {
    }
}
