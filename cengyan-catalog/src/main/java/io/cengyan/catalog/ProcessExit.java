package io.cengyan.catalog;

/**
 * 模拟进程在两次提交之间退出。用 Error 是为了不让补偿逻辑把它当成可回退的失败。
 */
public final class ProcessExit extends Error {
    public ProcessExit() {
        super("模拟进程退出");
    }
}
