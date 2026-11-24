package com.cengyan.core;

import java.util.List;
import java.util.Map;

/**
 * 批量写入时由调用方决定每一批的 requestId。断线后要在服务端幂等窗口内安全重放，
 * 同一批必须得到同一个 id——用作业 id + 批次编号即可，例如 {@code (i, rows) -> "job-42-" + i}。
 *
 * <p>返回值必须匹配 {@code [A-Za-z0-9_-]{1,120}}。
 */
@FunctionalInterface
public interface RequestIdProvider {
    /**
     * @param batchIndex 全局批次编号，从 0 开始，与重放时的 {@code startBatch} 同一编号体系
     * @param rows       这一批的行（只读）
     */
    String requestId(int batchIndex, List<Map<String, Object>> rows);
}
