package com.cengyan.core;

/**
 * 批量写入的全局进度快照。不可变。
 *
 * <p>成功返回时 {@link #failedBatch()} 为 null，{@link #requestId()} 是最后一个已确认批次的 id。
 * 从异常上取得时，{@link #failedBatch()} 是失败（或下一个未发送）批次的全局编号，
 * 可以直接作为重放时的 {@code startBatch}；{@link #requestId()} 是失败批次的 id（发送前失败时为 null）。
 */
public final class BulkProgress {
    private final long confirmed;
    private final long skipped;
    private final long duplicates;
    private final int completedBatches;
    private final Integer failedBatch;
    private final String requestId;

    public BulkProgress(long confirmed, long skipped, long duplicates, int completedBatches, Integer failedBatch, String requestId) {
        this.confirmed = confirmed;
        this.skipped = skipped;
        this.duplicates = duplicates;
        this.completedBatches = completedBatches;
        this.failedBatch = failedBatch;
        this.requestId = requestId;
    }

    /** 服务端确认写入的行数（rowsAffected 之和）。 */
    public long confirmed() {
        return confirmed;
    }

    /** 因端点不存在被跳过的边数（仅 skipMissingEndpoints 时）。 */
    public long skipped() {
        return skipped;
    }

    /** 服务端判定为重复的行数。 */
    public long duplicates() {
        return duplicates;
    }

    /** 已完成的批次数（含重放时按 startBatch 跳过的批次）。 */
    public int completedBatches() {
        return completedBatches;
    }

    /** 失败批次的全局编号；成功时为 null。 */
    public Integer failedBatch() {
        return failedBatch;
    }

    public String requestId() {
        return requestId;
    }

    @Override
    public String toString() {
        return "BulkProgress{confirmed=" + confirmed + ", skipped=" + skipped + ", duplicates=" + duplicates
                + ", completedBatches=" + completedBatches + (failedBatch == null ? "" : ", failedBatch=" + failedBatch)
                + (requestId == null ? "" : ", requestId=" + requestId) + "}";
    }
}
