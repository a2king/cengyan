package com.cengyan.core;

import java.util.regex.Pattern;

/**
 * 批量写入参数。不可变，用 {@link #builder()} 构造；{@link #defaults()} 为全部默认值。
 *
 * <p>重放时 rows、batchSize、maxBatchBytes、alignPartitions 必须与首次调用一致，
 * 并把异常里的 {@code failedBatch} 传给 {@link Builder#startBatch(int)}：之前的批次只占位不发送，
 * 批次编号和切批结果与首次相同，服务端按 requestId 去重。
 */
public final class BulkOptions {
    /** 服务端 bulkInsert 的单批行数上限。 */
    public static final int MAX_BATCH_ROWS = 10_000;
    /** 服务端 bulkInsert 的单批字节上限（含服务端追加的字段）。 */
    public static final int MAX_BATCH_BYTES = 4 * 1024 * 1024;
    static final int MIN_BATCH_BYTES = 1024;
    static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,120}");

    private static final BulkOptions DEFAULTS = builder().build();

    private final int batchSize;
    private final int maxBatchBytes;
    private final RequestIdProvider requestId;
    private final int startBatch;
    private final boolean alignPartitions;
    private final boolean skipMissingEndpoints;

    private BulkOptions(Builder builder) {
        this.batchSize = builder.batchSize;
        this.maxBatchBytes = builder.maxBatchBytes;
        this.requestId = builder.requestId;
        this.startBatch = builder.startBatch;
        this.alignPartitions = builder.alignPartitions;
        this.skipMissingEndpoints = builder.skipMissingEndpoints;
    }

    public static BulkOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int batchSize() {
        return batchSize;
    }

    public int maxBatchBytes() {
        return maxBatchBytes;
    }

    /** null 表示每批随机生成 UUID。 */
    public RequestIdProvider requestId() {
        return requestId;
    }

    public int startBatch() {
        return startBatch;
    }

    public boolean alignPartitions() {
        return alignPartitions;
    }

    public boolean skipMissingEndpoints() {
        return skipMissingEndpoints;
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.batchSize = batchSize;
        builder.maxBatchBytes = maxBatchBytes;
        builder.requestId = requestId;
        builder.startBatch = startBatch;
        builder.alignPartitions = alignPartitions;
        builder.skipMissingEndpoints = skipMissingEndpoints;
        return builder;
    }

    public static final class Builder {
        private int batchSize = 1024;
        private int maxBatchBytes = MAX_BATCH_BYTES;
        private RequestIdProvider requestId;
        private int startBatch;
        private boolean alignPartitions = true;
        private boolean skipMissingEndpoints;

        private Builder() {
        }

        /** 每批最多行数，1 到 10000，默认 1024。 */
        public Builder batchSize(int value) {
            if (value < 1 || value > MAX_BATCH_ROWS) throw new IllegalArgumentException("batchSize 必须在 1 到 " + MAX_BATCH_ROWS + " 之间：" + value);
            this.batchSize = value;
            return this;
        }

        /** 每批最多字节数，1 KiB 到 4 MiB，默认 4 MiB（SDK 实际最多用到 4 MiB − 64 KiB，给服务端追加字段留余量）。 */
        public Builder maxBatchBytes(int value) {
            if (value < MIN_BATCH_BYTES || value > MAX_BATCH_BYTES) {
                throw new IllegalArgumentException("maxBatchBytes 必须在 " + MIN_BATCH_BYTES + " 到 " + MAX_BATCH_BYTES + " 之间：" + value);
            }
            this.maxBatchBytes = value;
            return this;
        }

        /** 每批 requestId 由调用方决定（断线后重放的唯一正确做法）。null 恢复为随机 UUID。 */
        public Builder requestId(RequestIdProvider provider) {
            this.requestId = provider;
            return this;
        }

        /** 每一批都用同一个固定 requestId：只适用于一批就写完的场景。 */
        public Builder requestId(String fixed) {
            if (fixed == null || !REQUEST_ID.matcher(fixed).matches()) {
                throw new IllegalArgumentException("requestId 必须匹配 [A-Za-z0-9_-]{1,120}：" + fixed);
            }
            this.requestId = (index, rows) -> fixed;
            return this;
        }

        /** 跳过前 n 个批次（重放时传异常里的 failedBatch）。 */
        public Builder startBatch(int value) {
            if (value < 0) throw new IllegalArgumentException("startBatch 必须是非负整数：" + value);
            this.startBatch = value;
            return this;
        }

        /**
         * 仅对写点生效，默认 true：按主键分片攒批，每批只落在一个分片上，并优先发往与该分片 Leader 同机的计算节点。
         * 写边总是按输入顺序切批。
         */
        public Builder alignPartitions(boolean value) {
            this.alignPartitions = value;
            return this;
        }

        /** 仅对写边生效：端点不存在的边跳过（计入 skipped），而不是整批失败。 */
        public Builder skipMissingEndpoints(boolean value) {
            this.skipMissingEndpoints = value;
            return this;
        }

        public BulkOptions build() {
            return new BulkOptions(this);
        }
    }
}
