package com.cengyan.core;

import com.cengyan.core.internal.BulkWriter;

import java.time.Duration;
import java.util.Map;

/**
 * 一个图上的操作。不保存状态，图名随每次请求发送；可随意创建和共享。
 *
 * <p>{@link #execute} 默认按只读处理，SDK 不猜 GQL 的读写属性：写语句用 {@link #write} 或 {@code execute(..., true, ...)}。
 */
public final class ClusterGraph {
    private final ClusterClient client;
    private final String name;

    ClusterGraph(ClusterClient client, String name) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("图名不能为空");
        this.client = client;
        this.name = name;
    }

    public String name() {
        return name;
    }

    public ClusterClient client() {
        return client;
    }

    /**
     * 执行一条 GQL。
     *
     * @param params  查询参数，可以为 null
     * @param write   写语句传 true：断线时用同一 requestId 重试，最终无法确认时抛 CommitUncertainException
     * @param timeout 总预算，null 用客户端默认超时
     */
    public Result execute(String query, Map<String, ?> params, boolean write, Duration timeout) {
        return client.execute(query, name, params, write, timeout);
    }

    /** 只读查询；断线不自动重算。 */
    public Result read(String query) {
        return execute(query, null, false, null);
    }

    public Result read(String query, Map<String, ?> params) {
        return execute(query, params, false, null);
    }

    /** 写语句；断线最多复用同一 requestId 重试三次。 */
    public Result write(String query) {
        return execute(query, null, true, null);
    }

    public Result write(String query, Map<String, ?> params) {
        return execute(query, params, true, null);
    }

    /** 批量写点，默认参数（每批 1024 行、按分片对齐、每批随机 requestId）。 */
    public BulkProgress insertNodes(String type, Iterable<?> rows) {
        return insertNodes(type, rows, BulkOptions.defaults());
    }

    /**
     * 批量写点。每行是 {@code Map<String, ?>}（主键放在 {@code id}，其余为属性）或 POJO
     * （主键为 {@code @CengyanId} 字段或名为 id 的字段）。rows 可以是惰性的 Iterable。
     *
     * <p>默认按主键分片攒批，每批只落在一个分片上。失败时异常上的 {@link com.cengyan.core.exception.CengyanException#bulkProgress()}
     * 给出全局进度，{@code failedBatch} 可直接作为重放时的 {@code startBatch}。
     */
    public BulkProgress insertNodes(String type, Iterable<?> rows, BulkOptions options) {
        return BulkWriter.write(this::send, name, false, type, rows, options);
    }

    /** 批量写边，默认参数。 */
    public BulkProgress insertEdges(String type, Iterable<?> rows) {
        return insertEdges(type, rows, BulkOptions.defaults());
    }

    /**
     * 批量写边。每行必须带边主键 {@code id}，以及 {@code src}、{@code dst}；
     * 缺 id 的行在进入网络请求前就被拒绝，已确认批次的进度保留在异常里。
     */
    public BulkProgress insertEdges(String type, Iterable<?> rows, BulkOptions options) {
        return BulkWriter.write(this::send, name, true, type, rows, options);
    }

    private Map<String, Object> send(Map<String, Object> payload, boolean write, Address address) {
        return client.request(payload, write, null, address);
    }

    @Override
    public String toString() {
        return "ClusterGraph{" + name + "}";
    }
}
