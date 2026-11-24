package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.BulkOptions;
import com.cengyan.core.BulkProgress;
import com.cengyan.core.RequestIdProvider;
import com.cengyan.core.exception.BulkInsertException;
import com.cengyan.core.exception.CengyanException;
import com.cengyan.core.exception.CommitUncertainException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 一次批量写入：按行数和字节上限切批，全局批次编号，失败时把全局进度附到异常上。
 * 与 pycengyan 的 {@code _BulkRun} / {@code _Batch} 语义一致。
 *
 * <p>写点默认按主键分片攒批（各分片共用一套全局编号，按实际发送顺序递增）；写边按输入顺序切批。
 * 遇到无效行时已缓冲但未发送的行不发出——修好数据后用 {@code startBatch(failedBatch)} 重放，切批结果与首次一致。
 */
public final class BulkWriter {
    /** 发送一条请求并检查响应码（即 {@code ClusterClient.request}）。 */
    public interface Requester {
        Map<String, Object> request(Map<String, Object> payload, boolean write, Address address);
    }

    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,120}");
    /**
     * 服务端在校验 4 MiB 前会追加 _schemaLease / _reservation / _user / remainingTimeoutMs 等字段，
     * 并用 Jackson 重新序列化。贴近上限的批次要给这些留余量。
     */
    static final int SERVER_OVERHEAD_BYTES = 64 * 1024;

    private final Requester requester;
    private final String graph;
    private final boolean edge;
    private final String type;
    private final BulkOptions options;
    private final int header;
    private final int limit;
    private long confirmed;
    private long skipped;
    private long duplicates;
    private int completedBatches;
    private String lastRequestId;

    private BulkWriter(Requester requester, String graph, boolean edge, String type, BulkOptions options) {
        this.requester = requester;
        this.graph = graph;
        this.edge = edge;
        this.type = type;
        this.options = options;
        // 固定开销按 requestId 的最长形式计量，保证无论最终用哪个 id 都不会超限。
        this.header = Json.encodedSize(payload(Collections.<Map<String, Object>>emptyList(), repeat('x', 120)));
        this.limit = Math.min(options.maxBatchBytes(), BulkOptions.MAX_BATCH_BYTES - SERVER_OVERHEAD_BYTES);
    }

    /**
     * @param edge  true 写边（kind=edge），false 写点（kind=node）
     * @param rows  {@code Map<String, ?>} 或 POJO；可以是惰性的 Iterable
     */
    public static BulkProgress write(Requester requester, String graph, boolean edge, String type, Iterable<?> rows, BulkOptions options) {
        if (graph == null || graph.isEmpty()) throw new IllegalArgumentException("图名不能为空");
        if (type == null || type.isEmpty()) throw new IllegalArgumentException("类型名不能为空");
        if (rows == null) throw new IllegalArgumentException("rows 不能为 null");
        BulkWriter writer = new BulkWriter(requester, graph, edge, type, options == null ? BulkOptions.defaults() : options);
        if (!edge && writer.options.alignPartitions()) writer.aligned(rows);
        else writer.sequential(rows);
        return writer.progress(null, writer.lastRequestId);
    }

    private void sequential(Iterable<?> rows) {
        Batch batch = new Batch(null);
        for (Object item : rows) {
            Map<String, Object> row = toRow(item);
            int size = rowSize(row);
            // 已满批次先发出，再拒绝缺 id 的边，避免把已凑满的行留在未发送缓冲里。
            batch.makeRoom(size);
            if (edge) requireId(row);
            batch.append(row, size);
        }
        batch.flush();
    }

    private void aligned(Iterable<?> rows) {
        Map<String, Object> request = new LinkedHashMap<String, Object>();
        request.put("op", "bulkTopology");
        request.put("graph", graph);
        Map<String, Object> topology;
        try {
            topology = requester.request(request, false, null);
        } catch (CengyanException error) {
            throw annotate(error, null);
        }
        Object rawPartitions = topology.get("partitions");
        Object idType = topology.get(edge ? "edgeIdType" : "vertexIdType");
        Long partitions = rawPartitions instanceof Long || rawPartitions instanceof Integer ? ((Number) rawPartitions).longValue() : null;
        if (partitions == null || partitions < 1 || partitions > 65535 || !(idType instanceof String)) {
            throw annotate(new BulkInsertException("分片拓扑无效", "PROTOCOL_ERROR", topology), null);
        }
        Map<Object, Address> routes = routes(topology);
        Map<Integer, Batch> batches = new LinkedHashMap<Integer, Batch>();
        for (Object item : rows) {
            Map<String, Object> row = toRow(item);
            requireId(row);
            int part;
            try {
                part = PartitionRouter.partOf(row.get("id"), (String) idType, partitions.intValue());
            } catch (IllegalArgumentException ex) {
                throw annotate(new BulkInsertException("记录 id 无效：" + ex.getMessage(), "INVALID_ARGUMENT", null, ex), null);
            }
            int size = rowSize(row);
            Batch batch = batches.get(part);
            if (batch == null) {
                batch = new Batch(routes.get(part));
                batches.put(part, batch);
            }
            batch.makeRoom(size);
            batch.append(row, size);
        }
        for (Batch batch : batches.values()) batch.flush();
    }

    /** 分片 → 与该分片 Leader 同机的计算节点；匹配不上的分片走默认入口。 */
    private static Map<Object, Address> routes(Map<String, Object> topology) {
        Map<String, Address> computes = new LinkedHashMap<String, Address>();
        Object rawComputes = topology.get("computes");
        if (rawComputes instanceof List) {
            for (Object item : (List<?>) rawComputes) {
                if (!(item instanceof Map)) continue;
                Object host = ((Map<?, ?>) item).get("host");
                Object port = ((Map<?, ?>) item).get("port");
                if (host instanceof String && !((String) host).isEmpty() && port instanceof Long
                        && (Long) port >= 1 && (Long) port <= 65535 && !computes.containsKey(host)) {
                    computes.put((String) host, new Address((String) host, ((Long) port).intValue()));
                }
            }
        }
        Map<Object, Address> routes = new LinkedHashMap<Object, Address>();
        Object rawLeaders = topology.get("leaders");
        if (rawLeaders instanceof List) {
            for (Object item : (List<?>) rawLeaders) {
                if (!(item instanceof Map)) continue;
                Object part = ((Map<?, ?>) item).get("part");
                Object host = ((Map<?, ?>) item).get("host");
                Address address = host instanceof String ? computes.get(host) : null;
                if (part instanceof Long && address != null) routes.put(((Long) part).intValue(), address);
            }
        }
        return routes;
    }

    private Map<String, Object> toRow(Object item) {
        try {
            return PojoMapper.toRow(item, edge);
        } catch (IllegalArgumentException ex) {
            throw annotate(new BulkInsertException(ex.getMessage(), "INVALID_ARGUMENT", null, ex), null);
        }
    }

    /** 一行在批次里占用的字节（含行间逗号）；无法编码或单行超限时抛 BulkInsertException。 */
    private int rowSize(Map<String, Object> row) {
        int size;
        try {
            size = Json.encodedSize(row) + 1;
        } catch (IllegalArgumentException ex) {
            throw annotate(new BulkInsertException("记录无法编码为 JSON：" + ex.getMessage(), "INVALID_ARGUMENT", null, ex), null);
        }
        if (header + size > limit) {
            throw annotate(new BulkInsertException("单条记录超过批次字节上限", "RECORD_TOO_LARGE", null), null);
        }
        return size;
    }

    /** 主键与库内规则一致：调用方给出 id，客户端不推导。 */
    private void requireId(Map<String, Object> row) {
        if (row.get("id") == null) {
            throw annotate(new BulkInsertException(edge ? "边必须带 id" : "按分片写入时每行必须带 id", "INVALID_ARGUMENT", null), null);
        }
    }

    private CengyanException annotate(CengyanException error, String failedRequestId) {
        error.attachBulkProgress(progress(completedBatches, failedRequestId));
        return error;
    }

    private BulkProgress progress(Integer failedBatch, String requestId) {
        return new BulkProgress(confirmed, skipped, duplicates, completedBatches, failedBatch, requestId);
    }

    private Map<String, Object> payload(List<Map<String, Object>> rows, String requestId) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "bulkInsert");
        payload.put("graph", graph);
        payload.put("kind", edge ? "edge" : "node");
        payload.put("type", type);
        payload.put("rows", rows);
        payload.put("skipMissing", edge && options.skipMissingEndpoints());
        payload.put("requestId", requestId);
        return payload;
    }

    private void send(List<Map<String, Object>> rows, Address address) {
        int index = completedBatches;
        if (index < options.startBatch()) {
            // 重放时跳过已确认批次，但仍要占位，保证批次编号与首次调用一致。
            completedBatches++;
            return;
        }
        RequestIdProvider provider = options.requestId();
        String identifier;
        if (provider == null) {
            identifier = UUID.randomUUID().toString();
        } else {
            identifier = provider.requestId(index, Collections.unmodifiableList(rows));
            if (identifier == null || !REQUEST_ID.matcher(identifier).matches()) {
                throw new IllegalArgumentException("requestId 必须匹配 [A-Za-z0-9_-]{1,120}：" + identifier);
            }
        }
        Map<String, Object> response;
        try {
            response = requester.request(payload(rows, identifier), true, address);
        } catch (CengyanException error) {
            throw annotate(error, identifier);
        } catch (RuntimeException error) {
            // 非协议异常：提交状态同样无法确认。
            Map<String, Object> details = new LinkedHashMap<String, Object>();
            details.put("message", String.valueOf(error.getMessage()));
            throw annotate(new CommitUncertainException(String.valueOf(error.getMessage()), "FAILED", details, error), identifier);
        }
        Object code = response.get("code");
        if (code != null && !"OK".equals(code)) throw annotate(ErrorCodes.error(response, true), identifier);
        confirmed += count(response.get("rowsAffected"));
        skipped += count(response.get("skipped"));
        duplicates += count(response.get("duplicates"));
        completedBatches++;
        lastRequestId = identifier;
    }

    private static long count(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static String repeat(char c, int times) {
        char[] chars = new char[times];
        java.util.Arrays.fill(chars, c);
        return new String(chars);
    }

    /** 一个待发送批次：满行数立即发出，放不下下一行时先发出。 */
    private final class Batch {
        private final Address address;
        private List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        private int size;

        Batch(Address address) {
            this.address = address;
        }

        void makeRoom(int rowSize) {
            if (!rows.isEmpty() && header + size + rowSize > limit) flush();
        }

        void append(Map<String, Object> row, int rowSize) {
            rows.add(row);
            size += rowSize;
            if (rows.size() >= options.batchSize()) flush();
        }

        void flush() {
            if (rows.isEmpty()) return;
            List<Map<String, Object>> pending = rows;
            rows = new ArrayList<Map<String, Object>>();
            size = 0;
            send(pending, address);
        }
    }
}
