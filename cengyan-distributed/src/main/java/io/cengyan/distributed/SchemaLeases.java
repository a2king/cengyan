package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** 不逐请求复制的租约授权窗口：先提交保留上界，才发放该范围内租约。 */
public final class SchemaLeases {
    private static final long TTL = 30_000, WINDOW = 60_000, MAX_QUERY = TimeUnit.MINUTES.toMillis(30), SKEW = 5_000;
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    public SchemaLeases(StorageGateway storage) { this.storage = storage; catalog = new DistributedCatalog(storage); }

    public SchemaLease acquire(String graphName, String requestId, long deadline, long epoch, boolean write) {
        long now = System.currentTimeMillis();
        if (deadline <= now || deadline - now > MAX_QUERY) throw new ClusterException("INVALID_ARGUMENT", "Schema Lease 必须有不超过 30 分钟的请求截止时间");
        GraphDefinition graph = catalog.graph(graphName);
        if (write) DistributedCatalog.requireWritable(graph);
        if (!graph.getState().equals("ACTIVE") && !graph.getState().equals("MIGRATING")) throw new ClusterException("GRAPH_STATE", "目标图当前状态为 " + graph.getState());
        for (int attempt = 0; attempt < 16; attempt++) {
            if (write) DistributedCatalog.requireWritable(graph);
            ByteString key = key(graph.getId(), graph.getSchemaVersion());
            ByteString encoded = storage.get(0, 0, key);
            LeaseHorizon previous = encoded == null ? LeaseHorizon.getDefaultInstance() : parse(encoded);
            if (previous.getEpoch() > epoch) throw new ClusterException("STALE_CONTROLLER", "租约请求来自过期的 Metadata 任期");
            if (previous.getEpoch() == epoch && previous.getGrantUntilMs() > now && previous.getRetainUntilMs() >= deadline + SKEW) {
                // 授权窗口不是 Schema 缓存：发放前再经线性一致读取确认版本，防止 ACTIVATE 后给新请求发旧租约。
                GraphDefinition current = catalog.graph(graph.getName());
                if (!current.equals(graph)) { graph = current; continue; }
                return SchemaLease.newBuilder().setLeaseId(UUID.randomUUID().toString()).setRequestId(requestId).setGraphId(graph.getId())
                        .setSchemaVersion(graph.getSchemaVersion()).setGeneration(graph.getGeneration()).setIssuedAtMs(now)
                        .setExpiresAtMs(Math.min(now + TTL, deadline)).setHardDeadlineMs(deadline).setMetadataEpoch(epoch).build();
            }
            LeaseHorizon next = LeaseHorizon.newBuilder().setGraphId(graph.getId()).setSchemaVersion(graph.getSchemaVersion()).setGeneration(graph.getGeneration())
                    .setEpoch(epoch).setGrantUntilMs(now + WINDOW).setRetainUntilMs(Math.max(previous.getRetainUntilMs(), now + WINDOW + MAX_QUERY + SKEW)).build();
            try {
                storage.write(0, 0, StorageGateway.batch("lease.horizon/" + graph.getId() + "/" + UUID.randomUUID())
                        .addConditions(StorageGateway.condition(key, encoded))
                        .addConditions(StorageGateway.condition(DistributedCatalog.graphKey(graph.getName()), graph.toByteString()))
                        .addMutations(TransactionEngine.put(key, next.toByteString())).build());
            } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; graph = catalog.graph(graphName); }
        }
        throw new ClusterException("BUSY", "Schema 版本正在切换，请重新发起请求");
    }
    public SchemaLease renew(SchemaLease lease) {
        validate(lease, lease.getGraphId(), lease.getSchemaVersion());
        long now = System.currentTimeMillis();
        return lease.toBuilder().setExpiresAtMs(Math.min(lease.getHardDeadlineMs(), now + TTL)).build();
    }
    public void validate(SchemaLease lease, long graph, long version) {
        long now = System.currentTimeMillis();
        if (lease.getLeaseId().isBlank() || lease.getRequestId().isBlank() || lease.getGraphId() != graph || lease.getSchemaVersion() != version
                || lease.getExpiresAtMs() <= now || lease.getHardDeadlineMs() <= now || lease.getIssuedAtMs() > now + SKEW) {
            throw new ClusterException("SCHEMA_VERSION_MISMATCH", "Schema Lease 已失效或不匹配");
        }
        ByteString encoded = storage.get(0, 0, key(graph, version));
        if (encoded == null || parse(encoded).getRetainUntilMs() < lease.getHardDeadlineMs() + SKEW) {
            throw new ClusterException("SCHEMA_VERSION_MISMATCH", "Schema 版本保留边界不存在");
        }
    }
    public static ByteString key(long graph, long version) { return TransactionEngine.text("schema/lease/" + graph + "/" + version); }
    private static LeaseHorizon parse(ByteString bytes) {
        try { return LeaseHorizon.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "租约授权窗口损坏", ex); }
    }
}
