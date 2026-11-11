package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 在最长查询保护期之后压缩已提交历史。命令带完整旧值 CAS，绝不跨过活动预留。
 * 协调记录尚被版本引用时不删除；先完成所有参与者物化才允许清理决定。
 */
public final class VersionMaintenance {
    /** 最长查询保护期：此期间的版本绝不能被压缩掉。 */
    public static final long DEFAULT_READ_PROTECTION_MS = TimeUnit.MINUTES.toMillis(35);
    /** 已提交历史的默认保留期，超过后可压缩为基线版本。 */
    public static final long DEFAULT_RETENTION_MS = TimeUnit.HOURS.toMillis(24);
    public static final long MIN_RETENTION_MS = TimeUnit.MINUTES.toMillis(1);
    public static final long MAX_RETENTION_MS = TimeUnit.DAYS.toMillis(30);
    private final StorageGateway storage;
    private final long retentionMs;
    private final long readProtectionMs;
    public VersionMaintenance(StorageGateway storage) { this(storage, DEFAULT_RETENTION_MS, DEFAULT_READ_PROTECTION_MS); }
    /**
     * @param retentionMs 已提交历史保留期；决定分片库里旧版本值占用的空间
     * @param readProtectionMs 查询保护期，不能在活动查询期间压缩
     */
    public VersionMaintenance(StorageGateway storage, long retentionMs, long readProtectionMs) {
        if (retentionMs < MIN_RETENTION_MS || retentionMs > MAX_RETENTION_MS)
            throw new IllegalArgumentException("历史版本保留期必须在 1 分钟到 30 天之间：" + retentionMs);
        if (readProtectionMs < 0) throw new IllegalArgumentException("查询保护期不能为负数");
        this.storage = storage; this.retentionMs = retentionMs; this.readProtectionMs = readProtectionMs;
    }

    public void collect(TransactionRecord transaction, long now) {
        if (!transaction.getFinalized() || transaction.getDecidedAtMs() <= 0 || now - transaction.getDecidedAtMs() < retentionMs + readProtectionMs) return;
        TransactionRecord onKey = transaction;
        TransactionPlan summary = transaction.getPlan();
        ByteString savedPlan = null;
        if (summary.getParticipantsCount() == 0) {
            savedPlan = storage.get(summary.getGraphId(), summary.getCoordinatorPart(), TransactionEngine.transactionPlanKey(summary.getTransactionId()));
            if (savedPlan == null) {
                deleteIfPresent(summary.getGraphId(), summary.getCoordinatorPart(), TransactionEngine.transactionGcKey(summary.getTransactionId()),
                        "transaction.gc.stub/" + summary.getTransactionId());
                return;
            }
            try { transaction = TransactionRecord.parseFrom(savedPlan); }
            catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "事务计划损坏", ex); }
            summary = transaction.getPlan();
            if (!transaction.getFinalized() || summary.getParticipantsCount() == 0) return;
        }
        TransactionPlan plan = summary;
        for (ParticipantPlan participant : plan.getParticipantsList()) {
            for (ConditionalMutation change : participant.getChangesList()) {
                if (change.getReadOnly()) continue;
                ByteString key = change.getKey(), lock = TransactionEngine.text("__lock/").concat(key);
                ByteString encoded = storage.get(plan.getGraphId(), participant.getPartId(), key);
                if (encoded == null) continue;
                VersionedCell cell = parse(encoded);
                boolean referenced = cell.getVersionsList().stream().anyMatch(version -> version.getTransactionId().equals(plan.getTransactionId()));
                if (!referenced) continue;
                if (storage.get(plan.getGraphId(), participant.getPartId(), lock) != null) return;
                int baseline = -1;
                for (int i = 0; i < cell.getVersionsCount(); i++) {
                    CellVersion version = cell.getVersions(i);
                    if (version.getBaseline() || version.getCommittedAtMs() > 0 && now - version.getCommittedAtMs() >= readProtectionMs) baseline = i;
                    else break;
                }
                if (baseline < 0) return;
                CellVersion latest = cell.getVersions(baseline);
                var compacted = VersionedCell.newBuilder().addVersions(latest.toBuilder().setBaseline(true).clearTransactionId().clearCoordinatorPart());
                for (int i = baseline + 1; i < cell.getVersionsCount(); i++) compacted.addVersions(cell.getVersions(i));
                if (compacted.getVersionsList().stream().anyMatch(version -> version.getTransactionId().equals(plan.getTransactionId()))) return;
                var command = StorageGateway.batch("version.compact/" + plan.getTransactionId() + "/" + participant.getPartId() + "/" + digest(key.concat(encoded)))
                        .addConditions(StorageGateway.condition(lock, null)).addConditions(StorageGateway.condition(key, encoded))
                        .addMutations(TransactionEngine.put(key, compacted.build().toByteString()));
                try { storage.write(plan.getGraphId(), participant.getPartId(), command.build()); }
                catch (ClusterException ex) { if (ex.code().equals("WRITE_CONFLICT")) return; throw ex; }
            }
        }
        // 这里再次确认引用全部消失，再删协调记录，避免中途失败留下无法解释的版本。
        for (ParticipantPlan participant : plan.getParticipantsList()) for (ConditionalMutation change : participant.getChangesList()) {
            if (change.getReadOnly()) continue;
            ByteString encoded = storage.get(plan.getGraphId(), participant.getPartId(), change.getKey());
            if (encoded != null && parse(encoded).getVersionsList().stream().anyMatch(version -> version.getTransactionId().equals(plan.getTransactionId()))) return;
        }
        ByteString key = TransactionEngine.transactionKey(plan.getTransactionId());
        ByteString planKey = TransactionEngine.transactionPlanKey(plan.getTransactionId());
        ByteString gcKey = TransactionEngine.transactionGcKey(plan.getTransactionId());
        if (savedPlan == null) savedPlan = storage.get(plan.getGraphId(), plan.getCoordinatorPart(), planKey);
        ByteString gcBytes = storage.get(plan.getGraphId(), plan.getCoordinatorPart(), gcKey);
        // 长寿命状态机子命令不能因协调记录消失而重新 BEGIN；保留一个终态墓碑。
        TransactionRecord tombstone = onKey.toBuilder().setPlan(plan.toBuilder().clearParticipants()).build();
        var command = StorageGateway.batch("transaction.gc/" + plan.getTransactionId())
                .addConditions(StorageGateway.condition(key, onKey.toByteString()))
                .addMutations(TransactionEngine.put(key, tombstone.toByteString()));
        if (savedPlan != null) command.addConditions(StorageGateway.condition(planKey, savedPlan))
                .addMutations(Mutation.newBuilder().setKey(planKey).setDelete(true));
        if (gcBytes != null) command.addConditions(StorageGateway.condition(gcKey, gcBytes))
                .addMutations(Mutation.newBuilder().setKey(gcKey).setDelete(true));
        try { storage.write(plan.getGraphId(), plan.getCoordinatorPart(), command.build()); }
        catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        // 先去掉对外提交票据，过期 ID 不能继续声明持久可重放；子操作墓碑保守保留。
        ByteString ticket = TransactionEngine.text("requests/" + plan.getTransactionId());
        GraphDefinition graph = new DistributedCatalog(storage).graphs().stream().filter(g -> g.getId() == plan.getGraphId()).findFirst().orElse(null);
        long ticketGraph = graph == null ? 0 : DistributedGraph.ticketGraph(graph);
        int ticketPart = graph == null ? 0 : DistributedGraph.ticketPart(graph, plan.getTransactionId());
        ByteString saved = storage.get(ticketGraph, ticketPart, ticket);
        if (saved != null) storage.write(ticketGraph, ticketPart, StorageGateway.batch("request.gc/" + plan.getTransactionId())
                .addConditions(StorageGateway.condition(ticket, saved)).addMutations(Mutation.newBuilder().setKey(ticket).setDelete(true)).build());
    }
    private void deleteIfPresent(long graph, int part, ByteString key, String requestId) {
        ByteString saved = storage.get(graph, part, key);
        if (saved == null) return;
        try {
            storage.write(graph, part, StorageGateway.batch(requestId).addConditions(StorageGateway.condition(key, saved))
                    .addMutations(Mutation.newBuilder().setKey(key).setDelete(true)).build());
        } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
    }
    private static VersionedCell parse(ByteString bytes) {
        try { return VersionedCell.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "历史版本损坏", ex); }
    }
    private static String digest(ByteString bytes) {
        try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); }
        catch (java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
}
