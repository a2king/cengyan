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
    private static final long READ_PROTECTION = TimeUnit.MINUTES.toMillis(35);
    private static final long RETENTION = TimeUnit.HOURS.toMillis(24);
    private final StorageGateway storage;
    public VersionMaintenance(StorageGateway storage) { this.storage = storage; }

    public void collect(TransactionRecord transaction, long now) {
        if (!transaction.getFinalized() || transaction.getDecidedAtMs() <= 0 || now - transaction.getDecidedAtMs() < RETENTION + READ_PROTECTION) return;
        TransactionPlan plan = transaction.getPlan();
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
                    if (version.getBaseline() || version.getCommittedAtMs() > 0 && now - version.getCommittedAtMs() >= READ_PROTECTION) baseline = i;
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
        // 长寿命状态机子命令不能因协调记录消失而重新 BEGIN；保留一个终态墓碑。
        TransactionRecord tombstone = transaction.toBuilder().setPlan(plan.toBuilder().clearParticipants()).build();
        var command = StorageGateway.batch("transaction.gc/" + plan.getTransactionId())
                .addConditions(StorageGateway.condition(key, transaction.toByteString()))
                .addMutations(TransactionEngine.put(key, tombstone.toByteString()));
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
    private static VersionedCell parse(ByteString bytes) {
        try { return VersionedCell.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "历史版本损坏", ex); }
    }
    private static String digest(ByteString bytes) {
        try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); }
        catch (java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
}
