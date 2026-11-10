package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

/** Metadata Leader 持久化编排影子代次迁移，业务读者仍固定旧 Schema/generation。 */
public final class SchemaController {
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    public SchemaController(StorageGateway storage) { this.storage = storage; catalog = new DistributedCatalog(storage); }

    public void alter(String requestId, String name, GraphDefinition target) {
        ByteString existingTask = storage.get(0, 0, key(requestId));
        if (existingTask != null) { resume(parse(existingTask)); return; }
        GraphDefinition before = catalog.graph(name);
        DistributedCatalog.requireWritable(before);
        if (target.getId() != before.getId() || target.getPartitions() != before.getPartitions()
                || target.getVertexIdType() != before.getVertexIdType() || target.getEdgeIdType() != before.getEdgeIdType()) {
            throw new ClusterException("UNSUPPORTED", "第一版不允许改变图分片数量或 ID 类型");
        }
        ByteString key = key(requestId);
        ByteString existing = storage.get(0, 0, key);
        SchemaMigrationTask task;
        if (existing != null) task = parse(existing);
        else {
            GraphDefinition after = target.toBuilder().setSchemaVersion(before.getSchemaVersion() + 1)
                    .setGeneration(before.getGeneration() + 1).setWriteEpoch(before.getWriteEpoch() + 1).setState("ACTIVE").build();
            long retention = System.currentTimeMillis();
            ByteString horizon = storage.get(0, 0, SchemaLeases.key(before.getId(), before.getSchemaVersion()));
            if (horizon != null) {
                try { retention = LeaseHorizon.parseFrom(horizon).getRetainUntilMs(); }
                catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "Schema 租约保留边界损坏", ex); }
            }
            task = SchemaMigrationTask.newBuilder().setTaskId(requestId).setBefore(before).setAfter(after).setStage("PREPARE").setRetainUntilMs(retention).build();
            storage.write(0, 0, StorageGateway.batch("schema.prepare/" + requestId)
                    .addConditions(StorageGateway.condition(DistributedCatalog.graphKey(name), before.toByteString()))
                    .addConditions(StorageGateway.condition(key, null))
                    .addMutations(TransactionEngine.put(key, task.toByteString()))
                    .addMutations(TransactionEngine.put(DistributedCatalog.versionKey(before.getId(), before.getSchemaVersion()), before.toByteString()))
                    .addMutations(TransactionEngine.put(DistributedCatalog.graphKey(name), before.toBuilder().setState("MIGRATING").build().toByteString())).build());
        }
        resume(task);
    }

    public void resume(SchemaMigrationTask initial) {
        SchemaMigrationTask task = initial; var before = task.getBefore(); var after = task.getAfter();
        if (task.getStage().equals("FINISHED")) return;
        if (task.getStage().equals("CLEANED")) return;
        if (task.getStage().equals("PREPARE")) {
            for (int part = 0; part < before.getPartitions(); part++) {
                storage.write(before.getId(), part, StorageGateway.batch("schema.barrier/" + task.getTaskId() + "/" + part)
                        .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeGate"), TransactionEngine.text("MIGRATING"))).build());
            }
            // PREPARE 决定之前已接纳事务必须完成或中止，不能在旧视图复制后迟到提交。
            for (int part = 0; part < before.getPartitions(); part++) {
                ByteString last = ByteString.EMPTY; String cursor = "";
                do {
                    // 恢复事务必须读取完整计划；摘要会清空 participants，不能交给 resume() 收尾。
                    ScanPage page = storage.scan(before.getId(), part, TransactionEngine.TX_PREFIX, last, 100, cursor, false);
                    for (var entry : page.getEntriesList()) {
                        try {
                            TransactionRecord record = TransactionRecord.parseFrom(entry.getValue());
                            if (!record.getFinalized()) new TransactionEngine(storage).resume(record);
                        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { throw new ClusterException("CORRUPT_DATA", "事务记录损坏", ex); }
                    }
                    if (!page.getMore()) break; last = page.getLastKey(); cursor = page.getCursorId();
                } while (true);
            }
            task = stage(task, "COPYING");
        }
        if (task.getStage().equals("COPYING")) {
            var data = new DistributedGraph(storage);
            var scope = data.readScope(before.getId());
            final String taskId = task.getTaskId();
            final GraphDefinition target = after;
            for (boolean node : List.of(true, false)) {
                List<Entity> batch = new ArrayList<>(128);
                int[] offset = {0};
                data.forEachEntity(before, node, null, scope, entity -> {
                    batch.add(entity);
                    if (batch.size() == 128) {
                        String sub = taskId + "_" + (node ? "n" : "e") + "_" + offset[0];
                        data.migrateEntities(sub, target, List.copyOf(batch));
                        offset[0] += batch.size();
                        batch.clear();
                    }
                });
                if (!batch.isEmpty()) {
                    String sub = taskId + "_" + (node ? "n" : "e") + "_" + offset[0];
                    data.migrateEntities(sub, target, List.copyOf(batch));
                }
            }
            task = stage(task, "ACTIVATING");
        }
        if (task.getStage().equals("ACTIVATING")) {
            GraphDefinition current = catalog.graph(before.getName());
            if (current.getSchemaVersion() != after.getSchemaVersion()) catalog.update("schema.activate/" + task.getTaskId(), current, after);
            for (int part = 0; part < before.getPartitions(); part++) storage.write(before.getId(), part,
                    StorageGateway.batch("schema.unblock/" + task.getTaskId() + "/" + part)
                            .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/generation"), TransactionEngine.text(Long.toString(after.getGeneration()))))
                            .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeEpoch"), TransactionEngine.text(Long.toString(after.getWriteEpoch()))))
                            .addMutations(Mutation.newBuilder().setKey(TransactionEngine.text("__schema/writeGate")).setDelete(true)).build());
            stage(task, "FINISHED");
        }
    }
    public void recover() {
        ByteString after = ByteString.EMPTY; String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("tasks/schema/"), after, 100, cursor);
            for (var entry : page.getEntriesList()) {
                SchemaMigrationTask task = parse(entry.getValue());
                if (task.getStage().equals("FINISHED")) cleanup(task);
                else if (!task.getStage().equals("CLEANED")) resume(task);
            }
            if (!page.getMore()) break; after = page.getLastKey(); cursor = page.getCursorId();
        } while (true);
    }
    private void cleanup(SchemaMigrationTask task) {
        GraphDefinition before = task.getBefore();
        long retainUntil = task.getRetainUntilMs();
        ByteString last = ByteString.EMPTY; String cursor = "";
        // 同一代次可能经历多个兼容 Schema 版本，必须覆盖所有版本已授权的读截止时间。
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("schema/lease/" + before.getId() + "/"), last, 100, cursor);
            for (var entry : page.getEntriesList()) {
                try {
                    LeaseHorizon horizon = LeaseHorizon.parseFrom(entry.getValue());
                    if (horizon.getGeneration() == before.getGeneration()) retainUntil = Math.max(retainUntil, horizon.getRetainUntilMs());
                } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "旧版本租约保留边界无法读取", ex); }
            }
            if (!page.getMore()) break; last = page.getLastKey(); cursor = page.getCursorId();
        } while (true);
        if (System.currentTimeMillis() <= retainUntil) return;
        for (int part = 0; part < before.getPartitions(); part++) {
            for (byte kind : new byte[] {16, 17, 18, 19, 32}) {
                ByteString prefix = ByteString.copyFrom(ByteBuffer.allocate(9).put(kind).putLong(before.getGeneration()).array());
                ByteString after = ByteString.EMPTY; String scan = "";
                do {
                    ScanPage page = storage.scan(before.getId(), part, prefix, after, 128, scan);
                    if (page.getEntriesCount() > 0) {
                        String end = Base64.getUrlEncoder().withoutPadding().encodeToString(page.getLastKey().toByteArray());
                        String id = "schema.gc/" + task.getTaskId() + "/" + part + "/" + kind + "/" + Integer.toUnsignedString(end.hashCode());
                        var command = StorageGateway.batch(id);
                        for (var entry : page.getEntriesList()) command.addMutations(Mutation.newBuilder().setKey(entry.getKey()).setDelete(true));
                        storage.write(before.getId(), part, command.build());
                    }
                    if (!page.getMore()) break; after = page.getLastKey(); scan = page.getCursorId();
                } while (true);
            }
        }
        stage(task, "CLEANED");
    }
    private SchemaMigrationTask stage(SchemaMigrationTask old, String stage) {
        SchemaMigrationTask next = old.toBuilder().setStage(stage).build();
        try { storage.write(0, 0, StorageGateway.batch("schema.stage/" + old.getTaskId() + "/" + stage)
                .addConditions(StorageGateway.condition(key(old.getTaskId()), old.toByteString())).addMutations(TransactionEngine.put(key(old.getTaskId()), next.toByteString())).build()); }
        catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        return parse(storage.get(0, 0, key(old.getTaskId())));
    }
    private static ByteString key(String id) { return TransactionEngine.text("tasks/schema/" + id); }
    private static SchemaMigrationTask parse(ByteString bytes) {
        try { return SchemaMigrationTask.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "Schema 迁移任务损坏", ex); }
    }
}
