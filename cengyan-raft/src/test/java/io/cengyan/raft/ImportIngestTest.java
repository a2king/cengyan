package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.EntityKeys;
import io.cengyan.protocol.proto.ClusterProtocol.BatchResult;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.EntityTypeCount;
import io.cengyan.protocol.proto.ClusterProtocol.ImportChunk;
import io.cengyan.protocol.proto.ClusterProtocol.ImportConflict;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPublish;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ImportIngestTest {
    private static final long GENERATION = 7;
    private static final int PERSON = 1, ORG = 2, KNOWS = 3;
    @TempDir Path directory;

    @Test void newNodeTypeIsPublishedAtOnceAndRegisteredInTheDirectory() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] first = stage(machine, "s1", nodes(PERSON, "Person", 1, 2, 3));
            byte[] second = stage(machine, "s1", nodes(PERSON, "Person", 3, 4));
            ImportPublish publish = publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, first, second);
            var prepared = machine.prepareImport(publish);
            assertEquals("OK", prepared.getCode());
            assertEquals(4, prepared.getPrimaries(), "两个上传段里重复的主键只算一次");
            assertEquals(1, prepared.getDuplicates());

            BatchResult result = machine.publishLocal(publish);
            assertEquals("OK", result.getCode(), result.getMessage());
            assertEquals(4, result.getAffected());
            assertEquals(4, count(machine, true, "Person"));
            assertEquals("Person", read(machine, EntityKeys.entity(true, GENERATION, PERSON, id(2))).getType());
            assertEquals("Person", read(machine, EntityKeys.directory(GENERATION, id(4))).getType());
            assertNull(read(machine, EntityKeys.entity(true, GENERATION, PERSON, id(5))));

            // 同一次发布重放（Raft 日志重放、客户端重试）不能再加一遍计数。
            BatchResult again = machine.publishLocal(publish);
            assertEquals(result, again);
            assertEquals(4, count(machine, true, "Person"));
        }
    }

    @Test void nodeIdBelongsToOneTypeAcrossTheGraph() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] people = stage(machine, "s1", nodes(PERSON, "Person", 1, 2));
            assertEquals("OK", machine.publishLocal(publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, people)).getCode());

            byte[] strict = stage(machine, "s2", nodes(ORG, "Org", 2, 3));
            ImportPublish rejected = publish("s2", true, ORG, "Org", ImportConflict.IMPORT_ERROR, strict);
            assertEquals("ID_CONFLICT", machine.prepareImport(rejected).getCode(), "预检就要发现 ID 已属于别的类型");
            BatchResult failed = machine.publishLocal(rejected);
            assertEquals("ID_CONFLICT", failed.getCode());
            assertNull(read(machine, EntityKeys.entity(true, GENERATION, ORG, id(3))), "失败的发布一条都不能写进去");

            byte[] lenient = stage(machine, "s3", nodes(ORG, "Org", 2, 3));
            // 即使要求覆盖，也不能把一个 Person 变成 Org。
            BatchResult skipped = machine.publishLocal(publish("s3", true, ORG, "Org", ImportConflict.IMPORT_REPLACE, lenient));
            assertEquals("OK", skipped.getCode(), skipped.getMessage());
            assertEquals(1, skipped.getAffected());
            assertEquals(1, skipped.getSkipped());
            assertNull(read(machine, EntityKeys.entity(true, GENERATION, ORG, id(2))));
            assertEquals("Person", read(machine, EntityKeys.directory(GENERATION, id(2))).getType());
            assertEquals("Org", read(machine, EntityKeys.directory(GENERATION, id(3))).getType());
            assertEquals(2, count(machine, true, "Person"));
            assertEquals(1, count(machine, true, "Org"));
        }
    }

    @Test void sameTypeConflictsFollowTheChosenPolicy() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] original = stage(machine, "s1", nodes(PERSON, "Person", 1, 2));
            machine.publishLocal(publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, original));

            byte[] error = stage(machine, "s2", nodes(PERSON, "Person", 2, 3));
            assertEquals("KEY_EXISTS", machine.publishLocal(publish("s2", true, PERSON, "Person", ImportConflict.IMPORT_ERROR, error)).getCode());

            byte[] ignore = stage(machine, "s3", List.of(node(PERSON, "Person", 2, "new"), node(PERSON, "Person", 3, "new")));
            BatchResult ignored = machine.publishLocal(publish("s3", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, ignore));
            assertEquals(1, ignored.getAffected());
            assertEquals(1, ignored.getSkipped());
            assertEquals("n2", name(machine, 2), "忽略模式不改已有的点");

            byte[] replace = stage(machine, "s4", List.of(node(PERSON, "Person", 2, "replaced"), node(PERSON, "Person", 4, "replaced")));
            BatchResult replaced = machine.publishLocal(publish("s4", true, PERSON, "Person", ImportConflict.IMPORT_REPLACE, replace));
            assertEquals(2, replaced.getAffected());
            assertEquals(1, replaced.getReplaced());
            assertEquals("replaced", name(machine, 2));
            assertEquals(4, count(machine, true, "Person"), "覆盖不增加计数");
        }
    }

    @Test void edgeTypeIsIsolatedAndCannotMoveEndpoints() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] edges = stage(machine, "s1", edges(new long[] {10, 1, 2}, new long[] {11, 2, 3}));
            BatchResult published = machine.publishLocal(publish("s1", false, KNOWS, "Knows", ImportConflict.IMPORT_IGNORE, edges));
            assertEquals(2, published.getAffected());
            assertEquals(2, count(machine, false, "Knows"));
            assertNotNull(machine.readPhysical(ImportSst.physical(EntityKeys.adjacency(true, GENERATION, KNOWS, id(1), id(10)))));
            assertNotNull(machine.readPhysical(ImportSst.physical(EntityKeys.adjacency(false, GENERATION, KNOWS, id(3), id(11)))));

            byte[] same = stage(machine, "s2", edges(new long[] {10, 1, 2}, new long[] {12, 3, 1}));
            BatchResult again = machine.publishLocal(publish("s2", false, KNOWS, "Knows", ImportConflict.IMPORT_IGNORE, same));
            assertEquals(1, again.getAffected());
            assertEquals(1, again.getSkipped());

            byte[] moved = stage(machine, "s3", edges(new long[] {11, 1, 3}));
            ImportPublish moving = publish("s3", false, KNOWS, "Knows", ImportConflict.IMPORT_REPLACE, moved);
            assertEquals("ID_CONFLICT", machine.prepareImport(moving).getCode());
            assertEquals("ID_CONFLICT", machine.publishLocal(moving).getCode());
            assertEquals(3, count(machine, false, "Knows"));
        }
    }

    @Test void keysOfAnotherTypeAreRejectedBeforeAnythingIsWritten() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] run = stage(machine, "s1", nodes(ORG, "Org", 1));
            ImportPublish publish = publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, run);
            assertEquals("INVALID_ARGUMENT", machine.prepareImport(publish).getCode());
            assertEquals("INVALID_ARGUMENT", machine.publishLocal(publish).getCode());
            assertEquals(0, count(machine, true, "Org"));
        }
    }

    @Test void aReplicaMissingARunFailsInsteadOfRecordingAResult() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] run = stage(machine, "s1", nodes(PERSON, "Person", 1));
            byte[] absent = new byte[32];
            ImportPublish publish = publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, run, absent);
            assertThrows(IOException.class, () -> machine.prepareImport(publish));
            assertThrows(IOException.class, () -> machine.publishLocal(publish));
            assertEquals(0, count(machine, true, "Person"));
        }
    }

    @Test void backgroundMergeOfRunsGivesTheSameResult() throws Exception {
        Path root = Files.createDirectories(directory.resolve("staging"));
        ImportStaging staging = new ImportStaging(root);
        List<byte[]> runs = new ArrayList<>();
        for (int i = 0; i < ImportStaging.COMPACT_RUNS + 3; i++) {
            Path file = directory.resolve("run-" + i + ".sst");
            ImportSst.write(file, nodes(PERSON, "Person", i, i + 1));
            byte[] checksum = ImportSst.sha256(file);
            runs.add(checksum);
            staging.stage(chunk("s1", checksum, 0, Files.readAllBytes(file), true), 1);
            staging.stage(chunk("s2", checksum, 0, Files.readAllBytes(file), true), 1);
        }
        ImportPublish merged = publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, runs.toArray(byte[][]::new));
        ImportPublish plain = merged.toBuilder().setSessionId("s2").build();
        assertTrue(staging.compactRuns("s1"));
        assertFalse(staging.compactRuns("s1"), "剩下的上传段不够一批");
        var status = staging.status("s1");
        assertEquals(1, status.mergedRuns());
        assertEquals(ImportStaging.COMPACT_RUNS, status.coveredRuns());
        assertEquals(3, status.runs());
        var fromMerged = staging.prepare(merged);
        var fromRuns = staging.prepare(plain);
        assertEquals("OK", fromMerged.code());
        assertEquals(ImportStaging.COMPACT_RUNS + 4, fromMerged.primaries(), "相邻上传段重叠的那个主键只算一次");
        assertEquals(fromRuns.rows(), fromMerged.rows());
        assertEquals(fromRuns.duplicates(), fromMerged.duplicates());
        assertEquals(fromRuns.data().size(), fromMerged.data().size());
        for (int i = 0; i < fromRuns.data().size(); i++) {
            assertEquals(Files.size(fromRuns.data().get(i)), Files.size(fromMerged.data().get(i)), "后台合并不能改变最终文件");
        }
        assertEquals(fromMerged.data(), staging.prepare(merged).data(), "同一份清单重复归并直接返回上次的结果");
    }

    @Test void chunksMayBeResentButNotSkipped() throws Exception {
        ImportStaging staging = new ImportStaging(Files.createDirectories(directory.resolve("staging")));
        Path file = directory.resolve("run.sst");
        ImportSst.write(file, nodes(PERSON, "Person", 1, 2, 3));
        byte[] bytes = Files.readAllBytes(file);
        byte[] checksum = ImportSst.sha256(file);
        int middle = bytes.length / 2;
        assertFalse(staging.stage(chunk("s1", checksum, 0, Arrays.copyOfRange(bytes, 0, middle), false), 1));
        assertFalse(staging.stage(chunk("s1", checksum, 0, Arrays.copyOfRange(bytes, 0, middle), false), 1), "同一块重发覆盖原来的内容");
        assertThrows(IOException.class, () -> staging.stage(chunk("s1", checksum, middle + 1, new byte[] {1}, false), 1));
        assertTrue(staging.stage(chunk("s1", checksum, middle, Arrays.copyOfRange(bytes, middle, bytes.length), true), 1));
        assertFalse(staging.stage(chunk("s1", checksum, middle, Arrays.copyOfRange(bytes, middle, bytes.length), true), 1), "已完成的上传段重发是空操作");
        assertEquals(1, staging.status("s1").runs());
    }

    /** 进程启动后的 30 分钟内不删：停机期间收不到心跳，不能因此删掉客户端还在等着发布的数据。 */
    @Test void idleSessionsAreDeletedButNotRightAfterARestart() throws Exception {
        try (RocksStateMachine machine = open()) {
            stage(machine, "s1", nodes(PERSON, "Person", 1));
            machine.heartbeatImport("s1");
            assertEquals(0, machine.expireImports(System.currentTimeMillis() + ImportStaging.IDLE_MS / 2));
            assertEquals(1, machine.importStatus("s1").runs());
            assertEquals(1, machine.expireImports(System.currentTimeMillis() + 2 * ImportStaging.IDLE_MS));
            assertEquals(0, machine.importStatus("s1").runs());
        }
    }

    /** 发布在 Leader 上生效后客户端就结束导入，落后的副本这时还没应用同一条指令，归并结果不能删。 */
    @Test void finishingKeepsPreparedFilesForReplicasThatHaveNotPublishedYet() throws Exception {
        try (RocksStateMachine machine = open()) {
            byte[] run = stage(machine, "s1", nodes(PERSON, "Person", 1, 2));
            ImportPublish publish = publish("s1", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, run);
            assertEquals("OK", machine.prepareImport(publish).getCode());
            assertFalse(machine.finishImport("s1", false), "还没发布，归并结果要留着");
            assertEquals(List.of("n" + PERSON), machine.importStatus("s1").prepared());
            assertEquals(0, machine.importStatus("s1").runs(), "上传段已经没用了");
            assertEquals(2, machine.publishLocal(publish).getAffected(), "落后的副本稍后照样能发布");
            assertTrue(machine.finishImport("s1", false));

            byte[] abandoned = stage(machine, "s2", nodes(PERSON, "Person", 3));
            assertEquals("OK", machine.prepareImport(publish("s2", true, PERSON, "Person", ImportConflict.IMPORT_IGNORE, abandoned)).getCode());
            assertTrue(machine.finishImport("s2", true), "放弃的导入连归并结果一起删");
            assertEquals(List.of(), machine.importStatus("s2").prepared());
        }
    }

    @Test void droppedGraphRejectsFurtherImport() throws Exception {
        try (RocksStateMachine machine = open()) {
            machine.beginDrop();
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, nodes(PERSON, "Person", 1));
            assertThrows(IOException.class, () -> machine.stageImport(chunk("s1", ImportSst.sha256(file), 0, Files.readAllBytes(file), true)));
        }
    }

    private RocksStateMachine open() throws IOException { return RocksStateMachine.freshReplica(directory.resolve("db")); }

    /** 写成上传段，分两块传给副本。 @return 上传段的 SHA-256 */
    private byte[] stage(RocksStateMachine machine, String session, List<ImportSst.Row> rows) throws Exception {
        Path file = Files.createTempFile(directory, "run-", ".sst");
        ImportSst.write(file, rows);
        byte[] bytes = Files.readAllBytes(file);
        byte[] checksum = ImportSst.sha256(file);
        int middle = bytes.length / 2;
        machine.stageImport(chunk(session, checksum, 0, Arrays.copyOfRange(bytes, 0, middle), false));
        machine.stageImport(chunk(session, checksum, middle, Arrays.copyOfRange(bytes, middle, bytes.length), true));
        return checksum;
    }

    private static ImportChunk chunk(String session, byte[] checksum, long offset, byte[] data, boolean last) {
        return ImportChunk.newBuilder().setChecksum(ByteString.copyFrom(checksum)).setOffset(offset).setData(ByteString.copyFrom(data))
                .setLast(last).setSessionId(session).build();
    }

    private static ImportPublish publish(String session, boolean node, int typeId, String type, ImportConflict conflict, byte[]... runs) {
        var publish = ImportPublish.newBuilder().setSessionId(session).setNode(node).setTypeId(typeId).setTypeName(type)
                .setGeneration(GENERATION).setConflict(conflict);
        for (byte[] run : runs) publish.addRuns(ByteString.copyFrom(run));
        return publish.build();
    }

    private static byte[] id(long value) { return new EntityId.Int64(value).canonicalBytes(); }

    private static Value value(long id) { return Value.newBuilder().setIntValue(id).build(); }

    private static List<ImportSst.Row> nodes(int typeId, String type, long... ids) {
        List<ImportSst.Row> rows = new ArrayList<>();
        for (long id : ids) rows.add(node(typeId, type, id, "n" + id));
        return rows;
    }

    private static ImportSst.Row node(int typeId, String type, long id, String name) {
        Entity entity = Entity.newBuilder().setNode(true).setType(type).setId(value(id)).setState("ACTIVE")
                .putProperties("name", Value.newBuilder().setStringValue(name).build()).build();
        return new ImportSst.Row(EntityKeys.entity(true, GENERATION, typeId, id(id)), entity.toByteArray());
    }

    /** 每条边 {id, 起点, 终点} 带它的出邻接和入邻接。测试里所有键都在同一个分片上。 */
    private static List<ImportSst.Row> edges(long[]... edges) {
        List<ImportSst.Row> rows = new ArrayList<>();
        for (long[] edge : edges) {
            Entity entity = Entity.newBuilder().setNode(false).setType("Knows").setId(value(edge[0])).setSource(value(edge[1]))
                    .setTarget(value(edge[2])).setState("ACTIVE").build();
            byte[] hint = entity.toByteArray();
            rows.add(new ImportSst.Row(EntityKeys.entity(false, GENERATION, KNOWS, id(edge[0])), entity.toByteArray()));
            rows.add(new ImportSst.Row(EntityKeys.adjacency(true, GENERATION, KNOWS, id(edge[1]), id(edge[0])), hint));
            rows.add(new ImportSst.Row(EntityKeys.adjacency(false, GENERATION, KNOWS, id(edge[2]), id(edge[0])), hint));
        }
        return rows;
    }

    private static Entity read(RocksStateMachine machine, byte[] logical) throws Exception {
        byte[] cell = machine.readPhysical(ImportSst.physical(logical));
        if (cell == null) return null;
        VersionedCell parsed = VersionedCell.parseFrom(cell);
        return Entity.parseFrom(parsed.getVersions(parsed.getVersionsCount() - 1).getValue());
    }

    private static String name(RocksStateMachine machine, long id) throws Exception {
        return read(machine, EntityKeys.entity(true, GENERATION, PERSON, id(id))).getPropertiesOrThrow("name").getStringValue();
    }

    private static long count(RocksStateMachine machine, boolean node, String type) throws Exception {
        return machine.readCounts(GENERATION).getTypesList().stream().filter(item -> item.getNode() == node && item.getType().equals(type))
                .mapToLong(EntityTypeCount::getCount).sum();
    }
}
