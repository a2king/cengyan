package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.ImportChunk;
import io.cengyan.protocol.proto.ClusterProtocol.ImportConflict;
import io.cengyan.protocol.proto.ClusterProtocol.ImportDirective;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ImportIngestTest {
    @TempDir Path directory;

    @Test void stagingFilesAreRemovedThirtyMinutesAfterTheLastHeartbeat() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            long started = 1_700_000_000_000L;
            byte[] bytes = Files.readAllBytes(file);
            machine.stageImport(ImportChunk.newBuilder().setChecksum(ByteString.copyFrom(ImportSst.sha256(file)))
                    .setOffset(0).setData(ByteString.copyFrom(bytes)).setLast(true).setSessionId("live-job").build(), started);
            assertEquals(0, machine.expireImports(started + ImportSessions.IDLE_MS - 1));
            machine.heartbeatImport("live-job", started + 60_000);
            assertEquals(0, machine.expireImports(started + 60_000 + ImportSessions.IDLE_MS - 1));
            assertTrue(machine.expireImports(started + 60_000 + ImportSessions.IDLE_MS) > 0);
            assertEquals(0, machine.expireImports(started + 60_000 + ImportSessions.IDLE_MS), "重复清理不再删文件");
        }
    }

    @Test void droppedGraphRejectsFurtherImport() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, file);
            machine.beginDrop();
            var rejected = machine.applyImport(directive(file, ImportConflict.IMPORT_IGNORE, false));
            assertEquals("GRAPH_STATE", rejected.getCode());
            assertEquals(0, counts(machine));
            assertThrows(IOException.class, () -> machine.stageImport(chunk(ImportSst.sha256(file), 0, new byte[] {1}, false), 1L));
            machine.heartbeatImport("import-test", 1L);
        }
    }

    @Test void emptyTypeIngestIsIdempotentAndCountsBaselineCells() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            byte[] first = node(7, 1, "Person");
            byte[] second = node(7, 2, "Person");
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, List.of(new ImportSst.Row(first, entity(true, "Person", 1)), new ImportSst.Row(second, entity(true, "Person", 2))));
            stage(machine, file);
            var directive = directive(file, ImportConflict.IMPORT_IGNORE, false);
            var applied = machine.applyImport(directive);
            assertEquals("OK", applied.getCode());
            assertEquals(2, applied.getAffected());
            assertEquals(2, counts(machine));
            assertEquals(2, machine.applyImport(directive).getAffected(), "同一校验和重放不能再增加计数");
            assertEquals(2, counts(machine));
            try (var files = Files.walk(directory.resolve("db").resolve("import-staging"))) {
                assertTrue(files.noneMatch(path -> {
                    String name = path.getFileName().toString();
                    return name.endsWith(".sst") || name.endsWith(".apply.sst") || name.endsWith(".link");
                }), "提交后不应再保留导入文件");
            }
            assertTrue(machine.finishImport("import-test"));
            var undone = machine.applyImport(directive(file, ImportConflict.IMPORT_IGNORE, true));
            assertEquals("OK", undone.getCode());
            assertEquals(0, counts(machine));
            assertNull(machine.readPhysical(ImportSst.physical(first)));
        }
    }

    @Test void ignoreSkipsExistingKeysAndErrorStops() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            Path first = directory.resolve("first.sst");
            ImportSst.write(first, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, first);
            assertEquals("OK", machine.applyImport(directive(first, ImportConflict.IMPORT_IGNORE, false)).getCode());
            assertTrue(machine.finishImport("import-test"));
            Path second = directory.resolve("second.sst");
            ImportSst.write(second, List.of(
                    new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1)),
                    new ImportSst.Row(node(7, 3, "Person"), entity(true, "Person", 3))));
            stage(machine, second);
            var ignored = machine.applyImport(directive(second, ImportConflict.IMPORT_IGNORE, false));
            assertEquals("OK", ignored.getCode());
            assertEquals(1, ignored.getAffected());
            assertEquals(2, counts(machine));
            Path third = directory.resolve("third.sst");
            ImportSst.write(third, List.of(new ImportSst.Row(node(7, 3, "Person"), entity(true, "Person", 3))));
            stage(machine, third);
            var rejected = machine.applyImport(directive(third, ImportConflict.IMPORT_ERROR, false));
            assertEquals("KEY_EXISTS", rejected.getCode());
            assertEquals(2, counts(machine));
        }
    }

    @Test void sameSessionAppendDoesNotProbeExistingKeys() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            Path first = directory.resolve("first.sst");
            ImportSst.write(first, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, first);
            assertEquals(1, machine.applyImport(directive(first, ImportConflict.IMPORT_IGNORE, false)).getAffected());
            Path second = directory.resolve("second.sst");
            ImportSst.write(second, List.of(
                    new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1)),
                    new ImportSst.Row(node(7, 3, "Person"), entity(true, "Person", 3))));
            stage(machine, second);
            var appended = machine.applyImport(directive(second, ImportConflict.IMPORT_IGNORE, false));
            assertEquals(2, appended.getAffected());
            assertEquals(3, counts(machine));
        }
    }

    @Test void commitReturnsBeforeBackgroundCompactionFinishes() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            machine.importCompactWaveBytes = Long.MAX_VALUE;
            machine.importCompactIdleMs = Long.MAX_VALUE;
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, file);
            assertEquals("OK", machine.applyImport(directive(file, ImportConflict.IMPORT_IGNORE, false)).getCode());
            assertEquals(0, machine.completedCompactions());
            assertTrue(machine.finishImport("import-test"));
            assertEquals(0, machine.completedCompactions());
        }
    }

    @Test void overlappingImportCompactsLevelZeroInBackground() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            machine.importCompactWaveBytes = Long.MAX_VALUE;
            machine.importCompactIdleMs = Long.MAX_VALUE;
            Path first = directory.resolve("first.sst");
            ImportSst.write(first, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, first);
            assertEquals("OK", machine.applyImport(directive(first, ImportConflict.IMPORT_IGNORE, false)).getCode());
            machine.importCompactWaveBytes = 1;
            Path second = directory.resolve("second.sst");
            ImportSst.write(second, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, second);
            assertEquals("OK", machine.applyImport(directive(second, ImportConflict.IMPORT_IGNORE, false)).getCode());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (machine.uncompactedBytes() > 0 && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals(0, machine.uncompactedBytes());
        }
    }

    @Test void hourlyTailCompactsWithoutAFullWave() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            machine.importCompactWaveBytes = Long.MAX_VALUE;
            machine.importCompactIdleMs = 0;
            Path file = directory.resolve("nodes.sst");
            ImportSst.write(file, List.of(new ImportSst.Row(node(7, 1, "Person"), entity(true, "Person", 1))));
            stage(machine, file);
            assertEquals("OK", machine.applyImport(directive(file, ImportConflict.IMPORT_IGNORE, false)).getCode());
            assertTrue(machine.uncompactedBytes() > 0);
            machine.compactIfDue(System.currentTimeMillis());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (machine.uncompactedBytes() > 0 && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals(0, machine.uncompactedBytes());
        }
    }

    @Test void replaceMovesAdjacencyAndUndoRemovesInsertedKeys() throws Exception {
        try (RocksStateMachine machine = RocksStateMachine.freshReplica(directory.resolve("db"))) {
            byte[] edgeId = new EntityId.Int64(9).canonicalBytes();
            byte[] from = new EntityId.Int64(1).canonicalBytes();
            byte[] to = new EntityId.Int64(2).canonicalBytes();
            byte[] moved = new EntityId.Int64(3).canonicalBytes();
            Path created = directory.resolve("edge.sst");
            ImportSst.write(created, List.of(
                    new ImportSst.Row(edge(7, edgeId), edgeEntity(9, 1, 2)),
                    new ImportSst.Row(ImportSst.adjacency(true, 7, from, edgeId), edgeEntity(9, 1, 2)),
                    new ImportSst.Row(ImportSst.adjacency(false, 7, to, edgeId), edgeEntity(9, 1, 2))));
            stage(machine, created);
            assertEquals(1, machine.applyImport(directive(created, ImportConflict.IMPORT_IGNORE, false)).getAffected());
            assertEquals(1, counts(machine));

            Path replaced = directory.resolve("moved.sst");
            ImportSst.write(replaced, List.of(
                    new ImportSst.Row(edge(7, edgeId), edgeEntity(9, 1, 3)),
                    new ImportSst.Row(ImportSst.adjacency(true, 7, from, edgeId), edgeEntity(9, 1, 3)),
                    new ImportSst.Row(ImportSst.adjacency(false, 7, moved, edgeId), edgeEntity(9, 1, 3))));
            stage(machine, replaced);
            var result = machine.applyImport(directive(replaced, ImportConflict.IMPORT_REPLACE, false));
            assertEquals("OK", result.getCode());
            assertEquals(0, result.getAffected(), "覆盖已有边不增加计数");
            assertEquals(1, counts(machine));
            assertNull(machine.readPhysical(ImportSst.physical(ImportSst.adjacency(false, 7, to, edgeId))));
            assertNotNull(machine.readPhysical(ImportSst.physical(ImportSst.adjacency(false, 7, moved, edgeId))));
        }
    }

    private static void stage(RocksStateMachine machine, Path file) throws Exception {
        byte[] bytes = Files.readAllBytes(file);
        byte[] checksum = ImportSst.sha256(file);
        int midpoint = bytes.length / 2;
        machine.stageImport(chunk(checksum, 0, java.util.Arrays.copyOfRange(bytes, 0, midpoint), false));
        machine.stageImport(chunk(checksum, midpoint, java.util.Arrays.copyOfRange(bytes, midpoint, bytes.length), true));
    }

    private static ImportChunk chunk(byte[] checksum, int offset, byte[] data, boolean last) {
        return ImportChunk.newBuilder().setChecksum(ByteString.copyFrom(checksum)).setOffset(offset).setData(ByteString.copyFrom(data)).setLast(last).setSessionId("import-test").build();
    }

    private static ImportDirective directive(Path file, ImportConflict conflict, boolean undo) throws Exception {
        return ImportDirective.newBuilder().setChecksum(ByteString.copyFrom(ImportSst.sha256(file))).setConflict(conflict).setUndo(undo).setGeneration(7).build();
    }

    private static long counts(RocksStateMachine machine) throws Exception {
        var counts = machine.readCounts(7);
        return counts.getTotalNodes() + counts.getTotalEdges();
    }

    private static byte[] node(long generation, long id, String type) {
        byte[] encoded = new EntityId.Int64(id).canonicalBytes();
        return ByteBuffer.allocate(9 + encoded.length).put((byte) 16).putLong(generation).put(encoded).array();
    }

    private static byte[] edge(long generation, byte[] encoded) {
        return ByteBuffer.allocate(9 + encoded.length).put((byte) 17).putLong(generation).put(encoded).array();
    }

    private static byte[] entity(boolean node, String type, long id) {
        return Entity.newBuilder().setNode(node).setType(type).setId(Value.newBuilder().setIntValue(id)).setState("ACTIVE").build().toByteArray();
    }

    private static byte[] edgeEntity(long id, long source, long target) {
        return Entity.newBuilder().setNode(false).setType("Knows").setState("ACTIVE")
                .setId(Value.newBuilder().setIntValue(id))
                .setSource(Value.newBuilder().setIntValue(source))
                .setTarget(Value.newBuilder().setIntValue(target)).build().toByteArray();
    }
}
