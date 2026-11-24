package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.IndexFamilyName;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 每个索引一个 Column Family：创建和删除都走复制命令，删除不逐键写入。 */
class IndexColumnFamilyTest {
    @TempDir Path home;
    private long sequence;

    @Test @Timeout(60)
    void 新索引写进独立CF删除后读写都为空且重启不复活() throws Exception {
        Path directory = home.resolve("db");
        byte[] prefix = prefix(7, "req-index-1");
        String family = IndexFamilyName.of("req-index-1");
        byte[] first = key(prefix, "a"), second = key(prefix, "b");
        try (var machine = RocksStateMachine.freshReplica(directory)) {
            assertEquals("OK", apply(machine, batch("create").addCreateIndexPrefixes(ByteString.copyFrom(prefix))).getCode());
            assertTrue(machine.physicalUsage().containsKey(family));
            // 唯一索引的条件写：条件读也必须路由到索引 CF。
            assertEquals("OK", apply(machine, batch("w1").addConditions(absent(first)).addMutations(put(first, "owner-a"))).getCode());
            assertEquals("WRITE_CONFLICT", apply(machine, batch("w1-again").addConditions(absent(first)).addMutations(put(first, "owner-x"))).getCode());
            assertEquals("OK", apply(machine, batch("w2").addMutations(put(second, "owner-b"))).getCode());
            assertEquals("owner-a", get(machine, first).getValue().toStringUtf8());
            assertNull(machine.readPhysical(physical(first)), "新索引键不能写进默认 CF");
            ScanPage page = scan(machine, prefix);
            assertEquals(2, page.getEntriesCount());
            assertArrayEquals(first, page.getEntries(0).getKey().toByteArray());

            assertEquals("OK", apply(machine, batch("drop").addDropIndexPrefixes(ByteString.copyFrom(prefix))).getCode());
            assertFalse(get(machine, first).getFound());
            assertEquals(0, scan(machine, prefix).getEntriesCount());
            assertFalse(machine.physicalUsage().containsKey(family), "删除后 Column Family 应已物理删除");
            // 删除前规划的旧 Schema 写入迟到：丢弃，不重新建出这个索引。
            assertEquals("OK", apply(machine, batch("late").addMutations(put(key(prefix, "c"), "owner-c"))).getCode());
            assertFalse(get(machine, key(prefix, "c")).getFound());
            assertEquals("OK", apply(machine, batch("recreate").addCreateIndexPrefixes(ByteString.copyFrom(prefix))).getCode());
            assertFalse(machine.physicalUsage().containsKey(family), "已删除的索引身份不能被重新创建");
            // 同一 requestId 重放返回首次结果。
            assertEquals("OK", apply(machine, batch("drop").addDropIndexPrefixes(ByteString.copyFrom(prefix))).getCode());
        }
        try (var machine = RocksStateMachine.reopenSealedReplica(directory)) {
            assertFalse(get(machine, first).getFound());
            assertFalse(machine.physicalUsage().containsKey(family));
            assertEquals("OK", apply(machine, batch("after-restart").addMutations(put(first, "owner-z"))).getCode());
            assertFalse(get(machine, first).getFound(), "重启后删除登记仍然有效");
        }
    }

    @Test @Timeout(60)
    void 升级前写在默认CF的旧索引用范围删除且不碰其他键() throws Exception {
        byte[] legacy = prefix(3, "old-index");
        byte[] other = prefix(3, "old-index-2");
        // 主记录必须是真实的单元格，否则计数元数据无法解析。
        Entity entity = Entity.newBuilder().setNode(true).setType("Company").build();
        byte[] node = ByteBuffer.allocate(10).put((byte) 16).putLong(3).put((byte) 1).array();
        try (var machine = RocksStateMachine.freshReplica(home.resolve("db"))) {
            assertEquals("OK", apply(machine, batch("legacy").addMutations(put(key(legacy, "x"), "owner-x"))
                    .addMutations(put(key(other, "y"), "owner-y"))
                    .addMutations(Mutation.newBuilder().setKey(ByteString.copyFrom(node))
                            .setValue(ByteString.copyFrom(ImportSst.cell(entity.toByteString().toByteArray()))).build())).getCode());
            assertNotNull(machine.readPhysical(physical(key(legacy, "x"))), "没有登记 CF 的索引仍写默认 CF");
            assertEquals("OK", apply(machine, batch("drop-legacy").addDropIndexPrefixes(ByteString.copyFrom(legacy))).getCode());
            assertFalse(get(machine, key(legacy, "x")).getFound());
            assertNull(machine.readPhysical(physical(key(legacy, "x"))));
            assertEquals("owner-y", get(machine, key(other, "y")).getValue().toStringUtf8(), "前缀相近的其他索引不能被范围删除波及");
            assertTrue(get(machine, node).getFound());
        }
    }

    @Test @Timeout(60)
    void 索引CF指令校验前缀并且不能混入普通写入() throws Exception {
        byte[] prefix = prefix(1, "req-bad");
        try (var machine = RocksStateMachine.freshReplica(home.resolve("db"))) {
            assertEquals("INVALID_ARGUMENT", apply(machine, batch("bad-prefix")
                    .addCreateIndexPrefixes(ByteString.copyFrom(key(prefix, "value")))).getCode());
            assertEquals("INVALID_ARGUMENT", apply(machine, batch("mixed").addCreateIndexPrefixes(ByteString.copyFrom(prefix))
                    .addMutations(put(key(prefix, "a"), "owner"))).getCode());
            assertFalse(machine.physicalUsage().containsKey(IndexFamilyName.of("req-bad")), "被拒绝的指令不能留下 Column Family");
        }
    }
    @Test @Timeout(60)
    void 打开分片时补做停在刷盘之后的物理删除并拒绝不认识的CF() throws Exception {
        Path pending = home.resolve("pending");
        String family = IndexFamilyName.of("req-pending");
        byte[] prefix = prefix(1, "req-pending");
        createRaw(pending, family, raw -> {
            raw.db().put(raw.handles().get(1), key(prefix, "a"), "owner".getBytes(StandardCharsets.UTF_8));
            raw.db().put(raw.handles().get(0), registry(family), new byte[] {2});
        });
        try (var machine = RocksStateMachine.reopenSealedReplica(pending)) {
            assertFalse(machine.physicalUsage().containsKey(family), "DROPPED 登记的 CF 打开时就删除");
            assertFalse(get(machine, key(prefix, "a")).getFound());
        }
        try (Options options = new Options()) {
            assertTrue(RocksDB.listColumnFamilies(options, pending.toString()).stream()
                    .noneMatch(name -> new String(name, StandardCharsets.UTF_8).equals(family)));
        }
        Path unknown = home.resolve("unknown");
        createRaw(unknown, "nodes", raw -> raw.db().put(raw.handles().get(1), new byte[] {16, 1}, new byte[] {1}));
        var failure = assertThrows(IOException.class, () -> RocksStateMachine.reopenSealedReplica(unknown));
        assertTrue(failure.getMessage().contains("nodes"), failure.getMessage());
    }
    @Test @Timeout(90)
    void 快照安装把索引CF和删除登记一起带到新副本() throws Exception {
        int a, b;
        try (var one = new ServerSocket(0); var two = new ServerSocket(0)) { a = one.getLocalPort(); b = two.getLocalPort(); }
        RaftPeer first = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + a).build();
        RaftPeer second = RaftPeer.newBuilder().setId("s2").setAddress("127.0.0.1:" + b)
                .setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
        var id = GroupIds.data("index-snapshot", 1, 0);
        var initial = RaftGroup.valueOf(id, first);
        var settings = new RaftSettings(1024 * 1024, 10, 0, 1);
        byte[] kept = prefix(1, "req-kept"), gone = prefix(1, "req-gone");
        try (var source = new RaftNode("index-snapshot", "s1", "127.0.0.1", a, home.resolve("s1"), initial, settings)) {
            // 首次选主要 3~5 秒，客户端的重试窗口比这短，必须等 Leader 就绪再写。
            awaitLeader(source, id);
            try (var client = source.client(initial)) {
            assertEquals("OK", send(client, batch("c1").addCreateIndexPrefixes(ByteString.copyFrom(kept))).getCode());
            assertEquals("OK", send(client, batch("c2").addCreateIndexPrefixes(ByteString.copyFrom(gone))).getCode());
            assertEquals("OK", send(client, batch("w").addMutations(put(key(kept, "a"), "owner-a"))
                    .addMutations(put(key(gone, "b"), "owner-b"))).getCode());
            assertEquals("OK", send(client, batch("d").addDropIndexPrefixes(ByteString.copyFrom(gone))).getCode());
            var division = source.server().getDivision(id);
            long snapshot = division.getStateMachine().takeSnapshot();
            assertTrue(snapshot > 0);
            division.getRaftLog().purge(snapshot).get(10, TimeUnit.SECONDS);
            var targetGroup = RaftGroup.valueOf(id, first, second);
            try (var target = new RaftNode("index-snapshot", "s2", "127.0.0.1", b, home.resolve("s2"), targetGroup, settings)) {
                assertTrue(client.admin().setConfiguration(List.of(first), List.of(second)).isSuccess());
                RaftPeer voting = RaftPeer.newBuilder().setId("s2").setAddress(second.getAddress()).build();
                assertTrue(client.admin().setConfiguration(List.of(first, voting), List.of()).isSuccess());
                awaitApplied(target, id, snapshot);
                var machine = (RocksStateMachine) target.server().getDivision(id).getStateMachine();
                assertEquals("owner-a", get(machine, key(kept, "a")).getValue().toStringUtf8());
                assertFalse(get(machine, key(gone, "b")).getFound());
                assertTrue(machine.physicalUsage().containsKey(IndexFamilyName.of("req-kept")));
                assertFalse(machine.physicalUsage().containsKey(IndexFamilyName.of("req-gone")));
                // 装完快照后，继续复制来的索引写入也要落到同一个 CF。
                assertEquals("OK", send(client, batch("w2").addMutations(put(key(kept, "c"), "owner-c"))).getCode());
                awaitApplied(target, id, source.server().getDivision(id).getInfo().getLastAppliedIndex());
                assertEquals("owner-c", get(machine, key(kept, "c")).getValue().toStringUtf8());
                assertNull(machine.readPhysical(physical(key(kept, "c"))));
            }
            }
        }
    }

    private record Raw(RocksDB db, List<ColumnFamilyHandle> handles) { }
    private interface RawWork { void run(Raw raw) throws Exception; }

    /** 直接用 RocksDB 造一个含 default 和指定 CF 的分片库，模拟上一次进程留下的磁盘状态。 */
    private static void createRaw(Path directory, String family, RawWork work) throws Exception {
        try (var options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             var familyOptions = new org.rocksdb.ColumnFamilyOptions()) {
            List<ColumnFamilyDescriptor> descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions),
                    new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8), familyOptions));
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            try (RocksDB db = RocksDB.open(options, directory.toString(), descriptors, handles)) {
                try { work.run(new Raw(db, handles)); }
                finally { for (ColumnFamilyHandle handle : handles) handle.close(); }
            }
        }
    }

    private static void awaitLeader(RaftNode node, org.apache.ratis.protocol.RaftGroupId id) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!node.server().getDivision(id).getInfo().isLeaderReady() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(node.server().getDivision(id).getInfo().isLeaderReady(), "单节点分片没有选出 Leader");
    }

    private static void awaitApplied(RaftNode node, org.apache.ratis.protocol.RaftGroupId id, long index) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (node.server().getDivision(id).getInfo().getLastAppliedIndex() < index && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(node.server().getDivision(id).getInfo().getLastAppliedIndex() >= index, "新副本没有追上 Leader");
    }

    private BatchResult apply(RocksStateMachine machine, WriteBatch.Builder command) throws IOException {
        return machine.applyReplica(command.build(), ++sequence);
    }

    private static BatchResult send(org.apache.ratis.client.RaftClient client, WriteBatch.Builder command) throws Exception {
        var reply = client.io().send(RocksStateMachine.message(command.build().toByteArray()));
        assertTrue(reply.isSuccess(), reply.toString());
        return BatchResult.parseFrom(reply.getMessage().getContent().toByteArray());
    }

    private static StoredValue get(RocksStateMachine machine, byte[] key) throws Exception {
        var reply = machine.query(RocksStateMachine.message(ReadKey.newBuilder().setKey(ByteString.copyFrom(key)).build().toByteArray())).join();
        return StoredValue.parseFrom(reply.getContent().toByteArray());
    }

    private static ScanPage scan(RocksStateMachine machine, byte[] prefix) throws Exception {
        var request = ReadKey.newBuilder().setScan(true).setKey(ByteString.copyFrom(prefix)).setLimit(100).setMaxBytes(1 << 20).build();
        return ScanPage.parseFrom(machine.query(RocksStateMachine.message(request.toByteArray())).join().getContent().toByteArray());
    }

    private static WriteBatch.Builder batch(String requestId) {
        return WriteBatch.newBuilder().setRequestId(requestId).setExpiresAtMs(2_000_000_000_000L);
    }

    private static Condition absent(byte[] key) {
        return Condition.newBuilder().setKey(ByteString.copyFrom(key)).setExpectedPresent(false).build();
    }

    private static Mutation put(byte[] key, String value) {
        return Mutation.newBuilder().setKey(ByteString.copyFrom(key)).setValue(ByteString.copyFromUtf8(value)).build();
    }

    /** 与 EntityCodec.indexPrefix 相同的布局：[32][generation][len][indexId]。 */
    private static byte[] prefix(long generation, String identity) {
        byte[] name = identity.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(13 + name.length).put((byte) 32).putLong(generation).putInt(name.length).put(name).array();
    }

    private static byte[] key(byte[] prefix, String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(prefix.length + 4 + encoded.length).put(prefix).putInt(encoded.length).put(encoded).array();
    }

    private static byte[] physical(byte[] logical) { return ImportSst.physical(logical); }

    private static byte[] registry(String family) {
        byte[] name = family.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + name.length).put((byte) 6).put(name).array();
    }
}