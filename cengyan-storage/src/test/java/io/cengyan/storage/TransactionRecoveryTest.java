package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TransactionRecoveryTest {
    @TempDir Path home;
    @Test @Timeout(90)
    void 后参与者冲突时清理已准备版本且不覆盖旧数据() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("rollback", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
        try (var server = new StorageServer(config); var storage = new StorageGateway("rollback", List.of(config.rpcAddress()))) {
            var graph = new DistributedCatalog(storage).create("graph", "G", 2, 1, IdType.INT64, IdType.INT64);
            var tx = new TransactionEngine(storage);
            ByteString a = TransactionEngine.text("entity/a"), b = TransactionEngine.text("entity/b");
            var seed = plan("seed", graph.getId(),
                    participant(0, change(a, null, "old-a")), participant(1, change(b, null, "old-b")));
            assertEquals(TransactionState.COMMITTED, tx.execute(seed).getState());
            ByteString oldA = storage.get(graph.getId(), 0, a);
            var conflict = plan("conflict", graph.getId(), participant(0, change(a, oldA, "new-a")),
                    participant(1, change(b, null, "new-b")));
            assertEquals(TransactionState.ABORTED, tx.execute(conflict).getState());
            var scope = tx.new QueryScope(graph.getId(), 2);
            assertEquals("old-a", scope.value(storage.get(graph.getId(), 0, a)).toStringUtf8());
            assertEquals("old-b", scope.value(storage.get(graph.getId(), 1, b)).toStringUtf8());
            assertEquals(1, VersionedCell.parseFrom(storage.get(graph.getId(), 0, a)).getVersionsCount());
            assertTrue(tx.record(graph.getId(), 0, "conflict").getFinalized());
            var afterAbort = plan("after-abort", graph.getId(), participant(0, change(a, storage.get(graph.getId(), 0, a), "next-a")));
            assertEquals(TransactionState.COMMITTED, tx.execute(afterAbort).getState());
            // 同一读视图不能在另一分片读取时悄悄纳入后来提交的事务。
            assertEquals("old-a", scope.value(storage.get(graph.getId(), 0, a)).toStringUtf8());
            assertEquals("next-a", tx.new QueryScope(graph.getId(), 2).value(storage.get(graph.getId(), 0, a)).toStringUtf8());
            var expired = plan("expired", graph.getId(), participant(0, change(a, storage.get(graph.getId(), 0, a), "bad")))
                    .toBuilder().setDeadlineMs(System.currentTimeMillis() - 1).build();
            assertEquals(TransactionState.ABORTED, tx.execute(expired).getState());
            // 模拟超过去重保留窗口与最长查询保护期，不允许在活动查询中提前物化。
            long gcTime = System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(26);
            new VersionMaintenance(storage).collect(tx.record(graph.getId(), 0, "after-abort"), gcTime);
            VersionedCell compacted = VersionedCell.parseFrom(storage.get(graph.getId(), 0, a));
            assertEquals(1, compacted.getVersionsCount());
            assertTrue(compacted.getVersions(0).getBaseline());
            assertEquals("next-a", tx.new QueryScope(graph.getId(), 2).value(storage.get(graph.getId(), 0, a)).toStringUtf8());
            assertEquals(0, tx.record(graph.getId(), 0, "after-abort").getPlan().getParticipantsCount());
        }
    }
    private static TransactionPlan plan(String id, long graph, ParticipantPlan... participants) {
        return TransactionPlan.newBuilder().setTransactionId(id).setGraphId(graph).setDeadlineMs(System.currentTimeMillis() + 30000)
                .addAllParticipants(Arrays.asList(participants)).build();
    }
    private static ParticipantPlan participant(int part, ConditionalMutation... changes) {
        return ParticipantPlan.newBuilder().setPartId(part).addAllChanges(Arrays.asList(changes)).build();
    }
    private static ConditionalMutation change(ByteString key, ByteString old, String value) {
        var builder = ConditionalMutation.newBuilder().setKey(key).setValue(TransactionEngine.text(value)).setExpectedPresent(old != null);
        if (old != null) builder.setExpectedCell(old);
        return builder.build();
    }
}
