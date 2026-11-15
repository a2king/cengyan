package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.ServerSocket;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RaftConditionsTest {
    @TempDir Path directory;

    @Test @Timeout(40)
    void 预期状态原子校验以及Raft清理到期幂等记录() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var group = RaftGroup.valueOf(GroupIds.data("conditional-test", 1, 0),
                RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + port).build());
        try (var node = new RaftNode("conditional-test", "s1", "127.0.0.1", port, directory, group, RaftSettings.defaults());
             var client = node.client(group)) {
            var first = WriteBatch.newBuilder().setRequestId("create").setExpiresAtMs(100)
                    .addConditions(Condition.newBuilder().setKey(text("key")).setExpectedPresent(false))
                    .addMutations(Mutation.newBuilder().setKey(text("key")).setValue(text("value"))).build();
            var reply = client.io().send(RocksStateMachine.message(first.toByteArray()));
            assertTrue(reply.isSuccess());
            assertEquals("OK", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());

            var conflicting = first.toBuilder().setRequestId("conflict").build();
            reply = client.io().send(RocksStateMachine.message(conflicting.toByteArray()));
            assertEquals("WRITE_CONFLICT", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());

            var update = WriteBatch.newBuilder().setRequestId("update").setExpiresAtMs(1000)
                    .addConditions(Condition.newBuilder().setKey(text("key")).setExpectedPresent(true).setExpectedValue(text("value")))
                    .addMutations(Mutation.newBuilder().setKey(text("key")).setValue(text("new-value"))).build();
            reply = client.io().send(RocksStateMachine.message(update.toByteArray()));
            assertEquals("OK", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());

            // 未清理前同一个 requestId 始终返回原结果，不因各副本本地时钟自行过期。
            reply = client.io().send(RocksStateMachine.message(first.toByteArray()));
            assertEquals("OK", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());
            var purge = WriteBatch.newBuilder().setRequestId("purge-1").setExpiresAtMs(2000)
                    .addPurgeRequestIds("create").addPurgeRequestIds("update").setPurgeBeforeMs(100).build();
            reply = client.io().send(RocksStateMachine.message(purge.toByteArray()));
            assertEquals(1, BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getAffected());
            reply = client.io().send(RocksStateMachine.message(update.toByteArray()));
            assertEquals("OK", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());
            // create 的记录已清理，现在真正重新校验预留条件，不能重放覆盖数据。
            reply = client.io().send(RocksStateMachine.message(first.toByteArray()));
            assertEquals("WRITE_CONFLICT", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());
            var read = client.io().sendReadOnly(RocksStateMachine.message(ReadKey.newBuilder().setKey(text("key")).build().toByteArray()));
            assertEquals("new-value", StoredValue.parseFrom(read.getMessage().getContent().toByteArray()).getValue().toStringUtf8());
        }
    }

    private static ByteString text(String value) { return ByteString.copyFromUtf8(value); }
}
