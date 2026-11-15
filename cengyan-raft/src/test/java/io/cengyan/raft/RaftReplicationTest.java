package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class RaftReplicationTest {
    @TempDir Path directory;

    @Test @Timeout(90)
    void 三节点复制选主故障转移和快照恢复() throws Exception {
        List<RaftPeer> peers = new ArrayList<>();
        // 启动前持有所有端口，防止操作系统重复分配。
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                var socket = new ServerSocket(0);
                reservations.add(socket);
                peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
            }
        } finally { for (var socket : reservations) socket.close(); }
        RaftGroup group = RaftGroup.valueOf(GroupIds.data("integration", 1, 0), peers);
        var servers = new ArrayList<RaftServer>();
        var settings = new RaftSettings(4L * 1024 * 1024, 1000, 10, 2);
        var clientProperties = settings.properties(directory.resolve("client-unused"), "127.0.0.1", 0);
        try (var client = RaftClient.newBuilder().setRaftGroup(group).setProperties(clientProperties)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(60, TimeDuration.valueOf(200, TimeUnit.MILLISECONDS))).build()) {
            for (int i = 0; i < peers.size(); i++) {
                var server = newServer(peers.get(i), group, settings, i);
                servers.add(server);
                server.start();
            }
            assertEquals("OK", write(client, "r1", "one").getCode());
            assertEquals("one", read(client));
            // Count Store 必须随真实 Raft 复制，不能只存在 Leader 的内存中。
            writeCountedEntity(client);
            assertEquals(1, readCounts(client).getTotalNodes());
            assertEquals("OK", write(client, "r1", "one").getCode());
            assertEquals("REQUEST_CONFLICT", write(client, "r1", "different").getCode());
            assertEquals("one", read(client));

            int leader = -1;
            for (int i = 0; i < servers.size(); i++) {
                if (servers.get(i).getDivision(group.getGroupId()).getInfo().isLeader()) leader = i;
            }
            assertTrue(leader >= 0);
            servers.get(leader).close();
            servers.set(leader, null);
            assertEquals("OK", write(client, "r2", "two").getCode());
            assertEquals("two", read(client));
            assertEquals(1, readCounts(client).getTotalNodes());
            writeCountedEntity(client);
            assertEquals(1, readCounts(client).getTotalNodes());

            // 写成功的 Leader 必定已应用，通过其线性一致读确定状态后生成 Checkpoint。
            RaftPeerId currentLeader = client.getLeaderId();
            for (var server : servers) {
                if (server != null && server.getId().equals(currentLeader)) {
                    assertTrue(server.getDivision(group.getGroupId()).getStateMachine().takeSnapshot() > 0);
                }
            }
            for (var server : servers) if (server != null) server.close();
            servers.clear();
            for (int i = 0; i < peers.size(); i++) {
                var server = newServer(peers.get(i), group, settings, i);
                servers.add(server);
                server.start();
            }
            assertEquals("two", read(client));
            assertEquals(1, readCounts(client).getTotalNodes());
            assertEquals("OK", write(client, "r2", "two").getCode());
            assertEquals("REQUEST_CONFLICT", write(client, "r2", "changed-after-restart").getCode());
            assertEquals("two", read(client));
        } finally {
            for (var server : servers) if (server != null) server.close();
        }
    }

    private RaftServer newServer(RaftPeer peer, RaftGroup group, RaftSettings settings, int number) throws Exception {
        int port = Integer.parseInt(peer.getAddress().substring(peer.getAddress().lastIndexOf(':') + 1));
        return RaftServer.newBuilder().setServerId(peer.getId()).setGroup(group).setStateMachine(new RocksStateMachine())
                .setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER)
                .setProperties(settings.properties(directory.resolve("s" + number), "127.0.0.1", port)).build();
    }

    private static BatchResult write(RaftClient client, String request, String value) throws Exception {
        var command = WriteBatch.newBuilder().setRequestId(request).setExpiresAtMs(2_000_000_000_000L)
                .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("key")).setValue(ByteString.copyFromUtf8(value))).build();
        var reply = client.io().send(RocksStateMachine.message(command.toByteArray()));
        assertTrue(reply.isSuccess(), reply.toString());
        return BatchResult.parseFrom(reply.getMessage().getContent().toByteArray());
    }

    private static String read(RaftClient client) throws Exception {
        var request = ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("key")).build();
        var reply = client.io().sendReadOnly(RocksStateMachine.message(request.toByteArray()));
        assertTrue(reply.isSuccess(), reply.toString());
        var value = StoredValue.parseFrom(reply.getMessage().getContent().toByteArray());
        assertTrue(value.getFound());
        return value.getValue().toStringUtf8();
    }

    private static void writeCountedEntity(RaftClient client) throws Exception {
        ByteString key = ByteString.copyFrom(java.nio.ByteBuffer.allocate(18).put((byte) 16)
                .putLong(0).put((byte) 1).putLong(42).array());
        Entity entity = Entity.newBuilder().setNode(true).setType("Company").build();
        var cell = VersionedCell.newBuilder().addVersions(CellVersion.newBuilder().setTransactionId("count-tx")
                .setCommittedAtMs(100).setValue(entity.toByteString())).build();
        var command = WriteBatch.newBuilder().setRequestId("count-entity").setExpiresAtMs(2_000_000_000_000L)
                .addMutations(Mutation.newBuilder().setKey(key).setValue(cell.toByteString())).build();
        var reply = client.io().send(RocksStateMachine.message(command.toByteArray()));
        assertTrue(reply.isSuccess(), reply.toString());
        assertEquals("OK", BatchResult.parseFrom(reply.getMessage().getContent().toByteArray()).getCode());
    }

    private static EntityCounts readCounts(RaftClient client) throws Exception {
        var request = ReadKey.newBuilder().setEntityCounts(true).setCountsGeneration(0).build();
        var reply = client.io().sendReadOnly(RocksStateMachine.message(request.toByteArray()));
        assertTrue(reply.isSuccess(), reply.toString());
        EntityCounts counts = EntityCounts.parseFrom(reply.getMessage().getContent().toByteArray());
        assertEquals(1, counts.getFormatVersion());
        return counts;
    }
}
