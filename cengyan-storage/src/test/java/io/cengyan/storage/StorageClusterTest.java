package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import io.cengyan.rpc.RpcClient;
import io.cengyan.distributed.*;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class StorageClusterTest {
    @TempDir Path directory;

    @Test @Timeout(90)
    void 三存储节点经业务RPC定位Leader并在故障后路由到新Leader() throws Exception {
        var sockets = new ArrayList<ServerSocket>();
        var peers = new ArrayList<RaftPeer>();
        var configs = new ArrayList<StorageConfig>();
        var spec = DataGroupSpec.newBuilder().setGraphId(7).setPartId(2);
        try {
            for (int i = 0; i < 3; i++) {
                var raft = new ServerSocket(0);
                sockets.add(raft);
                var rpc = new ServerSocket(0);
                sockets.add(rpc);
                peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress("127.0.0.1:" + raft.getLocalPort())
                        .setClientAddress("127.0.0.1:" + rpc.getLocalPort()).build());
                spec.addMembers(Node.newBuilder().setClusterId("rpc-cluster").setNodeId("s" + i).setRole(NodeRole.STORAGE)
                        .setRaft(Endpoint.newBuilder().setHost("127.0.0.1").setPort(raft.getLocalPort()))
                        .setRpc(Endpoint.newBuilder().setHost("127.0.0.1").setPort(rpc.getLocalPort())));
            }
            for (int i = 0; i < 3; i++) configs.add(new StorageConfig("rpc-cluster", "s" + i,
                    new InetSocketAddress("127.0.0.1", sockets.get(2 * i + 1).getLocalPort()),
                    new InetSocketAddress("127.0.0.1", sockets.get(2 * i).getLocalPort()), directory.resolve("s" + i),
                    peers, new RaftSettings(4L * 1024 * 1024, 1000, 10, 2), 1024 * 1024, 32, 2, 32));
        } finally { for (var socket : sockets) socket.close(); }
        var servers = new ArrayList<StorageServer>();
        var clients = new ArrayList<RpcClient>();
        try {
            for (var config : configs) servers.add(new StorageServer(config));
            for (var config : configs) clients.add(new RpcClient(config.rpcAddress(), config.rpcMaxPayload(), 32));
            for (var client : clients) assertEquals("OK", call(client, "data.addGroup", "", spec.build().toByteString()).getCode());
            try (var gateway = new StorageGateway("rpc-cluster", configs.stream().map(StorageConfig::rpcAddress).toList())) {
                var catalog = new DistributedCatalog(gateway);
                var created = catalog.create("online-create", "Online", 1, 1, IdType.STRING, IdType.STRING);
                var before = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(created.getId(), 0)));
                assertEquals(1, before.getMembersCount());
                new ReplicaController(gateway).resize("online-scale", "Online", 3);
                var after = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(created.getId(), 0)));
                assertEquals(3, after.getMembersCount());
                assertEquals(3, catalog.graph("Online").getReplicas());
            }
            int leader = writeToLeader(clients, "first", "before");
            assertTrue(leader >= 0);
            var key = ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("k")).build().toByteString();
            assertEquals("before", StoredValue.parseFrom(call(clients.get(leader), "kv.read", "", key).getBody()).getValue().toStringUtf8());
            int follower = (leader + 1) % 3;
            RpcResponse staleRoute = call(clients.get(follower), "kv.read", "", key);
            assertEquals("NOT_LEADER", staleRoute.getCode());
            assertTrue(staleRoute.hasLeaderHint());
            assertEquals("s" + leader, staleRoute.getLeaderHint().getNodeId());
            assertEquals(configs.get(leader).rpcAddress().getPort(), staleRoute.getLeaderHint().getRpc().getPort());
            servers.get(leader).close();
            clients.get(leader).close();
            clients.set(leader, null);
            int newLeader = writeToLeader(clients, "second", "after");
            assertTrue(newLeader >= 0 && newLeader != leader);
            assertEquals("after", StoredValue.parseFrom(call(clients.get(newLeader), "kv.read", "", key).getBody()).getValue().toStringUtf8());
        } finally {
            for (var client : clients) if (client != null) client.close();
            for (var server : servers) server.close();
        }
    }

    private static int writeToLeader(List<RpcClient> clients, String id, String value) throws Exception {
        var batch = WriteBatch.newBuilder().setRequestId(id).setExpiresAtMs(2_000_000_000_000L)
                .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("k")).setValue(ByteString.copyFromUtf8(value)))
                .build().toByteString();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            for (int i = 0; i < clients.size(); i++) {
                if (clients.get(i) == null) continue;
                var response = call(clients.get(i), "kv.write", id, batch);
                if (response.getCode().equals("OK")) return i;
                assertTrue(response.getCode().equals("NOT_LEADER") || response.getCode().equals("FAILED"), response.toString());
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        fail("未能通过 Storage 业务 RPC 找到可提交写入的 Leader");
        return -1;
    }

    private static RpcResponse call(RpcClient client, String method, String id, ByteString body) throws Exception {
        return client.call(RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId("rpc-cluster")
                        .setGraphId(7).setPartId(2).setRequestId(id).setRemainingTimeoutMs(5000))
                .setMethod(method).setBody(body).build(), Duration.ofSeconds(8)).get(10, TimeUnit.SECONDS);
    }
}
