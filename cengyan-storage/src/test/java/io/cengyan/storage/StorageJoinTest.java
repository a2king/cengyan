package io.cengyan.storage;

import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class StorageJoinTest {
    @TempDir Path home;
    @Test @Timeout(90)
    void 新存储节点从种子加入Metadata后成为正式成员() throws Exception {
        List<ServerSocket> sockets = new ArrayList<>(); int[] ports = new int[4];
        try { for (int i = 0; i < 4; i++) { var socket = new ServerSocket(0); sockets.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : sockets) socket.close(); }
        var seed = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + ports[0]).setClientAddress("127.0.0.1:" + ports[1]).build();
        var first = config("s1", ports[0], ports[1], List.of(seed));
        var second = config("s2", ports[2], ports[3], List.of(seed));
        try (var s1 = new StorageServer(first); var s2 = new StorageServer(second);
             var gateway = new StorageGateway("join-test", List.of(first.rpcAddress(), second.rpcAddress()))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            List<Node> members = List.of();
            do {
                members = new DistributedCatalog(gateway).nodes();
                if (members.size() == 2) break;
                Thread.sleep(50);
            } while (System.nanoTime() < deadline);
            assertEquals(2, members.size());
            assertTrue(members.stream().anyMatch(node -> node.getNodeId().equals("s2")));
            var graph = new DistributedCatalog(gateway).create("create-after-join", "Joined", 2, 2, IdType.INT64, IdType.INT64);
            assertEquals(2, graph.getReplicas());
        }
    }
    private StorageConfig config(String id, int raft, int rpc, List<RaftPeer> peers) {
        return new StorageConfig("join-test", id, new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home.resolve(id), peers, RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
    }
}
