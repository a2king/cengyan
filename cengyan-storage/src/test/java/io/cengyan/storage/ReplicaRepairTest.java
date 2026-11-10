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
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicaRepairTest {
    @TempDir Path home;
    @Test @Timeout(120)
    void 等待故障窗口后替代副本追平且故障节点不再属于分片() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); List<RaftPeer> peers = new ArrayList<>();
        int[] ports = new int[8];
        try {
            for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); }
            for (int i = 0; i < 4; i++) peers.add(RaftPeer.newBuilder().setId("s" + i).setAddress(addr(ports[2*i])).setClientAddress(addr(ports[2*i+1])).build());
        } finally { for (var socket : reserved) socket.close(); }
        List<StorageServer> servers = new ArrayList<>();
        try (var gateway = new StorageGateway("repair", List.of(addr(ports[1]), addr(ports[3]), addr(ports[5]), addr(ports[7])))) {
            for (int i = 0; i < 4; i++) servers.add(new StorageServer(new StorageConfig("repair", "s" + i,
                    addr(ports[2*i+1]), addr(ports[2*i]), home.resolve("s" + i), peers, RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128)));
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create", "G", 1, 3, IdType.STRING, IdType.STRING);
            graph = catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            var data = new DistributedGraph(gateway);
            data.put("seed", "G", List.of(Entity.newBuilder().setNode(true).setType("N").setId(EntityCodec.value("a")).build()), false, Duration.ofSeconds(30));
            var before = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), 0)));
            Node failed = before.getMembers(1);
            int index = Integer.parseInt(failed.getNodeId().substring(1));
            servers.get(index).close(); servers.set(index, null);
            var controller = new ReplicaController(gateway);
            controller.reconcile(1);
            Thread.sleep(5);
            controller.reconcile(1);
            var after = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), 0)));
            assertEquals(3, after.getMembersCount());
            assertFalse(after.getMembersList().stream().anyMatch(node -> node.getNodeId().equals(failed.getNodeId())));
            assertNotNull(data.get(graph, true, EntityCodec.value("a"), data.readScope(graph.getId())));
        } finally { for (var server : servers) if (server != null) server.close(); }
    }
    private static InetSocketAddress addr(int port) { return new InetSocketAddress("127.0.0.1", port); }
}
