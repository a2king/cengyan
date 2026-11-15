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

class StorageRebalanceTest {
    @TempDir Path home;
    @Test @Timeout(120)
    void 新增节点后后台任务在线迁移单副本分片并保留数据() throws Exception {
        List<ServerSocket> held = new ArrayList<>(); int[] ports = new int[4];
        try { for (int i = 0; i < 4; i++) { var socket = new ServerSocket(0); held.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : held) socket.close(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress(address(ports[0])).setClientAddress(address(ports[1])).build();
        var first = config("s1", ports[0], ports[1], List.of(peer));
        var second = config("s2", ports[2], ports[3], List.of(peer));
        try (var s1 = new StorageServer(first); var gateway = new StorageGateway("rebalance", List.of(address(ports[1])))) {
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create", "G", 4, 1, IdType.STRING, IdType.STRING);
            graph = catalog.addType("type", "G", TypeDefinition.newBuilder().setNode(true).setName("N").build());
            var data = new DistributedGraph(gateway);
            List<Entity> rows = new ArrayList<>();
            for (int i = 0; i < 20; i++) rows.add(Entity.newBuilder().setNode(true).setType("N").setId(EntityCodec.value("n" + i)).build());
            data.put("seed", "G", rows, false, Duration.ofSeconds(30));
            try (var s2 = new StorageServer(second)) {
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(45);
                int onSecond = 0;
                do {
                    onSecond = 0;
                    for (int part = 0; part < 4; part++) {
                        var spec = DataGroupSpec.parseFrom(gateway.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                        if (spec.getMembersList().stream().anyMatch(node -> node.getNodeId().equals("s2"))) onSecond++;
                    }
                    if (onSecond > 0) break;
                    Thread.sleep(100);
                } while (System.nanoTime() < deadline);
                assertTrue(onSecond > 0, "新增节点未接收到在线迁移分片");
                // Keep the original Gateway alive: it must refresh warmed routes without a restart.
                assertEquals(20, data.scan(graph, true, "N", data.readScope(graph.getId())).size());
                assertEquals(1, data.put("after-move", "G", List.of(Entity.newBuilder().setNode(true).setType("N")
                        .setId(EntityCodec.value("after-move")).build()), false, Duration.ofSeconds(30)).affected());
                try (var fresh = new StorageGateway("rebalance", List.of(address(ports[1]), address(ports[3])))) {
                    var remote = new DistributedGraph(fresh);
                    assertEquals(21, remote.scan(graph, true, "N", remote.readScope(graph.getId())).size());
                }
            }
        }
    }
    private StorageConfig config(String id, int raft, int rpc, List<RaftPeer> members) {
        return new StorageConfig("rebalance", id, address(rpc), address(raft), home.resolve(id), members, RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
    }
    private static InetSocketAddress address(int port) { return new InetSocketAddress("127.0.0.1", port); }
}
