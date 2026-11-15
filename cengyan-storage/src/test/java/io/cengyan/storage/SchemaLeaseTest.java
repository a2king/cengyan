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
import static org.junit.jupiter.api.Assertions.*;

class SchemaLeaseTest {
    @TempDir Path home;
    @Test @Timeout(60)
    void 授权保留边界持久化且旧租约跨版本切换续租() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("leases", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft), home,
                List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
        try (var server = new StorageServer(config); var gateway = new StorageGateway("leases", List.of(config.rpcAddress()))) {
            var catalog = new DistributedCatalog(gateway);
            var graph = catalog.create("create", "G", 1, 1, IdType.INT64, IdType.INT64);
            var leases = new SchemaLeases(gateway);
            var lease = leases.acquire("G", "query-1", System.currentTimeMillis() + 60000, 1, false);
            assertEquals(1, lease.getSchemaVersion());
            LeaseHorizon horizon = LeaseHorizon.parseFrom(gateway.get(0, 0, SchemaLeases.key(graph.getId(), 1)));
            assertTrue(horizon.getRetainUntilMs() > lease.getHardDeadlineMs());
            catalog.update("activate", graph, graph.toBuilder().setSchemaVersion(2).build());
            var renewed = new SchemaLeases(gateway).renew(lease);
            assertEquals(1, renewed.getSchemaVersion());
            var newLease = leases.acquire("G", "query-2", System.currentTimeMillis() + 60000, 2, false);
            assertEquals(2, newLease.getSchemaVersion());
            assertThrows(ClusterException.class, () -> leases.validate(lease.toBuilder().setExpiresAtMs(1).build(), graph.getId(), 1));
            catalog.update("migrating", catalog.graph("G"), catalog.graph("G").toBuilder().setState("MIGRATING").build());
            assertThrows(ClusterException.class, () -> leases.acquire("G", "write-1", System.currentTimeMillis() + 60000, 2, true));
        }
    }
}
