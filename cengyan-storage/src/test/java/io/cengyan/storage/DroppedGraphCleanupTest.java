package io.cengyan.storage;

import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.protocol.proto.ClusterProtocol.IdType;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class DroppedGraphCleanupTest {
    @TempDir Path home;

    @Test @Timeout(90)
    void 删除图只提交状态后台再清空并且不碰其他图() throws Exception {
        int raft, rpc;
        try (var a = new ServerSocket(0); var b = new ServerSocket(0)) { raft = a.getLocalPort(); rpc = b.getLocalPort(); }
        var peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + raft).setClientAddress("127.0.0.1:" + rpc).build();
        var config = new StorageConfig("drop-cleanup", "s1", new InetSocketAddress("127.0.0.1", rpc), new InetSocketAddress("127.0.0.1", raft),
                home, List.of(peer), RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 64);
        Path raftRoot = home.resolve("raft");
        long goneId;
        String goneUuid;
        String stayUuid;
        try (var server = new StorageServer(config); var gateway = new StorageGateway("drop-cleanup", List.of(config.rpcAddress()))) {
            server.pauseDroppedGraphCleanup();
            var catalog = new DistributedCatalog(gateway);
            var gone = catalog.create("create-gone", "Gone", 1, 1, IdType.STRING, IdType.STRING);
            var stay = catalog.create("create-stay", "Stay", 1, 1, IdType.STRING, IdType.STRING);
            goneId = gone.getId();
            goneUuid = GroupIds.data("drop-cleanup", goneId, 0).getUuid().toString();
            stayUuid = GroupIds.data("drop-cleanup", stay.getId(), 0).getUuid().toString();
            assertTrue(Files.isDirectory(raftRoot.resolve(goneUuid)));
            assertTrue(Files.isDirectory(raftRoot.resolve(stayUuid)));
            catalog.dropGraph("drop-gone", "Gone");
            var missing = assertThrows(ClusterException.class, () -> catalog.graph("Gone"));
            assertEquals("GRAPH_NOT_FOUND", missing.code());
            assertTrue(Files.isDirectory(raftRoot.resolve(goneUuid)), "状态提交后数据文件应仍在");
            assertEquals("ACTIVE", catalog.graph("Stay").getState());
        }
        assertTrue(Files.isDirectory(raftRoot.resolve(goneUuid)), "进程退出前没有同步清空");
        try (var server = new StorageServer(config); var gateway = new StorageGateway("drop-cleanup", List.of(config.rpcAddress()))) {
            server.pauseDroppedGraphCleanup();
            awaitCleared(server, raftRoot, goneUuid);
            assertTrue(Files.isDirectory(raftRoot.resolve(stayUuid)), "其他图的目录必须保留");
            assertTrue(Files.isDirectory(raftRoot.resolve(GroupIds.metadata("drop-cleanup").getUuid().toString())), "Metadata 目录必须保留");
            var catalog = new DistributedCatalog(gateway);
            assertEquals("ACTIVE", catalog.graph("Stay").getState());
            var again = catalog.create("recreate-gone", "Gone", 1, 1, IdType.STRING, IdType.STRING);
            assertNotEquals(goneId, again.getId());
            assertTrue(Files.isDirectory(raftRoot.resolve(GroupIds.data("drop-cleanup", again.getId(), 0).getUuid().toString())));
            assertFalse(residue(raftRoot, goneUuid));
        }
    }

    private static void awaitCleared(StorageServer server, Path raftRoot, String uuid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Exception last = null;
        while (System.nanoTime() < deadline && residue(raftRoot, uuid)) {
            try { server.cleanupDroppedGraphs(); last = null; }
            catch (Exception ex) { last = ex; }
            Thread.sleep(100);
        }
        Exception failure = last;
        assertFalse(residue(raftRoot, uuid), () -> "已删除图的目录仍在 " + names(raftRoot) + " " + failure);
    }

    private static boolean residue(Path raftRoot, String uuid) throws IOException {
        if (Files.exists(raftRoot.resolve(uuid))) return true;
        if (!Files.isDirectory(raftRoot)) return false;
        try (var children = Files.list(raftRoot)) {
            return children.anyMatch(path -> path.getFileName().toString().startsWith("dropped-" + uuid));
        }
    }

    private static String names(Path raftRoot) {
        try (var children = Files.list(raftRoot)) {
            return children.map(path -> path.getFileName().toString()).toList().toString();
        } catch (IOException ex) { return ex.toString(); }
    }
}
