package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotInstallTest {
    @TempDir Path home;
    @Test @Timeout(90)
    void 新副本通过Checkpoint安装追平已压缩日志() throws Exception {
        int a, b;
        try (var one = new ServerSocket(0); var two = new ServerSocket(0)) { a = one.getLocalPort(); b = two.getLocalPort(); }
        RaftPeer first = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + a).build();
        RaftPeer second = RaftPeer.newBuilder().setId("s2").setAddress("127.0.0.1:" + b)
                .setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
        var id = GroupIds.data("snapshot-install", 1, 0);
        var initial = RaftGroup.valueOf(id, first);
        var settings = new RaftSettings(1024 * 1024, 10, 0, 1);
        try (var source = new RaftNode("snapshot-install", "s1", "127.0.0.1", a, home.resolve("s1"), initial, settings);
             var client = source.client(initial)) {
            for (int i = 0; i < 24; i++) {
                var write = WriteBatch.newBuilder().setRequestId("r" + i).setExpiresAtMs(2_000_000_000_000L)
                        .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("key")).setValue(ByteString.copyFromUtf8("value-" + i))).build();
                assertTrue(client.io().send(RocksStateMachine.message(write.toByteArray())).isSuccess());
            }
            var division = source.server().getDivision(id);
            long snapshot = division.getStateMachine().takeSnapshot();
            assertTrue(snapshot > 0);
            division.getRaftLog().purge(snapshot).get(10, TimeUnit.SECONDS);
            var targetGroup = RaftGroup.valueOf(id, first, second);
            try (var target = new RaftNode("snapshot-install", "s2", "127.0.0.1", b, home.resolve("s2"), targetGroup, settings)) {
                assertTrue(client.admin().setConfiguration(List.of(first), List.of(second)).isSuccess());
                RaftPeer voting = RaftPeer.newBuilder().setId("s2").setAddress(second.getAddress()).build();
                assertTrue(client.admin().setConfiguration(List.of(first, voting), List.of()).isSuccess());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (target.server().getDivision(id).getInfo().getLastAppliedIndex() < snapshot && System.nanoTime() < deadline) Thread.sleep(20);
                assertTrue(target.server().getDivision(id).getInfo().getLastAppliedIndex() >= snapshot);
                var read = target.server().getDivision(id).getStateMachine().query(RocksStateMachine.message(
                        ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("key")).build().toByteArray())).join();
                assertEquals("value-23", StoredValue.parseFrom(read.getContent().toByteArray()).getValue().toStringUtf8());
            }
        }
    }

    @Test @Timeout(90)
    void 重启直接打开原库而不是再复制一份快照() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        RaftPeer peer = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + port).build();
        var id = GroupIds.data("reopen", 1, 0);
        var group = RaftGroup.valueOf(id, peer);
        var settings = new RaftSettings(1024 * 1024, 10, 0, 1);
        Path root = home.resolve("reopen");
        try (var node = new RaftNode("reopen", "s1", "127.0.0.1", port, root, group, settings);
             var client = node.client(group)) {
            var write = WriteBatch.newBuilder().setRequestId("once").setExpiresAtMs(2_000_000_000_000L)
                    .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("key")).setValue(ByteString.copyFromUtf8("kept"))).build();
            assertTrue(client.io().send(RocksStateMachine.message(write.toByteArray())).isSuccess());
            assertTrue(node.server().getDivision(id).getStateMachine().takeSnapshot() > 0);
        }
        Path dbSst = null;
        Path snapSst = null;
        try (var files = java.nio.file.Files.walk(root)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                assertFalse(name.startsWith("snapshot.") || name.startsWith("work-"), "不再保留整库 zip 或第二份工作目录");
                if (!name.endsWith(".sst") || path.getParent() == null) continue;
                String parent = path.getParent().getFileName().toString();
                if (parent.equals("db")) dbSst = path;
                if (parent.startsWith("snap-")) snapSst = path;
            }
        }
        if (dbSst != null && snapSst != null) {
            assertEquals(java.nio.file.Files.getAttribute(dbSst, "basic:fileKey"), java.nio.file.Files.getAttribute(snapSst, "basic:fileKey"),
                    "快照应与工作库共享 SST，而不是再复制一份");
        }
        try (var node = new RaftNode("reopen", "s1", "127.0.0.1", port, root, group, settings)) {
            var read = node.server().getDivision(id).getStateMachine().query(RocksStateMachine.message(
                    ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("key")).build().toByteArray())).join();
            assertEquals("kept", StoredValue.parseFrom(read.getContent().toByteArray()).getValue().toStringUtf8());
        }
    }
}
