package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
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

class RaftProcessTest {
    @TempDir Path home;

    @Test @Timeout(90)
    void 独立JVM节点被强制终止后仍可提交并读取() throws Exception {
        var peers = new ArrayList<RaftPeer>();
        var sockets = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                var socket = new ServerSocket(0);
                sockets.add(socket);
                peers.add(RaftPeer.newBuilder().setId("p" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
            }
        } finally { for (var socket : sockets) socket.close(); }
        RaftGroup group = RaftGroup.valueOf(GroupIds.data("process-test", 1, 0), peers);
        List<Process> processes = new ArrayList<>();
        var settings = new RaftSettings(4L * 1024 * 1024, 1000, 10, 2);
        try (var client = RaftClient.newBuilder().setRaftGroup(group)
                .setProperties(settings.properties(home.resolve("unused"), "127.0.0.1", 0))
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(80, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))).build()) {
            for (int i = 0; i < 3; i++) processes.add(start(i, peers));
            assertEquals("OK", put(client, "before-crash", "one").getCode());
            RaftPeerId leader = client.getLeaderId();
            assertNotNull(leader);
            int leaderNumber = Integer.parseInt(leader.toString().substring(1));
            processes.get(leaderNumber).destroyForcibly();
            assertTrue(processes.get(leaderNumber).waitFor(10, TimeUnit.SECONDS));
            assertEquals("OK", put(client, "after-crash", "two").getCode());
            assertEquals("two", get(client));
            processes.set(leaderNumber, start(leaderNumber, peers));
            assertEquals("OK", put(client, "after-rejoin", "three").getCode());
            assertEquals("three", get(client));
        } finally {
            for (Process process : processes) {
                try { process.getOutputStream().close(); } catch (Exception ignored) { }
            }
            for (Process process : processes) {
                if (!process.waitFor(8, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(8, TimeUnit.SECONDS);
                }
            }
        }
    }

    private Process start(int number, List<RaftPeer> peers) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx128m");
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(RaftProcessMain.class.getName());
        command.add(Integer.toString(number));
        command.add(home.resolve("node-" + number).toString());
        for (var peer : peers) command.add(peer.getAddress().substring(peer.getAddress().lastIndexOf(':') + 1));
        return new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(home.resolve("node-" + number + ".log").toFile())).start();
    }

    private static BatchResult put(RaftClient client, String id, String value) throws Exception {
        var command = WriteBatch.newBuilder().setRequestId(id).setExpiresAtMs(2_000_000_000_000L)
                .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("k")).setValue(ByteString.copyFromUtf8(value))).build();
        var result = client.io().send(RocksStateMachine.message(command.toByteArray()));
        assertTrue(result.isSuccess(), result.toString());
        return BatchResult.parseFrom(result.getMessage().getContent().toByteArray());
    }

    private static String get(RaftClient client) throws Exception {
        var request = ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("k")).build();
        var result = client.io().sendReadOnly(RocksStateMachine.message(request.toByteArray()));
        assertTrue(result.isSuccess(), result.toString());
        return StoredValue.parseFrom(result.getMessage().getContent().toByteArray()).getValue().toStringUtf8();
    }
}
