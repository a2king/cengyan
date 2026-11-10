package io.cengyan.storage;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.RaftSettings;
import io.cengyan.rpc.RpcClient;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class StorageServerTest {
    @TempDir Path directory;

    @Test @Timeout(60)
    void 业务RPC动态建分片原子写入幂等读取及进程式重启恢复() throws Exception {
        int raftPort, rpcPort;
        try (var raft = new ServerSocket(0); var rpc = new ServerSocket(0)) {
            raftPort = raft.getLocalPort(); rpcPort = rpc.getLocalPort();
        }
        var peer = RaftPeer.newBuilder().setId("storage-1").setAddress("127.0.0.1:" + raftPort)
                .setClientAddress("127.0.0.1:" + rpcPort).build();
        var config = new StorageConfig("storage-test", "storage-1", new InetSocketAddress("127.0.0.1", rpcPort),
                new InetSocketAddress("127.0.0.1", raftPort), directory.resolve("node"), List.of(peer),
                new RaftSettings(4L * 1024 * 1024, 1000, 10, 2), 1024 * 1024, 32, 2, 32);
        var member = Node.newBuilder().setClusterId(config.clusterId()).setNodeId(config.nodeId()).setRole(NodeRole.STORAGE)
                .setRpc(Endpoint.newBuilder().setHost("127.0.0.1").setPort(rpcPort))
                .setRaft(Endpoint.newBuilder().setHost("127.0.0.1").setPort(raftPort));
        var spec = DataGroupSpec.newBuilder().setGraphId(1).setPartId(0).addMembers(member).build();
        try (var server = new StorageServer(config); var client = new RpcClient(config.rpcAddress(), config.rpcMaxPayload(), 32)) {
            assertEquals("OK", call(client, config.clusterId(), 0, "", "data.addGroup", spec.toByteString()).getCode());
            RpcResponse written = writeWhenReady(client, config, "request-1", "old");
            assertEquals("OK", written.getCode(), written.toString());
            assertEquals("OK", writeWhenReady(client, config, "request-1", "old").getCode());
            assertEquals("REQUEST_CONFLICT", writeWhenReady(client, config, "request-1", "changed").getCode());
            assertEquals("old", read(client, config));
            assertEquals("CLUSTER_MISMATCH", call(client, "other", 1, "", "ping", ByteString.EMPTY).getCode());
        }
        try (var server = new StorageServer(config); var client = new RpcClient(config.rpcAddress(), config.rpcMaxPayload(), 32)) {
            assertEquals("OK", writeWhenReady(client, config, "request-1", "old").getCode());
            assertEquals("old", read(client, config));
            assertEquals("OK", writeWhenReady(client, config, "request-2", "new").getCode());
            assertEquals("new", read(client, config));
        }
        var wrong = new StorageConfig("other-cluster", config.nodeId(), config.rpcAddress(), config.raftAddress(), config.directory(),
                config.metadataMembers(), config.raftSettings(), config.rpcMaxPayload(), 32, 2, 32);
        assertThrows(java.io.IOException.class, () -> new StorageServer(wrong));
    }

    @Test void 配置拒绝端口复用并支持相对目录() throws Exception {
        var file = directory.resolve("storage.properties");
        Files.writeString(file, """
                cluster_id=test
                node_id=s1
                rpc_address=127.0.0.1:19001
                raft_address=127.0.0.1:19000
                metadata_members=s1@127.0.0.1:19000@127.0.0.1:19001
                data_dir=node-data
                """);
        var config = StorageConfig.load(file);
        assertEquals(10, config.writeThreads());
        assertEquals(32 * 1024 * 1024, config.rpcMaxPayload());
        assertFalse(config.rebuildData());
        Files.writeString(file, Files.readString(file) + "\ndata_storage_mode=rebuild\n");
        assertTrue(StorageConfig.load(file).rebuildData());
        Files.writeString(file, Files.readString(file).replace("data_storage_mode=rebuild", "data_storage_mode=invalid"));
        assertThrows(IllegalArgumentException.class, () -> StorageConfig.load(file));
        Files.writeString(file, Files.readString(file).replace("data_storage_mode=invalid", "data_storage_mode=raft"));
        Files.writeString(file, Files.readString(file) + "\nwrite_threads=3\n");
        assertEquals(3, StorageConfig.load(file).writeThreads());
        Files.writeString(file, Files.readString(file).replace("write_threads=3", "write_threads=0"));
        assertThrows(IllegalArgumentException.class, () -> StorageConfig.load(file));
        Files.writeString(file, Files.readString(file).replace("write_threads=0", "write_threads=3"));
        assertEquals(directory.resolve("node-data").toAbsolutePath(), config.directory());
        Files.writeString(file, Files.readString(file).replace("rpc_address=127.0.0.1:19001", "rpc_address=127.0.0.1:19000"));
        assertThrows(IllegalArgumentException.class, () -> StorageConfig.load(file));
        assertThrows(IllegalArgumentException.class, () -> StorageConfig.endpoint("host:123/path"));
    }

    private static RpcResponse writeWhenReady(RpcClient client, StorageConfig config, String id, String value) throws Exception {
        var command = WriteBatch.newBuilder().setRequestId(id).setExpiresAtMs(2_000_000_000_000L)
                .addMutations(Mutation.newBuilder().setKey(ByteString.copyFromUtf8("key")).setValue(ByteString.copyFromUtf8(value))).build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        RpcResponse response;
        do {
            response = call(client, config.clusterId(), 1, id, "kv.write", command.toByteString());
            if (!response.getCode().equals("NOT_LEADER") && !response.getCode().equals("FAILED")) return response;
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return response;
    }

    private static String read(RpcClient client, StorageConfig config) throws Exception {
        var result = call(client, config.clusterId(), 1, "", "kv.read", ReadKey.newBuilder().setKey(ByteString.copyFromUtf8("key")).build().toByteString());
        assertEquals("OK", result.getCode(), result.toString());
        return StoredValue.parseFrom(result.getBody()).getValue().toStringUtf8();
    }

    private static RpcResponse call(RpcClient client, String cluster, long graph, String id, String method, ByteString body) throws Exception {
        return client.call(RpcRequest.newBuilder().setContext(RequestContext.newBuilder().setClusterId(cluster).setGraphId(graph)
                .setRequestId(id).setRemainingTimeoutMs(5000)).setMethod(method).setBody(body).build(), Duration.ofSeconds(8))
                .get(10, TimeUnit.SECONDS);
    }
}
