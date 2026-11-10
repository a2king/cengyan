package io.cengyan.query;

import io.cengyan.distributed.*;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class MetadataOnlyQueryTest {
    @TempDir Path work;

    @Test void useAndSchemaQueriesNeedNoDataRouteOrTransactionScan() throws Exception {
        AtomicInteger dataCalls = new AtomicInteger();
        var graph = GraphDefinition.newBuilder().setId(7).setName("G").setState("ACTIVE").setPartitions(32)
                .setSchemaVersion(1).addTypes(TypeDefinition.newBuilder().setNode(true).setName("N")
                        .addProperties(PropertyDefinition.newBuilder().setName("name").setType("STRING").setNullable(true))).build();
        try (var metadata = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "meta-only", 1024 * 1024, 16, request -> {
                 try {
                     ReadKey key = ReadKey.parseFrom(request.getBody());
                     String name = key.getKey().toStringUtf8();
                     if (request.getContext().getGraphId() > 0 || key.getScan() || name.startsWith("catalog/group/")) {
                         dataCalls.incrementAndGet();
                         return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("GROUP_UNAVAILABLE").build());
                     }
                     boolean found = name.equals("catalog/graph/G") || name.equals("catalog/graph/Deleted");
                     var stored = name.endsWith("Deleted") ? graph.toBuilder().setState("DROPPED").build() : graph;
                     return CompletableFuture.completedFuture(RpcResponse.newBuilder().setCode("OK").setBody(StoredValue.newBuilder()
                             .setFound(found).setValue(found ? stored.toByteString() : com.google.protobuf.ByteString.EMPTY).build().toByteString()).build());
                 } catch (Exception ex) { return CompletableFuture.failedFuture(ex); }
             }); var gateway = new StorageGateway("meta-only", List.of(metadata.address()))) {
            for (String query : List.of("USE G", "SHOW NODE TYPES", "SHOW REL TYPES", "SHOW INDEXES", "DESCRIBE NODE TYPE N", "RETURN 1 AS answer")) {
                var engine = new RemoteQueryEngine(gateway, work, "64MB", "1GB", 1);
                assertNotNull(engine.execute("metadata-test", "G", query, Map.of(), () -> false));
            }
            var engine = new RemoteQueryEngine(gateway, work, "64MB", "1GB", 1);
            assertNotNull(engine.execute("use-explicit-target", "PreviousGraph", "USE G", Map.of(), () -> false));
            assertEquals("GRAPH_NOT_FOUND", assertThrows(ClusterException.class,
                    () -> engine.execute("missing", "Missing", "USE Missing", Map.of(), () -> false)).code());
            assertEquals("GRAPH_NOT_FOUND", assertThrows(ClusterException.class,
                    () -> engine.execute("deleted", "Deleted", "USE Deleted", Map.of(), () -> false)).code());
            assertEquals(0, dataCalls.get());
        }
    }
}
