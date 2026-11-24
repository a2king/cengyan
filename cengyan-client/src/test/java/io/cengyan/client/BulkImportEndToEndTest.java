package io.cengyan.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.compute.ComputeConfig;
import io.cengyan.compute.ComputeServer;
import io.cengyan.raft.RaftSettings;
import io.cengyan.storage.StorageConfig;
import io.cengyan.storage.StorageServer;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 用真实的 cengyan-client import 命令，经过 Compute、Storage 和 Raft 导入，再用 GQL 查回来。 */
class BulkImportEndToEndTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path home;

    @Test @Timeout(240)
    void importedTypesAreQueryableAndVertexIdsStayUniqueAcrossTypes() throws Exception {
        int[] ports = ports(5);
        var storage = new StorageConfig("bulk", "s1", address(ports[0]), address(ports[1]), home.resolve("storage"),
                List.of(RaftPeer.newBuilder().setId("s1").setAddress(address(ports[1])).setClientAddress(address(ports[0])).build()),
                RaftSettings.defaults(), 16 * 1024 * 1024, 128, 4, 128);
        var compute = new ComputeConfig("bulk", "c1", address(ports[2]), address(ports[3]), address(ports[4]), home.resolve("compute"),
                List.of(new ComputeConfig.Peer("c1", address(ports[3]), address(ports[4]), address(ports[2]))), List.of(address(ports[0])),
                4, 32, 10000, 120000, 64L * 1024 * 1024, 128L * 1024 * 1024, 1024L * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                10000, 1024 * 1024, "secret");
        Files.createDirectories(storage.directory());
        try (var storageServer = new StorageServer(storage); var computeServer = new ComputeServer(compute);
             var socket = new Socket("127.0.0.1", ports[2])) {
            socket.setSoTimeout(120000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String token = call(writer, reader, JSON.createObjectNode().put("op", "login").put("user", "root").put("password", "secret"))
                    .path("token").asText();
            ok(call(writer, reader, request(token, "createGraph").put("name", "G").put("partitions", 4)
                    .put("vertexIdType", "STRING").put("edgeIdType", "STRING")));
            ok(call(writer, reader, query(token, "CREATE NODE TYPE Person ID STRING(64) (name STRING NULL)")));
            ok(call(writer, reader, query(token, "CREATE NODE TYPE Org ID STRING(64) (name STRING NULL)")));
            ok(call(writer, reader, query(token, "CREATE REL TYPE works ()")));

            // 一个已有的点，用普通事务写入：导入要和它共存，也要看得到它占用的 ID。
            ok(call(writer, reader, query(token, "INSERT (o:Org {id:'o0', name:'Existing'})")));

            StringBuilder people = new StringBuilder("id\tname\n");
            for (int i = 0; i < 2000; i++) people.append("p").append(i).append("\tPerson ").append(i).append('\n');
            people.append("p1\tDuplicate\n");
            Path peopleFile = Files.writeString(home.resolve("people.tsv"), people);
            assertEquals(0, importFile(ports[2], "--node", "Person", peopleFile, "--batch-mb", "1"));

            // p5 已经是 Person：点 ID 在全图唯一，Org 里的 p5 必须被跳过。
            Path orgFile = Files.writeString(home.resolve("orgs.tsv"), "id\tname\no1\tAcme\no2\tGlobex\np5\tWrong type\n");
            assertEquals(0, importFile(ports[2], "--node", "Org", orgFile));

            StringBuilder works = new StringBuilder("id\tsrc\tdst\n");
            for (int i = 0; i < 2000; i++) works.append("w").append(i).append("\tp").append(i).append("\to").append(1 + i % 2).append('\n');
            Path worksFile = Files.writeString(home.resolve("works.tsv"), works);
            assertEquals(0, importFile(ports[2], "--edge", "works", worksFile, "--src-id", "src", "--dst-id", "dst"));

            JsonNode count = call(writer, reader, query(token, "MATCH (a:Person) RETURN count(a.id)"));
            ok(count);
            assertEquals(2000, count.path("rows").get(0).get(0).asInt(), "客户端去掉了文件里重复的 p1");
            JsonNode orgs = call(writer, reader, query(token, "MATCH (a:Org) RETURN a.id ORDER BY a.id"));
            ok(orgs);
            assertEquals(List.of("o0", "o1", "o2"), ids(orgs));
            JsonNode p5 = call(writer, reader, query(token, "MATCH (a:Person {id:'p5'}) RETURN a.name"));
            ok(p5);
            assertEquals("Person 5", p5.path("rows").get(0).get(0).asText());

            JsonNode employer = call(writer, reader, query(token, "MATCH (a:Person {id:'p7'})-[:works]->(b:Org) RETURN b.id, b.name"));
            ok(employer);
            assertEquals("o2", employer.path("rows").get(0).get(0).asText());
            assertEquals("Globex", employer.path("rows").get(0).get(1).asText());
            JsonNode staff = call(writer, reader, query(token, "MATCH (a:Org {id:'o1'})<-[:works]-(b:Person) RETURN count(b.id)"));
            ok(staff);
            assertEquals(1000, staff.path("rows").get(0).get(0).asInt());

            // 导入的点也能被普通事务引用和修改。
            ok(call(writer, reader, query(token, "INSERT (a {id:'p3'})-[:works {id:'w-new'}]->(b {id:'o0'})")));
            JsonNode both = call(writer, reader, query(token, "MATCH (a:Person {id:'p3'})-[:works]->(b:Org) RETURN b.id ORDER BY b.id"));
            ok(both);
            assertEquals(List.of("o0", "o2"), ids(both));
            JsonNode clash = call(writer, reader, query(token, "INSERT (n:Org {id:'p9', name:'clash'})"));
            assertNotEquals("OK", clash.path("code").asText(), "普通写入也不能让一个 ID 属于两个点类型");

            JsonNode stats = call(writer, reader, request(token, "admin").put("command", "stats").put("refresh", false));
            ok(stats);
            try (var files = Files.walk(home.resolve("storage"))) {
                assertTrue(files.noneMatch(path -> path.toString().contains("import-staging") && path.getFileName().toString().endsWith(".sst")),
                        "导入结束后各副本的暂存文件都已删除");
            }
        }
    }

    private int importFile(int computePort, String kind, String type, Path file, String... extra) throws Exception {
        List<String> args = new ArrayList<>(List.of("import", "--compute-nodes", "127.0.0.1:" + computePort, "--username", "root",
                "--password", "secret", "--graph-space", "G", "--file-type", "tsv", "--files", file.toString(), kind, type,
                "--id", "id", "--thread", "3", "--retry", "1", "--retry-interval", "0"));
        if (kind.equals("--node")) args.addAll(List.of("--parameter", "name"));
        args.addAll(List.of(extra));
        return new CengyanClient().run(args.toArray(String[]::new));
    }

    private static List<String> ids(JsonNode result) {
        List<String> ids = new ArrayList<>();
        for (JsonNode row : result.path("rows")) ids.add(row.get(0).asText());
        return ids;
    }

    private static int[] ports(int count) throws Exception {
        List<ServerSocket> reserved = new ArrayList<>();
        int[] ports = new int[count];
        try { for (int i = 0; i < count; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : reserved) socket.close(); }
        return ports;
    }

    private static InetSocketAddress address(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static ObjectNode request(String token, String op) {
        return JSON.createObjectNode().put("op", op).put("token", token).put("graph", "G").put("requestId", UUID.randomUUID().toString());
    }
    private static ObjectNode query(String token, String gql) { return request(token, "execute").put("query", gql); }
    private static JsonNode call(BufferedWriter writer, BufferedReader reader, ObjectNode request) throws Exception {
        writer.write(request.toString()); writer.newLine(); writer.flush();
        String line = reader.readLine(); assertNotNull(line); return JSON.readTree(line);
    }
    private static void ok(JsonNode response) { assertEquals("OK", response.path("code").asText(), response.toString()); }
}
