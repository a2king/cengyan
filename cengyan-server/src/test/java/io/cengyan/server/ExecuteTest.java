package io.cengyan.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ExecuteTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void 一行语句走查询线程并返回行() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-exec-");
        ServerConfig config = ServerConfig.defaults().forTest(dir);
        try (GraphDbServer server = GraphDbServer.start(config)) {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(5000);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                String session = "";
                session = ok(writer, reader, session, "CREATE GRAPH Company");
                session = ok(writer, reader, session, "USE Company");
                session = ok(writer, reader, session, "CREATE NODE TYPE Company (name STRING NULL)");
                session = ok(writer, reader, session, "CREATE REL TYPE holder (ratio DOUBLE NULL)");
                session = ok(writer, reader, session,
                        "INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2})");
                JsonNode rows = send(writer, reader, session,
                        "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN b.id, e.ratio");
                assertEquals("OK", rows.get("code").asText());
                assertEquals(session, rows.get("session").asText());
                assertEquals(2, rows.get("rows").get(0).get(0).asLong());
                assertEquals(0.5d, rows.get("rows").get(0).get(1).asDouble());
                JsonNode missing = send(writer, reader, "", "MATCH (a)-[:holder]->(b) RETURN b.id");
                assertEquals("GRAPH_NOT_FOUND", missing.get("code").asText());
                assertFalse(missing.has("rows"));
                assertFalse(missing.has("columns"));
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void 管理命令在查询线程上重建索引() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-admin-");
        ServerConfig config = ServerConfig.defaults().forTest(dir);
        try (GraphDbServer server = GraphDbServer.start(config)) {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(5000);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                String session = "";
                session = ok(writer, reader, session, "CREATE GRAPH Company");
                session = ok(writer, reader, session, "USE Company");
                session = ok(writer, reader, session, "CREATE NODE TYPE Company (name STRING NULL)");
                session = ok(writer, reader, session, "CREATE REL TYPE holder ()");
                session = ok(writer, reader, session, "INSERT (a:Company {id:1, name:'root'})");
                session = ok(writer, reader, session, "CREATE INDEX by_name ON NODE TYPE Company (name)");
                JsonNode rebuilt = admin(writer, reader, session, "rebuildIndex", "by_name");
                assertEquals("OK", rebuilt.get("code").asText(), rebuilt.toString());
                assertEquals(1, rebuilt.get("rowsAffected").asInt());
                JsonNode shown = send(writer, reader, session, "SHOW INDEXES");
                assertEquals("OK", shown.get("code").asText(), shown.toString());
                assertEquals(true, shown.get("rows").get(0).get(5).asBoolean());
                JsonNode stats = admin(writer, reader, session, "stats", "");
                assertEquals("OK", stats.get("code").asText(), stats.toString());
                assertEquals("NODE", stats.get("rows").get(0).get(0).asText());
                assertEquals(1, stats.get("rows").get(0).get(2).asInt());
            }
        } finally {
            deleteTree(dir);
        }
    }

    private static String ok(BufferedWriter writer, BufferedReader reader, String session, String query) throws Exception {
        JsonNode node = send(writer, reader, session, query);
        assertEquals("OK", node.get("code").asText(), node.toString());
        return node.get("session").asText();
    }

    private static JsonNode send(BufferedWriter writer, BufferedReader reader, String session, String query) throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "execute");
        request.put("session", session);
        request.put("query", query);
        request.set("params", MAPPER.createObjectNode());
        writer.write(MAPPER.writeValueAsString(request));
        writer.write('\n');
        writer.flush();
        return MAPPER.readTree(reader.readLine());
    }

    private static JsonNode admin(
            BufferedWriter writer, BufferedReader reader, String session, String command, String index) throws Exception {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "admin");
        request.put("session", session);
        request.put("command", command);
        request.put("index", index);
        writer.write(MAPPER.writeValueAsString(request));
        writer.write('\n');
        writer.flush();
        return MAPPER.readTree(reader.readLine());
    }

    private static void deleteTree(Path root) throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                if (!Files.exists(root)) {
                    return;
                }
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(path);
                    }
                }
                return;
            } catch (Exception ex) {
                last = ex;
                System.gc();
                Thread.sleep(50L * (attempt + 1));
            }
        }
        if (last != null) {
            throw last;
        }
    }
}
