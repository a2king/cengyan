package io.cengyan.studio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/** 测试用图数据库：每条连接一行请求、一行响应，按请求内容决定响应。可以接受多条连接。 */
public final class FakeDb implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ServerSocket server;
    private final Thread thread;
    public final List<JsonNode> seen = new CopyOnWriteArrayList<>();

    public FakeDb(Function<JsonNode, String> handler) throws IOException {
        server = new ServerSocket(0);
        thread = new Thread(() -> {
            while (!server.isClosed()) {
                try (Socket socket = server.accept()) {
                    var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                    String line = reader.readLine();
                    if (line == null) continue;
                    JsonNode request = MAPPER.readTree(line);
                    seen.add(request);
                    writer.write(handler.apply(request));
                    writer.write('\n');
                    writer.flush();
                } catch (IOException ignored) { }
            }
        }, "fake-db");
        thread.setDaemon(true);
        thread.start();
    }

    public int port() { return server.getLocalPort(); }

    public List<String> ops() { return seen.stream().map(n -> n.path("op").asText()).toList(); }

    @Override public void close() throws IOException {
        server.close();
        try { thread.join(2000); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
    }
}
