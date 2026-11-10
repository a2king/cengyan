package io.cengyan.studio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** Studio 到新 Compute 外部 JSON 入口的最小运维客户端。每次调用独立连接，避免复用失效 token。 */
final class ComputeOpsClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    ComputeOpsClient(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    boolean configured() {
        return !host.isBlank() && port > 0 && !user.isBlank() && !password.isBlank();
    }

    JsonNode overview(int timeoutMs) throws IOException {
        if (!configured()) throw new IOException("Studio 尚未配置 Compute 运维地址和账号");
        JsonNode login = call(object("op", "login", "user", user, "password", password), timeoutMs);
        String token = login.path("token").asText(login.path("session").asText(""));
        if (token.isBlank() || !"OK".equals(login.path("code").asText())) {
            throw new IOException(login.path("message").asText("Compute 登录失败"));
        }
        ObjectNode request = MAPPER.createObjectNode().put("op", "ops.overview").put("token", token);
        return call(request, timeoutMs);
    }

    private JsonNode call(ObjectNode request, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), Math.min(timeoutMs, 3000));
            socket.setSoTimeout(timeoutMs);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                writer.write(MAPPER.writeValueAsString(request));
                writer.write('\n');
                writer.flush();
                String line = reader.readLine();
                if (line == null) throw new IOException("Compute 关闭了连接");
                return MAPPER.readTree(line);
            }
        }
    }

    private static ObjectNode object(String... values) {
        ObjectNode node = MAPPER.createObjectNode();
        for (int i = 0; i + 1 < values.length; i += 2) node.put(values[i], values[i + 1]);
        return node;
    }
}
