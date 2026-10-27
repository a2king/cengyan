package io.cengyan.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 交互式客户端。一行 GQL 包成 {@code execute}，以冒号开头的行当作管理命令。
 */
public final class CengyanCli {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String session = "";

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 19669;
        List<String> rest = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> {
                    host = args[++i];
                }
                case "--port" -> {
                    port = Integer.parseInt(args[++i]);
                }
                default -> rest.add(args[i]);
            }
        }
        CengyanCli cli = new CengyanCli();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            socket.setTcpNoDelay(true);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            if (!rest.isEmpty()) {
                cli.roundTrip(writer, reader, String.join(" ", rest));
                return;
            }
            System.out.println("层岩图数据库客户端。输入 GQL，ping 检查连接，exit 退出。");
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = stdin.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.equalsIgnoreCase("exit") || trimmed.equalsIgnoreCase("quit")) {
                    return;
                }
                if (trimmed.isEmpty()) {
                    continue;
                }
                cli.roundTrip(writer, reader, trimmed);
            }
        }
    }

    private void roundTrip(BufferedWriter writer, BufferedReader reader, String line) throws IOException {
        writer.write(request(line));
        writer.write('\n');
        writer.flush();
        String response = reader.readLine();
        if (response == null) {
            System.err.println("连接已关闭");
            return;
        }
        print(response);
    }

    String request(String line) throws IOException {
        String trimmed = line.trim();
        ObjectNode node = MAPPER.createObjectNode();
        if (trimmed.equalsIgnoreCase("ping") || trimmed.equals(":ping")) {
            node.put("op", "ping");
        } else if (trimmed.startsWith(":")) {
            String body = trimmed.substring(1).trim();
            String[] parts = body.isEmpty() ? new String[0] : body.split("\\s+");
            String command = parts.length == 0 ? "" : parts[0];
            if (command.equalsIgnoreCase("login")) {
                node.put("op", "login");
                node.put("session", session);
                node.put("user", part(parts, 1));
                node.put("password", parts.length > 2
                        ? String.join(" ", Arrays.copyOfRange(parts, 2, parts.length))
                        : "");
            } else {
                node.put("op", "admin");
                node.put("session", session);
                node.put("command", command);
                switch (command) {
                    case "rebuildIndex" -> node.put("index", part(parts, 1));
                    case "createUser" -> {
                        node.put("user", part(parts, 1));
                        node.put("password", part(parts, 2));
                        node.put("role", part(parts, 3));
                    }
                    case "setPassword" -> {
                        node.put("user", part(parts, 1));
                        node.put("password", part(parts, 2));
                    }
                    case "setRole" -> {
                        node.put("user", part(parts, 1));
                        node.put("role", part(parts, 2));
                    }
                    case "dropUser" -> node.put("user", part(parts, 1));
                    default -> {
                    }
                }
            }
        } else {
            node.put("op", "execute");
            node.put("session", session);
            node.put("query", trimmed);
            node.set("params", MAPPER.createObjectNode());
        }
        return MAPPER.writeValueAsString(node);
    }

    private static String part(String[] parts, int index) {
        return parts.length > index ? parts[index] : "";
    }

    private void print(String response) throws IOException {
        JsonNode node = MAPPER.readTree(response);
        if (node.hasNonNull("session") && !node.get("session").asText().isEmpty()) {
            session = node.get("session").asText();
        }
        String code = node.path("code").asText("");
        if (!"OK".equals(code)) {
            System.err.println(code + " " + node.path("message").asText(""));
            return;
        }
        JsonNode columns = node.get("columns");
        JsonNode rows = node.get("rows");
        if (columns == null || !columns.isArray() || columns.isEmpty()) {
            System.out.println(response);
            return;
        }
        StringBuilder header = new StringBuilder();
        for (JsonNode column : columns) {
            if (!header.isEmpty()) {
                header.append('\t');
            }
            header.append(column.asText());
        }
        System.out.println(header);
        if (rows != null && rows.isArray()) {
            for (JsonNode row : rows) {
                StringBuilder line = new StringBuilder();
                for (JsonNode cell : row) {
                    if (!line.isEmpty()) {
                        line.append('\t');
                    }
                    line.append(cell.isTextual() ? cell.asText() : cell.toString());
                }
                System.out.println(line);
            }
        }
    }
}
