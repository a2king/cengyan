package io.cengyan.studio;

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

/**
 * 一次请求一条连接。会话号在 JSON 里，不绑在这条 TCP 上。
 */
final class DbClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final int port;

    DbClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    String exchange(ObjectNode request, int timeoutMs) throws IOException {
        int wait = Math.max(timeoutMs, 1000) + 3000;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(wait);
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.write(MAPPER.writeValueAsString(request));
            writer.write('\n');
            writer.flush();
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("图数据库关闭了连接");
            }
            return line;
        }
    }
}
