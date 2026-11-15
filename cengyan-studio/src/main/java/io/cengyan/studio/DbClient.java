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

    private final java.util.function.Supplier<InetSocketAddress> target;

    DbClient(String host, int port) {
        this(() -> InetSocketAddress.createUnresolved(host, port));
    }

    /** 每次请求重新取目标地址：受管集群中计算节点增减后无需重启 Studio。 */
    DbClient(java.util.function.Supplier<InetSocketAddress> target) {
        this.target = target;
    }

    String exchange(ObjectNode request, int timeoutMs) throws IOException {
        int wait = Math.max(timeoutMs, 1000) + 3000;
        InetSocketAddress address = target.get();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address.getHostString(), address.getPort()), 3000);
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
