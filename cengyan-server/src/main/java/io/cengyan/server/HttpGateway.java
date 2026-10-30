package io.cengyan.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** HTTP JSON facade for the same one-line protocol used by TCP clients. */
final class HttpGateway implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_BODY = 16 * 1024 * 1024;

    private final HttpServer server;
    private final ExecutorService workers;
    private final String tcpHost;
    private final int tcpPort;
    private final ConcurrentMap<String, TcpConnection> sessions = new ConcurrentHashMap<>();

    private HttpGateway(HttpServer server, ExecutorService workers, String tcpHost, int tcpPort) {
        this.server = server;
        this.workers = workers;
        this.tcpHost = tcpHost;
        this.tcpPort = tcpPort;
    }

    static HttpGateway start(String bindHost, int httpPort, String tcpHost, int tcpPort,
                             boolean tlsEnabled, Path keystore, String password) throws IOException {
        HttpServer server;
        if (tlsEnabled) {
            if (password == null || password.isEmpty() || !Files.exists(keystore)) {
                throw new IOException("HTTP TLS keystore is missing or password is empty: " + keystore);
            }
            try {
                KeyStore store = KeyStore.getInstance("PKCS12");
                try (InputStream input = Files.newInputStream(keystore)) {
                    store.load(input, password.toCharArray());
                }
                KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                factory.init(store, password.toCharArray());
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(factory.getKeyManagers(), null, null);
                com.sun.net.httpserver.HttpsServer https = com.sun.net.httpserver.HttpsServer.create(
                        new InetSocketAddress(bindHost, httpPort), 128);
                https.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(context));
                server = https;
            } catch (Exception ex) {
                throw new IOException("cannot initialize HTTP TLS", ex);
            }
        } else {
            server = HttpServer.create(new InetSocketAddress(bindHost, httpPort), 128);
        }
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        HttpGateway gateway = new HttpGateway(server, workers, tcpHost, tcpPort);
        server.setExecutor(workers);
        server.createContext("/api/v1/health", gateway::health);
        server.createContext("/api/v1/request", gateway::request);
        server.createContext("/api/v1/login", gateway::request);
        server.createContext("/api/v1/logout", gateway::request);
        server.createContext("/api/v1/query", gateway::request);
        server.createContext("/api/v1/bulk", gateway::request);
        server.createContext("/api/v1/admin", gateway::request);
        server.createContext("/api/v1/cancel", gateway::request);
        server.createContext("/api/v1/queries", gateway::request);
        server.createContext("/api/v1/capabilities", gateway::capabilities);
        server.start();
        return gateway;
    }

    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        sessions.values().forEach(TcpConnection::close);
        sessions.clear();
        workers.shutdownNow();
    }

    private void health(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "{\"code\":\"SYNTAX\",\"message\":\"only GET is supported\"}");
            return;
        }
        send(exchange, 200, "{\"code\":\"OK\"}");
    }

    private void capabilities(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "{\"code\":\"SYNTAX\",\"message\":\"only GET is supported\"}");
            return;
        }
        send(exchange, 200, "{\"code\":\"OK\",\"protocol\":\"1\",\"bulkInsert\":true,\"logout\":true}");
    }

    private void request(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "{\"code\":\"SYNTAX\",\"message\":\"only POST is supported\"}");
            return;
        }
        byte[] body;
        try (InputStream input = exchange.getRequestBody()) {
            body = input.readNBytes(MAX_BODY + 1);
        }
        if (body.length > MAX_BODY) {
            send(exchange, 413, "{\"code\":\"SYNTAX\",\"message\":\"request body is too large\"}");
            return;
        }
        try {
            JsonNode request = MAPPER.readTree(body);
            if (request == null || !request.isObject()) {
                send(exchange, 400, "{\"code\":\"SYNTAX\",\"message\":\"request must be an object\"}");
                return;
            }
            String operation = request.path("op").asText("");
            String session = request.path("session").asText("");
            String line = proxy(MAPPER.writeValueAsString(request), operation, session);
            JsonNode response = MAPPER.readTree(line);
            int status = "OK".equals(response.path("code").asText()) ? 200 : 400;
            send(exchange, status, line);
        } catch (Exception ex) {
            send(exchange, 502, "{\"code\":\"UNAVAILABLE\",\"message\":\"" + escape(ex.getMessage()) + "\"}");
        }
    }

    private String proxy(String request, String operation, String sessionId) throws IOException {
        boolean persistent = !sessionId.isBlank() || "login".equals(operation);
        TcpConnection connection = persistent && !sessionId.isBlank() ? sessions.get(sessionId) : new TcpConnection(tcpHost, tcpPort);
        boolean stored = false;
        try {
            String line = connection.exchange(request);
            JsonNode response = MAPPER.readTree(line);
            String returnedSession = response.path("session").asText("");
            if ("login".equals(operation) && "OK".equals(response.path("code").asText()) && !returnedSession.isBlank()) {
                sessions.put(returnedSession, connection);
                stored = true;
            }
            if ("logout".equals(operation) && !sessionId.isBlank()) {
                sessions.remove(sessionId, connection);
            }
            return line;
        } finally {
            if (!stored && (!persistent || "logout".equals(operation))) {
                connection.close();
            }
        }
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private static String escape(String value) {
        if (value == null) return "http proxy failed";
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", " ").replace("\n", " ");
    }

    private static final class TcpConnection {
        private final Socket socket;

        private TcpConnection(String host, int port) throws IOException {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), 10_000);
            socket.setSoTimeout(0);
        }

        private synchronized String exchange(String request) throws IOException {
            socket.getOutputStream().write((request + "\n").getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            StringBuilder text = new StringBuilder();
            int value;
            while ((value = socket.getInputStream().read()) >= 0 && value != '\n') {
                text.append((char) value);
            }
            if (text.isEmpty()) throw new IOException("TCP server closed HTTP session connection");
            return text.toString();
        }

        private void close() {
            try { socket.close(); } catch (IOException ignored) { }
        }
    }
}
