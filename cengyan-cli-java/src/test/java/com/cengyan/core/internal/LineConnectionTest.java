package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.exception.ConnectionException;
import com.cengyan.core.exception.ProtocolException;
import com.cengyan.core.exception.ReadTimeoutException;
import com.cengyan.core.exception.RequestTooLargeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineConnectionTest {
    /** 本地行协议服务器：每读到一行，按 handler 写回若干字节（null 表示关闭连接）。 */
    static final class LineServer implements AutoCloseable {
        interface Handler {
            String reply(String line) throws Exception;
        }

        final ServerSocket server;
        final List<String> received = Collections.synchronizedList(new ArrayList<String>());
        final AtomicInteger accepted = new AtomicInteger();
        private final Thread thread;

        LineServer(Handler handler) throws IOException {
            server = new ServerSocket(0);
            thread = new Thread(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket socket = server.accept();
                        accepted.incrementAndGet();
                        Thread worker = new Thread(() -> serve(socket, handler));
                        worker.setDaemon(true);
                        worker.start();
                    } catch (IOException ex) {
                        return;
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        private void serve(Socket socket, Handler handler) {
            try (Socket s = socket;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
                OutputStream out = s.getOutputStream();
                String line;
                while ((line = reader.readLine()) != null) {
                    received.add(line);
                    String reply = handler.reply(line);
                    if (reply == null) return;
                    out.write(reply.getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
            } catch (Exception ignored) {
                // 测试服务器
            }
        }

        Address address() {
            return new Address("127.0.0.1", server.getLocalPort());
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private LineServer server;

    @AfterEach
    void stop() throws IOException {
        if (server != null) server.close();
    }

    private static Map<String, Object> ping() {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "ping");
        return payload;
    }

    @Test
    void requestsAreSentAsLinesAndConnectionIsReused() throws Exception {
        server = new LineServer(line -> "{\"code\":\"OK\",\"echo\":" + line + "}\n");
        LineConnection connection = new LineConnection(server.address(), null);
        try {
            for (int i = 0; i < 3; i++) assertEquals("OK", connection.request(ping(), 5000).get("code"));
            assertEquals(1, server.accepted.get());
            assertEquals("{\"op\":\"ping\"}", server.received.get(0));
            assertTrue(connection.reusable());
        } finally {
            connection.close();
        }
    }

    @Test
    void responseSplitAcrossReadsAndLargerThanBufferIsAssembled() throws Exception {
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 200_000; i++) big.append('x');
        server = new LineServer(line -> "{\"code\":\"OK\",\"v\":\"" + big + "\"}\n{\"code\":\"OK\",\"second\":true}\n");
        LineConnection connection = new LineConnection(server.address(), null);
        try {
            assertEquals(200_000, ((String) connection.request(ping(), 5000).get("v")).length());
        } finally {
            connection.close();
        }
    }

    @Test
    void timeoutPoisonsConnection() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        server = new LineServer(line -> {
            release.await(5, TimeUnit.SECONDS);
            return "{\"code\":\"OK\"}\n";
        });
        LineConnection connection = new LineConnection(server.address(), null);
        assertThrows(ReadTimeoutException.class, () -> connection.request(ping(), 200));
        assertFalse(connection.reusable());
        release.countDown();
        assertThrows(ConnectionException.class, () -> connection.request(ping(), 200));
    }

    @Test
    void serverCloseAndBadJsonAreReported() throws Exception {
        server = new LineServer(line -> line.contains("close") ? null : "not json\n");
        LineConnection closed = new LineConnection(server.address(), null);
        Map<String, Object> payload = ping();
        payload.put("close", true);
        assertThrows(ConnectionException.class, () -> closed.request(payload, 5000));
        assertFalse(closed.reusable());

        LineConnection bad = new LineConnection(server.address(), null);
        assertThrows(ProtocolException.class, () -> bad.request(ping(), 5000));
        assertFalse(bad.reusable());
    }

    @Test
    void unreachableNodeIsAConnectionError() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        LineConnection connection = new LineConnection(new Address("127.0.0.1", port), null);
        assertThrows(ConnectionException.class, () -> connection.request(ping(), 2000));
    }

    @Test
    void oversizedAndUnencodableRequestsAreNotSentAndKeepConnectionUsable() throws Exception {
        server = new LineServer(line -> "{\"code\":\"OK\"}\n");
        LineConnection connection = new LineConnection(server.address(), null);
        try {
            StringBuilder text = new StringBuilder();
            while (text.length() <= LineConnection.MAX_LINE_BYTES) text.append("0123456789abcdef");
            Map<String, Object> huge = ping();
            huge.put("query", text.toString());
            assertThrows(RequestTooLargeException.class, () -> connection.request(huge, 5000));
            Map<String, Object> nan = ping();
            nan.put("v", Double.NaN);
            assertThrows(IllegalArgumentException.class, () -> connection.request(nan, 5000));
            assertEquals(0, server.accepted.get());
            assertTrue(connection.reusable());
            assertEquals("OK", connection.request(ping(), 5000).get("code"));
        } finally {
            connection.close();
        }
    }
}
