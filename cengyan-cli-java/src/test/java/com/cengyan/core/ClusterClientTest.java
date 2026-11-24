package com.cengyan.core;

import com.cengyan.core.exception.AuthenticationException;
import com.cengyan.core.exception.CommitUncertainException;
import com.cengyan.core.exception.GqlSyntaxException;
import com.cengyan.core.exception.InternalException;
import com.cengyan.core.internal.Transport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterClientTest {
    /** 假传输：记录 (payload, write)，按 handler 返回响应。 */
    static final class Recorder implements Transport {
        final List<Map<String, Object>> requests = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
        final List<Boolean> writes = Collections.synchronizedList(new ArrayList<Boolean>());
        final List<Address> targets = Collections.synchronizedList(new ArrayList<Address>());
        List<Address> updated;
        int closed;
        private final Function<Map<String, Object>, Map<String, Object>> handler;

        Recorder(Function<Map<String, Object>, Map<String, Object>> handler) {
            this.handler = handler;
        }

        @Override
        public Map<String, Object> request(Map<String, Object> payload, Long timeoutMillis, boolean write, Address address) {
            requests.add(new LinkedHashMap<String, Object>(payload));
            writes.add(write);
            targets.add(address);
            return handler.apply(payload);
        }

        @Override
        public void updateAddresses(List<Address> addresses) {
            updated = addresses;
        }

        @Override
        public void close() {
            closed++;
        }

        List<Map<String, Object>> ops(String op) {
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            for (Map<String, Object> request : requests) if (op.equals(request.get("op"))) list.add(request);
            return list;
        }
    }

    static Map<String, Object> response(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i + 1 < pairs.length; i += 2) map.put((String) pairs[i], pairs[i + 1]);
        return map;
    }

    /** 登录返回 t1；其余交给 handler。 */
    static Recorder recorder(Function<Map<String, Object>, Map<String, Object>> handler) {
        return new Recorder(payload -> "login".equals(payload.get("op")) ? response("code", "OK", "token", "t1") : handler.apply(payload));
    }

    @Test
    void firstRequestLogsInAndSendsTokenAndGraph() {
        Recorder transport = recorder(p -> response("code", "OK", "columns", Arrays.asList("n"), "rows", Arrays.asList(Arrays.asList(1L))));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        Result result = client.graph("g").read("MATCH (n) RETURN n");
        assertEquals(1L, result.scalar());
        Map<String, Object> login = transport.requests.get(0);
        assertEquals("login", login.get("op"));
        assertEquals("root", login.get("user"));
        assertEquals("secret", login.get("password"));
        Map<String, Object> execute = transport.requests.get(1);
        assertEquals("t1", execute.get("token"));
        assertEquals("g", execute.get("graph"));
        assertEquals(Collections.emptyMap(), execute.get("params"));
        assertFalse(execute.containsKey("requestId"));
        assertEquals(Arrays.asList(false, false), transport.writes);
    }

    @Test
    void writeGetsRequestIdOnceAndReloginResendsSameId() {
        AtomicInteger executes = new AtomicInteger();
        Recorder transport = new Recorder(p -> {
            if ("login".equals(p.get("op"))) return response("code", "OK", "token", executes.get() == 0 ? "t1" : "t2");
            return executes.incrementAndGet() == 1 ? response("code", "UNAUTHENTICATED") : response("code", "OK");
        });
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        client.graph("g").write("INSERT (n:P {id: 1})");
        List<Map<String, Object>> sent = transport.ops("execute");
        assertEquals(2, sent.size());
        assertEquals(sent.get(0).get("requestId"), sent.get(1).get("requestId"));
        assertTrue(String.valueOf(sent.get(0).get("requestId")).length() > 0);
        assertEquals("t1", sent.get(0).get("token"));
        assertEquals("t2", sent.get(1).get("token"));
        assertEquals(Arrays.asList(false, true, false, true), transport.writes);
    }

    @Test
    void callerRequestIdIsKeptAndCallerPayloadIsNotModified() {
        Recorder transport = recorder(p -> response("code", "OK"));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        Map<String, Object> payload = response("op", "execute", "query", "q", "requestId", "mine");
        client.request(payload, true);
        assertEquals("mine", transport.ops("execute").get(0).get("requestId"));
        assertFalse(payload.containsKey("token"));
    }

    @Test
    void concurrentThreadsShareOneRelogin() throws Exception {
        AtomicInteger logins = new AtomicInteger();
        Recorder transport = new Recorder(p -> {
            logins.incrementAndGet();
            return response("code", "OK", "token", "t2");
        });
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        client.relogin(null);
        logins.set(0);
        CyclicBarrier gate = new CyclicBarrier(2);
        List<String> results = Collections.synchronizedList(new ArrayList<String>());
        Runnable worker = () -> {
            try {
                gate.await();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
            results.add(client.relogin("t1"));
        };
        Thread a = new Thread(worker);
        Thread b = new Thread(worker);
        a.start();
        b.start();
        a.join();
        b.join();
        assertEquals(Arrays.asList("t2", "t2"), results);
        assertEquals(0, logins.get(), "令牌已经不是 t1，不需要再登录");
        client.relogin("t2");
        assertEquals(1, logins.get());
    }

    @Test
    void failedIsUncertainOnlyForWrites() {
        Recorder transport = recorder(p -> response("code", "FAILED", "message", "NullPointerException"));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        assertThrows(InternalException.class, () -> client.graph("g").read("MATCH (n) RETURN n"));
        CommitUncertainException error = assertThrows(CommitUncertainException.class, () -> client.graph("g").write("INSERT (n:P)"));
        // 服务端响应没有回带 requestId 时，异常上也必须能取到本次写请求的 id。
        assertEquals(transport.ops("execute").get(1).get("requestId"), error.requestId());
    }

    @Test
    void serverErrorsKeepTheirType() {
        Recorder transport = recorder(p -> response("code", "SYNTAX", "message", "bad", "queryId", "q-1"));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        GqlSyntaxException error = assertThrows(GqlSyntaxException.class, () -> client.execute("SHOW"));
        assertEquals("SYNTAX", error.code());
        assertEquals("q-1", error.queryId());
        assertEquals("SYNTAX bad", error.getMessage());
    }

    @Test
    void badPasswordLogsInOnceAndMissingTokenIsAuthenticationError() {
        AtomicInteger logins = new AtomicInteger();
        ClusterClient denied = new ClusterClient(new Recorder(p -> {
            logins.incrementAndGet();
            return response("code", "UNAUTHENTICATED", "message", "密码错误");
        }), "root", "wrong");
        assertThrows(AuthenticationException.class, () -> denied.execute("SHOW GRAPHS"));
        assertEquals(1, logins.get());

        ClusterClient empty = new ClusterClient(new Recorder(p -> response("code", "OK")), "root", "p");
        assertThrows(AuthenticationException.class, empty::login);
    }

    @Test
    void adminIsReadOnlyUnlessWriteIsExplicitAndCannotOverrideProtocolFields() {
        Recorder transport = recorder(p -> response("code", "OK"));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        client.admin("stats", false, Collections.singletonMap("graph", "g"));
        client.admin("showMembers");
        client.admin("setConfiguration", true, Collections.singletonMap("voters", Collections.emptyList()));
        List<Map<String, Object>> admins = transport.ops("admin");
        assertEquals("g", admins.get(0).get("graph"));
        assertEquals(Arrays.asList(false, false, false, true), transport.writes);
        for (String field : new String[] {"op", "command", "token", "session"}) {
            assertThrows(IllegalArgumentException.class, () -> client.admin("stats", false, Collections.singletonMap(field, "x")));
        }
        assertEquals(3, admins.size());
    }

    @Test
    void createGraphIsAWriteWithExplicitIdTypes() {
        Recorder transport = recorder(p -> response("code", "OK", "graphId", 7L));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        assertEquals(7L, client.createGraph("Company", 3, 1, IdType.STRING, IdType.INT64).get("graphId"));
        Map<String, Object> sent = transport.ops("createGraph").get(0);
        assertEquals(3, sent.get("partitions"));
        assertEquals("STRING", sent.get("vertexIdType"));
        assertTrue(transport.writes.get(1));
        assertTrue(sent.containsKey("requestId"));
        assertThrows(IllegalArgumentException.class, () -> client.createGraph("x", 0));
    }

    @Test
    void refreshNodesReplacesTransportAddresses() {
        Map<String, Object> node = response("host", "10.0.0.7", "port", 19669L);
        Recorder transport = recorder(p -> response("code", "OK", "addresses", Arrays.asList(node)));
        ClusterClient client = new ClusterClient(transport, "root", "secret");
        assertEquals(Arrays.asList(new Address("10.0.0.7", 19669)), client.refreshNodes());
        assertEquals(Arrays.asList(new Address("10.0.0.7", 19669)), transport.updated);
    }

    @Test
    void builderValidatesAndCloseClosesTransport() {
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.builder().addresses("a:1").build());
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.builder().password("p").build());
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.builder().addresses("a:1", "a:1").password("p").build());
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.builder().timeout(java.time.Duration.ZERO));
        Recorder transport = recorder(p -> response("code", "OK"));
        try (ClusterClient client = new ClusterClient(transport, "root", "secret")) {
            client.login();
        }
        assertEquals(1, transport.closed);
    }

    @Test
    void graphHelpersMarkReadAndWriteModes() {
        Recorder transport = recorder(p -> response("code", "OK"));
        ClusterGraph graph = new ClusterClient(transport, "root", "secret").graph("g");
        graph.read("MATCH (n) RETURN n.id");
        graph.write("INSERT (n {id: 1})");
        graph.execute("MATCH (n) RETURN n", Collections.singletonMap("x", 1), false, null);
        assertEquals(Arrays.asList(false, false, true, false), transport.writes);
        assertEquals(Collections.singletonMap("x", 1), transport.ops("execute").get(2).get("params"));
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(transport, "root", "secret").graph(""));
    }

    @Test
    void endToEndThroughRealTransportStack() throws Exception {
        // 用真实 ComputeTransport + 连接池，只替换最底层连接：写请求断线重试时 requestId 不变。
        AtomicInteger calls = new AtomicInteger();
        List<Object> ids = Collections.synchronizedList(new ArrayList<Object>());
        CountDownLatch done = new CountDownLatch(1);
        ClusterClient client = ClusterClient.builder().addresses("a:1", "b:2").password("p")
                .connectionFactory(address -> new com.cengyan.core.internal.Connection() {
                    private boolean broken;

                    @Override
                    public Map<String, Object> request(Map<String, Object> payload, long timeoutMillis) {
                        if ("login".equals(payload.get("op"))) return response("code", "OK", "token", "t");
                        ids.add(payload.get("requestId"));
                        if (calls.incrementAndGet() == 1) {
                            broken = true;
                            throw new com.cengyan.core.exception.ConnectionException("lost", "CONNECTION_ERROR", null);
                        }
                        done.countDown();
                        return response("code", "OK", "rowsAffected", 1L);
                    }

                    @Override
                    public boolean reusable() {
                        return !broken;
                    }

                    @Override
                    public Address address() {
                        return address;
                    }

                    @Override
                    public void close() {
                    }
                }).build();
        try {
            assertEquals(1L, client.graph("g").write("INSERT (n:P {id: 1})").rowsAffected());
            assertEquals(2, ids.size());
            assertEquals(ids.get(0), ids.get(1));
        } finally {
            client.close();
        }
    }
}
