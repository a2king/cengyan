package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.cengyan.studio.StudioConfig;
import io.cengyan.studio.StudioServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ClusterApiTest {
    @TempDir Path tmp;
    private final HttpClient http = HttpClient.newHttpClient();

    private record Reply(int status, JsonNode body) { }

    private Reply call(int port, String method, String path, String token, String json) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/cluster/" + path));
        if (token != null) request.header("X-Cengyan-Session", token);
        request.method(method, json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json));
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), ClusterStore.JSON.readTree(response.body()));
    }

    /** Studio 根路径下的接口（/api/login 等）。 */
    private Reply api(int port, String path, String token, String json) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
        if (token != null) request.header("X-Cengyan-Session", token);
        HttpResponse<String> response = http.send(request.POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), ClusterStore.JSON.readTree(response.body()));
    }

    private Reply get(int port, String path, String token) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
        if (token != null) request.header("X-Cengyan-Session", token);
        HttpResponse<String> response = http.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), ClusterStore.JSON.readTree(response.body()));
    }

    private static StudioConfig config(Path home) { return config(home, 1); }

    private static StudioConfig config(Path home, int dbPort) {
        return new StudioConfig(0, "127.0.0.1", "127.0.0.1", dbPort, 2000, 2000, "", 0, "", "", home.toString());
    }

    private static String login(String user, String password) {
        return "{\"user\":\"" + user + "\",\"password\":\"" + password + "\"}";
    }

    /** 首次部署：设置 root 密码即以 root 本地登录；数据库不可用时 root 本地兜底登录，失败多次锁定。 */
    @Test void setupRootPasswordAndLocalFallback() throws Exception {
        Path home = Fakes.home(tmp.resolve("home"));
        try (StudioServer studio = StudioServer.start(config(home))) {
            int port = studio.port();
            JsonNode status = call(port, "GET", "admin/status", null, null).body();
            assertFalse(status.path("initialized").asBoolean());
            assertFalse(status.path("legacy").asBoolean());
            assertEquals(401, call(port, "GET", "overview", null, null).status(), "未登录不能访问");
            assertEquals("INVALID_ARGUMENT", call(port, "POST", "admin/setup", null, "{\"password\":\"short\"}").body().path("code").asText());
            JsonNode setup = call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-1\"}").body();
            String token = setup.path("session").asText();
            assertTrue(token.startsWith("local."), setup.toString());
            assertEquals("admin", setup.path("role").asText());
            assertEquals(409, call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-2\"}").status(), "只能初始化一次");
            Reply overview = call(port, "GET", "overview", token, null);
            assertEquals(200, overview.status());
            assertTrue(overview.body().path("clusterId").asText().startsWith("cengyan-"));
            assertTrue(overview.body().path("rootPasswordSet").asBoolean());
            assertEquals(401, call(port, "GET", "overview", "local.forged", null).status());
            Reply catalog = call(port, "GET", "catalog", token, null);
            assertTrue(catalog.body().path("data").path("STORAGE").toString().contains("写入线程数"));
            assertEquals(404, call(port, "GET", "nope", token, null).status());
            assertEquals(404, call(port, "POST", "root-password", token, "{\"password\":\"root-pass-3\"}").status(), "单独的 root 密码接口已去掉");
            JsonNode session = api(port, "/api/session", token, "{}").body();
            assertEquals("OK", session.path("code").asText());
            assertTrue(session.path("local").asBoolean());
            Reply query = api(port, "/api/query", token, "{\"query\":\"SHOW GRAPHS\"}");
            assertEquals("LOCAL_SESSION", query.body().path("code").asText(), "本地会话不能用 GQL");
            assertEquals(200, get(port, "/api/ops/shards", token).status(), "本地会话可以看运维监控");
            // 没有计算节点时，本地会话可以改 root 密码。
            assertEquals("UNAUTHORIZED", api(port, "/api/password", token, "{\"current\":\"wrong\",\"password\":\"root-pass-2\"}").body().path("code").asText());
            assertEquals("OK", api(port, "/api/password", token, "{\"current\":\"root-pass-1\",\"password\":\"root-pass-2\"}").body().path("code").asText());
            api(port, "/api/logout", token, "{}");
            assertEquals(401, call(port, "GET", "overview", token, null).status(), "退出后本地会话失效");
        }
        // 本地会话只在内存中；root 密码保存在 state 里，数据库（端口 1）不可用时 root 本地兜底登录。
        try (StudioServer studio = StudioServer.start(config(home))) {
            int port = studio.port();
            assertTrue(call(port, "GET", "admin/status", null, null).body().path("initialized").asBoolean());
            assertEquals("UNAVAILABLE", api(port, "/api/login", null, login("someone", "root-pass-2")).body().path("code").asText(), "只有 root 能兜底，其他用户看到数据库不可用");
            JsonNode local = api(port, "/api/login", null, login("root", "root-pass-2")).body();
            assertEquals("OK", local.path("code").asText(), local.toString());
            assertTrue(local.path("local").asBoolean());
            assertEquals(200, call(port, "GET", "overview", local.path("session").asText(), null).status());
            for (int i = 0; i < 5; i++) assertEquals("UNAUTHENTICATED", api(port, "/api/login", null, login("root", "wrong")).body().path("code").asText());
            assertEquals("LOCKED", api(port, "/api/login", null, login("root", "root-pass-2")).body().path("code").asText(), "多次失败后锁定");
        }
    }

    /** 旧版本设置过 Studio 管理员密码、还没设置 root 密码：先输入旧管理员密码才能初始化。 */
    @Test void legacyAdminPasswordGuardsSetup() throws Exception {
        Path home = Fakes.home(tmp.resolve("legacy-home"));
        Files.createDirectories(home.resolve("data"));
        byte[] salt = new byte[16];
        var spec = new javax.crypto.spec.PBEKeySpec("old-admin-pass".toCharArray(), salt, 1000, 256);
        String hash = java.util.Base64.getEncoder().encodeToString(javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded());
        Files.writeString(home.resolve("data/admin.json"), "{\"salt\":\"" + java.util.Base64.getEncoder().encodeToString(salt) + "\",\"hash\":\"" + hash + "\",\"iterations\":1000}");
        try (StudioServer studio = StudioServer.start(config(home))) {
            int port = studio.port();
            assertTrue(call(port, "GET", "admin/status", null, null).body().path("legacy").asBoolean());
            assertEquals("UNAUTHORIZED", call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-1\"}").body().path("code").asText());
            assertEquals("UNAUTHORIZED", call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-1\",\"legacyPassword\":\"nope\"}").body().path("code").asText());
            JsonNode setup = call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-1\",\"legacyPassword\":\"old-admin-pass\"}").body();
            assertEquals("OK", setup.path("code").asText(), setup.toString());
            assertFalse(call(port, "GET", "admin/status", null, null).body().path("legacy").asBoolean());
        }
    }

    /** 数据库能回答时不走兜底；集群管理只给 admin，运维监控所有角色；root 登录成功后同步保存的密码。 */
    @Test void databaseSessionsAndRoles() throws Exception {
        Path home = Fakes.home(tmp.resolve("db-home"));
        java.util.Map<String, String[]> sessions = java.util.Map.of("db-root", new String[] {"root", "admin"},
                "db-writer", new String[] {"writer", "write"}, "db-reader", new String[] {"reader", "read"});
        try (var db = new io.cengyan.studio.FakeDb(request -> switch (request.path("op").asText()) {
            case "login" -> request.path("user").asText().equals("root") && request.path("password").asText().equals("root-pass-cli")
                    ? "{\"code\":\"OK\",\"session\":\"db-root\",\"token\":\"db-root\",\"columns\":[\"user\",\"role\"],\"rows\":[[\"root\",\"admin\"]]}"
                    : "{\"code\":\"UNAUTHENTICATED\",\"message\":\"用户名或密码不正确\"}";
            case "session" -> {
                String[] who = sessions.get(request.path("session").asText());
                yield who == null ? "{\"code\":\"UNAUTHENTICATED\",\"message\":\"令牌无效\"}"
                        : "{\"code\":\"OK\",\"user\":\"" + who[0] + "\",\"role\":\"" + who[1] + "\"}";
            }
            default -> "{\"code\":\"OK\"}";
        }); StudioServer studio = StudioServer.start(config(home, db.port()))) {
            int port = studio.port();
            assertEquals("OK", call(port, "POST", "admin/setup", null, "{\"password\":\"root-pass-1\"}").body().path("code").asText());
            // 数据库能连上但密码错误（Studio 保存的是旧密码）：不走兜底。
            assertEquals("UNAUTHENTICATED", api(port, "/api/login", null, login("root", "root-pass-1")).body().path("code").asText());
            // root 在命令行改过密码：数据库登录成功后，Studio 保存的 root 密码同步为新密码。
            JsonNode root = api(port, "/api/login", null, login("root", "root-pass-cli")).body();
            assertEquals("db-root", root.path("session").asText());
            assertTrue(Files.readString(home.resolve("data/state.json")).contains("root-pass-cli"), "root 密码已同步");

            assertEquals(200, call(port, "GET", "overview", "db-root", null).status());
            assertEquals(403, call(port, "GET", "overview", "db-writer", null).status(), "write 不能进集群管理");
            assertEquals(403, call(port, "GET", "overview", "db-reader", null).status(), "read 不能进集群管理");
            assertEquals(401, call(port, "GET", "overview", "db-unknown", null).status());
            assertEquals(200, get(port, "/api/ops/shards", "db-reader").status(), "read 可以看运维监控");
            assertEquals(200, get(port, "/api/ops/overview", "db-writer").status());
            assertEquals(401, get(port, "/api/ops/overview", null).status(), "运维监控需要登录");
        }
    }

    @Test void clusterApiIsAbsentWithoutHome() throws Exception {
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", 1))) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + studio.port() + "/api/cluster/admin/status")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertNotEquals(200, response.statusCode());
        }
    }

    @Test void probeRules() {
        var ok = new Remote.Probe("x86_64", "Rocky 9", "h", java.util.List.of("10.0.0.5"), "2.34", true, 5000, true, true);
        assertEquals("10.0.0.5", Remote.require(ok, "10.0.0.5"));
        assertEquals("10.0.0.5", Remote.clusterAddress("db-1.internal", java.util.List.of("10.0.0.5"), java.util.List.of("10.0.0.5")));
        assertThrows(ClusterError.class, () -> Remote.clusterAddress("craw-plat1", java.util.List.of("10.0.0.5"), java.util.List.of()), "解析不了的主机名不能写入 Raft 地址");
        assertThrows(ClusterError.class, () -> Remote.require(ok, "10.0.0.9"), "填写的 IP 必须在服务器网卡上");
        assertThrows(ClusterError.class, () -> Remote.require(new Remote.Probe("x86_64", "Alpine", "h", java.util.List.of(), "musl", true, 5000, true, true), "h"));
        assertThrows(ClusterError.class, () -> Remote.require(new Remote.Probe("ppc64le", "x", "h", java.util.List.of(), "2.34", true, 5000, true, true), "h"));
        assertThrows(ClusterError.class, () -> Remote.require(new Remote.Probe("x86_64", "x", "h", java.util.List.of(), "2.34", false, 5000, true, true), "h"));
        assertFalse(Remote.safeDataDir("/"));
        assertFalse(Remote.safeDataDir("/data"));
        assertFalse(Remote.safeDataDir("/etc/cengyan"));
        assertFalse(Remote.safeDataDir("/opt/cengyan/jdk/x"));
        assertFalse(Remote.safeDataDir("/data/../etc"));
        assertFalse(Remote.safeDataDir("/data/a b"));
        assertTrue(Remote.safeDataDir("/data1/cengyan/storage-1"));
        assertEquals("'it'\"'\"'s'", RemoteShell.quote("it's"));
    }

    @Test void opsOverviewReadsLocalSnapshot() throws Exception {
        Path home = Fakes.home(tmp.resolve("ops-home"));
        try (StudioServer studio = StudioServer.start(config(home))) {
            String token = call(studio.port(), "POST", "admin/setup", null, "{\"password\":\"root-pass-1\"}").body().path("session").asText();
            Reply reply = get(studio.port(), "/api/ops/overview", token);
            assertEquals(200, reply.status());
            JsonNode body = reply.body();
            assertEquals("OK", body.path("code").asText());
            assertEquals("UNKNOWN", body.path("clusterHealth").asText());
            assertTrue(body.path("storageNodes").isArray());
            assertTrue(body.path("events").isArray());
            assertEquals(0, body.path("shardSummary").path("total").asInt());
            assertTrue(Files.exists(home.resolve("data/ops.db")));

            JsonNode series = get(studio.port(), "/api/ops/series?range=7d", token).body();
            assertEquals("7d", series.path("range").asText());
            assertTrue(series.path("hosts").isArray());
            JsonNode fallback = get(studio.port(), "/api/ops/series?range=bogus", token).body();
            assertEquals("1h", fallback.path("range").asText(), "不认识的范围按 1 小时");
            JsonNode shards = get(studio.port(), "/api/ops/shards", token).body();
            assertTrue(shards.path("shards").isArray());
        }
    }

    @Test void sshKeyIsGeneratedOnceInOpenSshFormat() throws Exception {
        var factory = new SshShell.Factory(tmp);
        String key = factory.publicKey();
        assertTrue(key.matches("ssh-rsa [A-Za-z0-9+/=]+"), key);
        assertEquals(key, new SshShell.Factory(tmp).publicKey(), "已有密钥不重新生成");
        assertNotNull(com.jcraft.jsch.KeyPair.load(new com.jcraft.jsch.JSch(), tmp.resolve("ssh/id_rsa").toString()));
    }
}
