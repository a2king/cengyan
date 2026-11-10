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
        if (token != null) request.header("X-Cengyan-Admin", token);
        request.method(method, json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json));
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), ClusterStore.JSON.readTree(response.body()));
    }

    private static StudioConfig config(Path home) {
        return new StudioConfig(0, "127.0.0.1", "127.0.0.1", 1, 2000, 2000, "", 0, "", "", home.toString());
    }

    @Test void adminSetupLoginAndProtectedEndpoints() throws Exception {
        Path home = Fakes.home(tmp.resolve("home"));
        try (StudioServer studio = StudioServer.start(config(home))) {
            int port = studio.port();
            assertFalse(call(port, "GET", "admin/status", null, null).body().path("initialized").asBoolean());
            assertEquals(401, call(port, "GET", "overview", null, null).status(), "未登录不能访问");
            assertEquals("INVALID_ARGUMENT", call(port, "POST", "admin/setup", null, "{\"password\":\"short\"}").body().path("code").asText());
            String token = call(port, "POST", "admin/setup", null, "{\"password\":\"admin-pass-1\"}").body().path("token").asText();
            assertFalse(token.isBlank());
            assertEquals(409, call(port, "POST", "admin/setup", null, "{\"password\":\"admin-pass-2\"}").status(), "只能初始化一次");
            Reply overview = call(port, "GET", "overview", token, null);
            assertEquals(200, overview.status());
            assertTrue(overview.body().path("clusterId").asText().startsWith("cengyan-"));
            assertEquals(401, call(port, "GET", "overview", "forged", null).status());
            Reply catalog = call(port, "GET", "catalog", token, null);
            assertTrue(catalog.body().path("data").path("STORAGE").toString().contains("写入线程数"));
            assertEquals(404, call(port, "GET", "nope", token, null).status());
            for (int i = 0; i < 5; i++) assertEquals(401, call(port, "POST", "admin/login", null, "{\"password\":\"wrong\"}").status());
            assertEquals("LOCKED", call(port, "POST", "admin/login", null, "{\"password\":\"admin-pass-1\"}").body().path("code").asText(), "多次失败后锁定");
            assertTrue(Files.readString(home.resolve("data/admin.json")).contains("hash"));
            assertFalse(Files.readString(home.resolve("data/admin.json")).contains("admin-pass-1"), "不保存明文密码");
        }
        // Sessions are in memory; the hash persists.
        try (StudioServer studio = StudioServer.start(config(home))) {
            assertTrue(call(studio.port(), "GET", "admin/status", null, null).body().path("initialized").asBoolean());
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
        Remote.require(ok, "10.0.0.5");
        Remote.require(ok, "db-1.internal");
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

    @Test void sshKeyIsGeneratedOnceInOpenSshFormat() throws Exception {
        var factory = new SshShell.Factory(tmp);
        String key = factory.publicKey();
        assertTrue(key.matches("ssh-rsa [A-Za-z0-9+/=]+"), key);
        assertEquals(key, new SshShell.Factory(tmp).publicKey(), "已有密钥不重新生成");
        assertNotNull(com.jcraft.jsch.KeyPair.load(new com.jcraft.jsch.JSch(), tmp.resolve("ssh/id_rsa").toString()));
    }
}
