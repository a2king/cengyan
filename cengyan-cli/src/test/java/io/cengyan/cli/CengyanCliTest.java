package io.cengyan.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CengyanCliTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void 旧单机管理命令拒绝并保留集群字符串身份() throws Exception {
        var cli = new CengyanCli();
        for (String command : new String[] {":addStorage host:123", ":addCompute host:123", ":setPartCount G 4", ":rebuildIndex i"}) {
            assertThrows(IllegalArgumentException.class, () -> cli.request(command));
        }
        assertEquals("storage-3", MAPPER.readTree(cli.request(":removeStorage storage-3")).path("nodeId").asText());
        assertTrue(MAPPER.readTree(cli.request(":removeStorage 3")).path("nodeId").isTextual());
        cli.request("USE Company");
        assertEquals("Company", MAPPER.readTree(cli.request("MATCH (n) RETURN n")).path("graph").asText());
    }

    @Test
    void 普通文本包成execute() throws Exception {
        JsonNode node = MAPPER.readTree(new CengyanCli().request("RETURN 1"));
        assertEquals("execute", node.get("op").asText());
        assertEquals("RETURN 1", node.get("query").asText());
    }

    @Test
    void ping和冒号命令() throws Exception {
        CengyanCli cli = new CengyanCli();
        assertEquals("ping", MAPPER.readTree(cli.request("ping")).get("op").asText());
        JsonNode flush = MAPPER.readTree(cli.request(":flush"));
        assertEquals("admin", flush.get("op").asText());
        assertEquals("flush", flush.get("command").asText());
        assertEquals("", flush.get("session").asText());
        JsonNode login = MAPPER.readTree(cli.request(":login root cengyan"));
        assertEquals("login", login.get("op").asText());
        assertEquals("root", login.get("user").asText());
        assertEquals("cengyan", login.get("password").asText());
        JsonNode stats = MAPPER.readTree(cli.request(":stats"));
        assertEquals("stats", stats.get("command").asText());
        assertFalse(stats.path("refresh").asBoolean(false));
        JsonNode refreshed = MAPPER.readTree(cli.request(":stats refresh"));
        assertEquals("stats", refreshed.get("command").asText());
        assertTrue(refreshed.get("refresh").asBoolean());
        JsonNode created = MAPPER.readTree(cli.request(":createUser reader secret read"));
        assertEquals("admin", created.get("op").asText());
        assertEquals("createUser", created.get("command").asText());
        assertEquals("reader", created.get("user").asText());
        assertEquals("read", created.get("role").asText());
    }
}
