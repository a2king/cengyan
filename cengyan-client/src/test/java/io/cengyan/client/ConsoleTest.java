package io.cengyan.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsoleTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void removedStandaloneCommandsStayRejected() throws Exception {
        var console = new Console();
        for (String command : new String[] {":addStorage host:123", ":addCompute host:123", ":setPartCount G 4", ":rebuildIndex i"}) {
            assertThrows(IllegalArgumentException.class, () -> console.request(command));
        }
        assertEquals("storage-3", MAPPER.readTree(console.request(":removeStorage storage-3")).path("nodeId").asText());
        console.request("USE Company");
        assertEquals("Company", MAPPER.readTree(console.request("MATCH (n) RETURN n")).path("graph").asText());
    }

    @Test
    void plainTextBecomesExecute() throws Exception {
        JsonNode node = MAPPER.readTree(new Console().request("RETURN 1"));
        assertEquals("execute", node.get("op").asText());
        assertEquals("RETURN 1", node.get("query").asText());
    }

    @Test
    void pingAndAdminCommands() throws Exception {
        Console console = new Console();
        assertEquals("ping", MAPPER.readTree(console.request("ping")).get("op").asText());
        JsonNode stats = MAPPER.readTree(console.request(":stats"));
        assertEquals("stats", stats.get("command").asText());
        assertFalse(stats.path("refresh").asBoolean(false));
        JsonNode refreshed = MAPPER.readTree(console.request(":stats refresh"));
        assertTrue(refreshed.get("refresh").asBoolean());
        JsonNode created = MAPPER.readTree(console.request(":createUser reader secret read"));
        assertEquals("createUser", created.get("command").asText());
        assertEquals("reader", created.get("user").asText());
        assertEquals("read", created.get("role").asText());
    }
}
