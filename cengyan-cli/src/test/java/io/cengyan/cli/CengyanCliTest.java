package io.cengyan.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CengyanCliTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
    }
}
