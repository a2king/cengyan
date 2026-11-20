package io.cengyan.compute;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CurrentGraphLeaseTest {
    @Test void 目录语句不因会话图申请租约() {
        assertFalse(ComputeServer.requiresCurrentGraph("execute", "SHOW GRAPHS"));
        assertFalse(ComputeServer.requiresCurrentGraph("execute", "  show aliases"));
        assertFalse(ComputeServer.requiresCurrentGraph("execute", "CREATE GRAPH demo PARTS 32"));
        assertFalse(ComputeServer.requiresCurrentGraph("createGraph", ""));
        assertTrue(ComputeServer.requiresCurrentGraph("execute", "SHOW NODE TYPES"));
        assertTrue(ComputeServer.requiresCurrentGraph("execute", "MATCH (n) RETURN n"));
        assertTrue(ComputeServer.requiresCurrentGraph("bulkInsert", "SHOW GRAPHS"));
    }
}
