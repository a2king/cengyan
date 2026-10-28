package io.cengyan.server;

import io.cengyan.query.QueryHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunningQueryTest {
    @Test
    void 只列出写了语句的查询() {
        QueryRegistry registry = new QueryRegistry();
        QueryHandle login = QueryHandle.start(5_000);
        registry.register(login);
        QueryHandle running = QueryHandle.start(5_000);
        running.describe("root", "MATCH (a:Person) RETURN a");
        registry.register(running);
        List<RunningQuery> rows = registry.list();
        assertEquals(1, rows.size());
        assertEquals(running.id(), rows.get(0).queryId());
        assertEquals("root", rows.get(0).user());
        assertEquals("MATCH (a:Person) RETURN a", rows.get(0).statement());
        assertTrue(rows.get(0).elapsedMs() >= 0);
        assertTrue(registry.cancel(running.id(), "被终止"));
        assertTrue(running.cancelled());
        assertEquals("被终止", running.reason());
    }
}
