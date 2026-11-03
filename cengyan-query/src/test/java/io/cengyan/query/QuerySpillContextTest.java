package io.cengyan.query;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuerySpillContextTest {
    @Test
    void 超级节点spill后visited仍保持去重和遍历顺序() throws Exception {
        Path temp = Files.createTempDirectory("cengyan-supernode-spill-");
        QueryLimits limits = new QueryLimits(
                10_000,
                10_000,
                3,
                10_000,
                64L * 1024 * 1024,
                true,
                2,
                true,
                64L * 1024 * 1024,
                2);
        DuckSettings settings = new DuckSettings("64MB", temp, "64MB", 1);
        try (QuerySpillContext.Scope ignored = QuerySpillContext.open(limits, settings)) {
            QuerySpillContext context = QuerySpillContext.current();
            QuerySpillContext.SpillSet set = context.newSet();
            assertTrue(set.add(1));
            assertTrue(!set.add(1));
            assertTrue(set.add(2));
            context.activate(set);
            assertEquals(2, set.size());
            List<Long> values = new ArrayList<>();
            set.forEach(values::add);
            assertEquals(List.of(1L, 2L), values);
        } finally {
            try (var walk = Files.walk(temp)) {
                for (Path path : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }
}
