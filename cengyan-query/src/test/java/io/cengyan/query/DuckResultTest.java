package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Stmt;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckResultTest {
    @Test
    void 内存放不下时写到临时目录() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-spill-");
        List<List<Object>> rows = new ArrayList<>();
        List<List<Object>> keys = new ArrayList<>();
        String chunk = "x".repeat(8 * 1024);
        for (int i = 0; i < 3000; i++) {
            rows.add(List.of(chunk + i));
            keys.add(List.of(chunk + String.format("%05d", 3000 - i)));
        }
        AtomicLong largest = new AtomicLong();
        DuckSettings settings = new DuckSettings("80MB", dir, path -> largest.set(largest(path)));
        List<List<Object>> ordered = DuckResult.arrange(
                rows, keys, List.of(new Stmt.Sort(new Expr.Literal(1), false)), false, null, null, settings);
        assertEquals(3000, ordered.size());
        assertEquals(chunk + "2999", ordered.get(0).get(0));
        assertEquals(chunk + "0", ordered.get(ordered.size() - 1).get(0));
        assertTrue(largest.get() > 8L * 1024 * 1024, "临时文件只有 " + largest.get() + " 字节");
    }

    private static long largest(Path dir) {
        long size = 0L;
        if (!Files.exists(dir)) {
            return 0L;
        }
        try (var walk = Files.walk(dir)) {
            for (Path file : walk.filter(Files::isRegularFile).toList()) {
                size = Math.max(size, Files.size(file));
            }
        } catch (java.io.IOException ex) {
            return size;
        }
        return size;
    }

    @Test
    void 已经取消时不再交给DuckDB() {
        QueryHandle handle = QueryHandle.start(1_000);
        handle.cancel("测试");
        List<List<Object>> rows = List.of(List.of(1L), List.of(2L));
        List<List<Object>> keys = List.of(List.of(2L), List.of(1L));
        CengyanException canceled = assertThrows(CengyanException.class, () -> DuckResult.arrange(
                rows,
                keys,
                List.of(new Stmt.Sort(new Expr.Literal(1L), false)),
                false,
                null,
                null,
                new DuckSettings("1GB", Path.of(System.getProperty("java.io.tmpdir"), "cengyan-duck")),
                handle));
        assertEquals(ErrorCode.QUERY_CANCELED, canceled.code());
    }
}
