package io.cengyan.client;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImportFilesTest {
    @Test void quotedTsvKeepsDelimiterInsideQuotes() {
        assertEquals(List.of("a", "b,c", "d"), ImportFiles.split("a,\"b,c\",d", ','));
    }

    @Test void importRequiresExactlyOneKind() {
        assertThrows(IllegalArgumentException.class, () -> CengyanClient.Options.parse(new String[] {"import",
                "--compute-nodes", "127.0.0.1:19669", "--username", "root", "--password", "secret",
                "--graph-space", "Company", "--file-type", "tsv", "--files", "nodes.tsv", "--id", "id"}));
    }

    @Test void batchSizeIsMegabytes() {
        String[] base = {"import", "--compute-nodes", "127.0.0.1:19669", "--username", "root", "--password", "secret",
                "--graph-space", "Company", "--file-type", "tsv", "--files", "nodes.tsv", "--node", "Person", "--id", "id"};
        assertEquals(64L * 1024 * 1024, CengyanClient.Options.parse(base).batchBytes);
        String[] sized = java.util.Arrays.copyOf(base, base.length + 2);
        sized[sized.length - 2] = "--batch-mb";
        sized[sized.length - 1] = "128";
        assertEquals(128L * 1024 * 1024, CengyanClient.Options.parse(sized).batchBytes);
        String[] zero = java.util.Arrays.copyOf(base, base.length + 2);
        zero[zero.length - 2] = "--batch-mb";
        zero[zero.length - 1] = "0";
        assertThrows(IllegalArgumentException.class, () -> CengyanClient.Options.parse(zero));
        assertEquals(3, CengyanClient.Options.parse(base).retries);
        assertEquals(5, CengyanClient.Options.parse(base).retryIntervalSeconds);
        String[] retry = java.util.Arrays.copyOf(base, base.length + 4);
        retry[retry.length - 4] = "--retry";
        retry[retry.length - 3] = "2";
        retry[retry.length - 2] = "--retry-interval";
        retry[retry.length - 1] = "10";
        assertEquals(2, CengyanClient.Options.parse(retry).retries);
        assertEquals(10, CengyanClient.Options.parse(retry).retryIntervalSeconds);
        assertTrue(CengyanClient.retryable("FAILED Storage 响应中断，写入状态可能未知"));
        assertFalse(CengyanClient.retryable("KEY_EXISTS 导入主键已存在"));
        assertFalse(CengyanClient.retryable("INVALID_ARGUMENT 属性未定义"));
    }

    @Test void tsvHeaderBecomesColumnNames() throws Exception {
        var file = Files.createTempFile("nodes", ".tsv");
        Files.writeString(file, "id\tname\np1\tAlice\n");
        List<Map<String, String>> rows = new ArrayList<>();
        ImportFiles.read("tsv", file, '\t', rows::add);
        assertEquals("p1", rows.get(0).get("id"));
        assertEquals("Alice", rows.get(0).get("name"));
    }

    @Test void orcRepeatingAndEmptyStringsStayReadable() {
        BytesColumnVector kind = new BytesColumnVector(4);
        byte[] repeated = "org".getBytes(StandardCharsets.UTF_8);
        kind.setRef(0, repeated, 0, repeated.length);
        kind.isRepeating = true;
        kind.vector[1] = null;
        assertEquals("org", ImportFiles.cell(kind, 1));

        BytesColumnVector empty = new BytesColumnVector(1);
        empty.vector[0] = null;
        empty.length[0] = 0;
        assertEquals("", ImportFiles.cell(empty, 0));

        BytesColumnVector missing = new BytesColumnVector(1);
        missing.noNulls = false;
        missing.isNull[0] = true;
        missing.vector[0] = null;
        assertEquals("", ImportFiles.cell(missing, 0));
    }

    @Test void progressUsesInfoTimestamp() {
        assertEquals("2026-11-12 12:39:01 INFO  CengyanClient - 完成总条数 0 条 | 总跳过 0 条 | 平均导入速度 0 条/s | 最新导入速度 0 条/s | 网络异常重试次数 0 次",
                CengyanClient.progressLine(LocalDateTime.of(2026, 11, 12, 12, 39, 1), 60, 0, 0, 0, 0));
        assertEquals("2026-11-13 14:16:00 INFO  CengyanClient - 完成总条数 160366560 条 | 总跳过 0 条 | 平均导入速度 21908 条/s | 最新导入速度 21908 条/s | 网络异常重试次数 0 次",
                CengyanClient.progressLine(LocalDateTime.of(2026, 11, 13, 14, 16, 0), 7320, 160_366_560, 0, 21_908, 0));
        assertEquals(21_908, CengyanClient.recentPerSecond(160_366_560, 159_052_080, 60_000));
    }

    @Test void reportLinesAlignTheColon() {
        List<String> lines = CengyanClient.reportLines(
                LocalDateTime.of(2026, 11, 12, 11, 45, 23),
                LocalDateTime.of(2026, 11, 12, 11, 45, 24),
                1_000, 0, 0, "8724fe72-44ca-429d-a204-73739ccb5366",
                "Cannot read the array length because \"bytes\" is null");
        int column = -1;
        for (String line : lines) {
            if (line.isEmpty()) continue;
            int colon = line.indexOf(':');
            int width = CengyanClient.displayWidth(line.substring(0, colon));
            if (column < 0) column = width;
            assertEquals(column, width);
        }
        assertEquals("起始时间         : 2026-11-12 11:45:23", lines.get(0));
        assertEquals("总耗时           : 1 s", lines.get(2));
        assertEquals("完成条数         : 0 条", lines.get(3));
        assertEquals("平均速度         : 0 条/s", lines.get(5));
        assertEquals("唯一索引构建时间 : 0 s", lines.get(6));
        assertEquals("", lines.get(9));
        assertEquals("导数发生错误     : Cannot read the array length because \"bytes\" is null", lines.get(10));
    }
}
