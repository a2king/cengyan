package com.cengyan.core;

import com.cengyan.core.exception.ResultException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultTest {
    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i + 1 < pairs.length; i += 2) map.put((String) pairs[i], pairs[i + 1]);
        return map;
    }

    static Result result(List<String> columns, List<?>... rows) {
        return new Result(map("code", "OK", "columns", columns, "rows", Arrays.asList(rows), "rowsAffected", 2L, "queryId", "q-1"));
    }

    static Map<String, Object> node(Object id, String type, Map<String, Object> properties) {
        return map("id", id, "type", type, "properties", properties);
    }

    @Test
    void rowsScalarFirstOneAndMetadata() {
        Result result = result(Arrays.asList("id", "name"), Arrays.asList(1L, "a"), Arrays.asList(2L, "b"));
        assertEquals(2, result.size());
        assertEquals(2L, result.rowsAffected());
        assertEquals("q-1", result.queryId());
        assertEquals(1L, result.scalar());
        assertEquals(map("id", 1L, "name", "a"), result.first().asMap());
        assertThrows(ResultException.class, result::one);
        int count = 0;
        for (Record record : result) count += record.getInt("id");
        assertEquals(3, count);
        assertEquals(2, result.stream().count());

        Result empty = new Result(map("code", "OK"));
        assertTrue(empty.isEmpty());
        assertNull(empty.first());
        assertNull(empty.scalar());
        assertEquals(0L, empty.rowsAffected());
        assertEquals("RESULT_SHAPE", assertThrows(ResultException.class, empty::one).code());
    }

    @Test
    void typedGettersAreStrict() {
        Record record = result(Arrays.asList("i", "big", "d", "s", "b", "t", "n", "l", "m"),
                Arrays.asList(7L, 1L << 40, 1.5, "x", true, "2026-11-10T08:30:00.123Z", null, Arrays.asList(1L), map("k", 1L))).one();
        assertEquals(7, (int) record.getInt("i"));
        assertEquals(1L << 40, (long) record.getLong("big"));
        assertEquals("RESULT_TYPE", assertThrows(ResultException.class, () -> record.getInt("big")).code());
        assertEquals(7.0, record.getDouble("i"));
        assertEquals(1.5, record.getDouble("d"));
        assertThrows(ResultException.class, () -> record.getLong("d"));
        assertThrows(ResultException.class, () -> record.getString("i"));
        assertEquals(Boolean.TRUE, record.getBoolean("b"));
        assertEquals(Instant.parse("2026-11-10T08:30:00.123Z"), record.getInstant("t"));
        assertThrows(ResultException.class, () -> record.getInstant("s"));
        assertNull(record.getLong("n"));
        assertNull(record.getString("n"));
        assertEquals(Arrays.asList(1L), record.getList("l"));
        assertEquals(map("k", 1L), record.getMap("m"));
        assertEquals("RESULT_COLUMN", assertThrows(ResultException.class, () -> record.get("missing")).code());
        assertThrows(ResultException.class, () -> record.get(99));
    }

    @Test
    void shortRowsYieldNullForMissingColumns() {
        Record record = result(Arrays.asList("a", "b"), Collections.singletonList(1L)).one();
        assertNull(record.get("b"));
        assertEquals(map("a", 1L, "b", null), record.asMap());
    }

    @Test
    void serverDatetimeFormatsAreAccepted() {
        Record record = result(Arrays.asList("a", "b", "c"), Arrays.asList("2024-01-31 12:00:00", "2024-01-31T12:00:00", "2024-01-31T20:00:00+08:00")).one();
        Instant expected = Instant.parse("2024-01-31T12:00:00Z");
        assertEquals(expected, record.getInstant("a"));
        assertEquals(expected, record.getInstant("b"));
        assertEquals(expected, record.getInstant("c"));
    }

    @Test
    void nodesEdgesAndPaths() {
        Map<String, Object> a = node(1L, "Person", map("name", "A"));
        Map<String, Object> b = node(2L, "Person", map("name", "B"));
        Map<String, Object> e = map("id", 10L, "type", "knows", "properties", map("since", 2020L), "src", 1L, "dst", 2L);
        Map<String, Object> p = map("nodes", Arrays.asList(a, b), "edges", Arrays.asList(e), "length", 1L);
        Record record = result(Arrays.asList("n", "e", "p"), Arrays.asList(a, e, p)).one();
        GraphNode n = record.getNode("n");
        assertEquals(1L, n.id());
        assertEquals("Person", n.type());
        assertEquals("A", n.property("name"));
        GraphEdge edge = record.getEdge("e");
        assertEquals(1L, edge.src());
        assertEquals(2L, edge.dst());
        GraphPath path = record.getPath("p");
        assertEquals(1, path.length());
        assertEquals(n, path.start());
        assertEquals("B", path.end().property("name"));
        assertThrows(ResultException.class, () -> record.getNode("e"));
        assertThrows(ResultException.class, () -> record.getEdge("n"));
        assertThrows(ResultException.class, () -> record.getPath("n"));
    }

    @Test
    void malformedShapesAreRejected() {
        assertThrows(ResultException.class, () -> new Result(map("columns", "x")));
        assertThrows(ResultException.class, () -> new Result(map("columns", Arrays.asList(1L))));
        assertThrows(ResultException.class, () -> new Result(map("columns", Arrays.asList("a"), "rows", Arrays.asList("x"))));
    }

    @Test
    void exportsWriteJsonAndCsv(@TempDir Path directory) throws Exception {
        Result result = result(Arrays.asList("n", "name", "big"),
                Arrays.asList(node(1L, "P", map("name", "张三")), "a,\"b\"", new BigInteger("123456789012345678901234567890")),
                Arrays.asList(null, "x", 1L));
        assertEquals("[{\"n\":{\"id\":1,\"type\":\"P\",\"properties\":{\"name\":\"张三\"}},\"name\":\"a,\\\"b\\\"\",\"big\":123456789012345678901234567890},"
                + "{\"n\":null,\"name\":\"x\",\"big\":1}]", result.toJson());
        StringWriter lines = new StringWriter();
        result.writeJsonLines(lines);
        assertEquals(2, lines.toString().split("\n").length);
        StringWriter csv = new StringWriter();
        result.writeCsv(csv);
        assertEquals("n,name,big\r\n\"{\"\"id\"\":1,\"\"type\"\":\"\"P\"\",\"\"properties\"\":{\"\"name\"\":\"\"张三\"\"}}\",\"a,\"\"b\"\"\","
                + "123456789012345678901234567890\r\n,x,1\r\n", csv.toString());

        Path file = directory.resolve("out.csv");
        result.toCsv(file);
        byte[] bytes = Files.readAllBytes(file);
        assertArrayEquals(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, Arrays.copyOf(bytes, 3));
        assertTrue(new String(bytes, StandardCharsets.UTF_8).contains("张三"));
    }
}
