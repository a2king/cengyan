package com.cengyan.core.internal;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTest {
    private static String text(Object value) {
        return new String(Json.encode(value), StandardCharsets.UTF_8);
    }

    @Test
    void compactEncodingMatchesServerStyle() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("a", 1);
        map.put("b", Arrays.asList(1L, "张三", true, null));
        assertEquals("{\"a\":1,\"b\":[1,\"张三\",true,null]}", text(map));
        assertEquals(Json.encode(map).length, Json.encodedSize(map));
    }

    @Test
    void nanInfinityAndUnsupportedTypesAreRejected() {
        for (Object bad : new Object[] {Double.NaN, Double.POSITIVE_INFINITY, Float.NaN, new Object(), new byte[] {1},
                BigInteger.ONE.shiftLeft(63), "\uD800", singleton(1, "x")}) {
            assertThrows(IllegalArgumentException.class, () -> Json.encode(Arrays.asList(bad)), String.valueOf(bad));
        }
    }

    @Test
    void timeValuesUseUtcMillisecondIso() {
        Instant instant = Instant.parse("2026-11-10T08:30:00.123456Z");
        assertEquals("\"2026-11-10T08:30:00.123Z\"", text(instant));
        assertEquals("\"2026-11-10T00:30:00Z\"", text(OffsetDateTime.of(2026, 11, 10, 8, 30, 0, 0, ZoneOffset.ofHours(8))));
        assertEquals("\"2026-11-10T08:30:00Z\"", text(LocalDateTime.of(2026, 11, 10, 8, 30)));
        assertEquals("\"2026-11-10\"", text(LocalDate.of(2026, 11, 10)));
        assertEquals("\"1970-01-01T00:00:01Z\"", text(new Date(1000)));
        assertEquals("\"1970-01-01T00:00:01Z\"", text(new java.sql.Date(1000)));
    }

    @Test
    void decimalsUuidEnumsAndBigIntegersInRange() {
        UUID id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        assertEquals("[\"1.25\",\"" + id + "\",\"SECONDS\",9223372036854775807,[1,2]]",
                text(Arrays.asList(new BigDecimal("1.25"), id, java.util.concurrent.TimeUnit.SECONDS,
                        BigInteger.valueOf(Long.MAX_VALUE), new int[] {1, 2})));
    }

    @Test
    @SuppressWarnings("unchecked")
    void decodeProducesLongDoubleMapList() {
        Map<String, Object> value = (Map<String, Object>) Json.decode(
                "{\"i\":1,\"f\":1.5,\"big\":123456789012345678901234567890,\"o\":{\"k\":[true,null]}}".getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, value.get("i"));
        assertEquals(1.5, value.get("f"));
        assertTrue(value.get("big") instanceof BigInteger);
        assertEquals(Arrays.asList(true, null), ((Map<String, Object>) value.get("o")).get("k"));
        assertTrue(value instanceof LinkedHashMap);
        assertTrue(((Map<String, Object>) value.get("o")).get("k") instanceof List);
    }

    @Test
    void malformedJsonIsRejected() {
        for (String bad : new String[] {"", "{", "{\"a\":1} x", "nul"}) {
            assertThrows(IllegalArgumentException.class, () -> Json.decode(bad.getBytes(StandardCharsets.UTF_8)), bad);
        }
    }

    private static Map<Object, Object> singleton(Object key, Object value) {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(key, value);
        return map;
    }
}
