package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexCodecTest {
    @Test
    void 整数和浮点按数值排序且负零相同() {
        assertBefore(ValueType.INT64, -2L, -1L);
        assertBefore(ValueType.INT64, -1L, 0L);
        assertBefore(ValueType.INT64, 0L, 1L);
        assertBefore(ValueType.DOUBLE, -1.5d, -0.0d);
        assertArrayEquals(IndexCodec.encode(ValueType.DOUBLE, -0.0d), IndexCodec.encode(ValueType.DOUBLE, 0.0d));
        assertBefore(ValueType.DOUBLE, 0.0d, 0.5d);
        assertBefore(ValueType.BOOL, false, true);
        assertBefore(ValueType.DATETIME, 1L, 2L);
    }

    @Test
    void 非有限浮点和坏记录被拒绝() {
        CengyanException nan = assertThrows(CengyanException.class,
                () -> IndexCodec.encode(ValueType.DOUBLE, Double.NaN));
        assertEquals(ErrorCode.TYPE_MISMATCH, nan.code());
        assertTrue(nan.getMessage().contains("无穷"));
        CengyanException infinite = assertThrows(CengyanException.class,
                () -> IndexCodec.encode(ValueType.DOUBLE, Double.POSITIVE_INFINITY));
        assertEquals(ErrorCode.TYPE_MISMATCH, infinite.code());
    }

    @Test
    void 目录记录能来回读写() {
        byte[] raw = IndexCodec.record("by_name", Catalog.NODE_KIND, 3, new int[] {0, 2}, true, false);
        IndexCodec.Decoded decoded = IndexCodec.decode(raw);
        assertEquals("by_name", decoded.name());
        assertEquals(Catalog.NODE_KIND, decoded.kind());
        assertEquals(3, decoded.typeId());
        assertArrayEquals(new int[] {0, 2}, decoded.propertyIndexes());
        assertTrue(decoded.unique());
        assertFalse(decoded.ready());
    }

    private static void assertBefore(ValueType type, Object lower, Object higher) {
        assertTrue(Arrays.compareUnsigned(IndexCodec.encode(type, lower), IndexCodec.encode(type, higher)) < 0);
    }
}
