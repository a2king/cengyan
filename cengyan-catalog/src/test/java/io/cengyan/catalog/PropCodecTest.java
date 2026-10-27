package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropCodecTest {
    @Test
    void 旧行把末尾新增的可空列读成空() {
        List<PropDef> original = List.of(
                new PropDef("capital", ValueType.INT64, false),
                new PropDef("name", ValueType.STRING, true));
        byte[] raw = PropCodec.encode(1, original, Map.of("capital", 8L));
        List<PropDef> current = List.of(
                new PropDef("capital", ValueType.INT64, false),
                new PropDef("name", ValueType.STRING, true),
                new PropDef("note", ValueType.STRING, true));
        Map<String, Object> values = PropCodec.decode(current, 2, raw);
        assertEquals(8L, values.get("capital"));
        assertNull(values.get("name"));
        assertNull(values.get("note"));
    }

    @Test
    void 版本对不上就失败() {
        List<PropDef> properties = List.of(new PropDef("capital", ValueType.INT64, false));
        byte[] newerRaw = PropCodec.encode(2, properties, Map.of("capital", 1L));
        CengyanException newer = assertThrows(CengyanException.class,
                () -> PropCodec.decode(properties, 1, newerRaw));
        assertEquals(ErrorCode.SCHEMA_MISMATCH, newer.code());

        byte[] raw = PropCodec.encode(1, properties, Map.of("capital", 1L));
        List<PropDef> jumped = List.of(
                new PropDef("capital", ValueType.INT64, false),
                new PropDef("note", ValueType.STRING, true));
        CengyanException gap = assertThrows(CengyanException.class,
                () -> PropCodec.decode(jumped, 3, raw));
        assertEquals(ErrorCode.SCHEMA_MISMATCH, gap.code());
    }

    @Test
    void 非法浮点和整数不能当成浮点() {
        List<PropDef> properties = List.of(new PropDef("ratio", ValueType.DOUBLE, false));
        CengyanException nan = assertThrows(CengyanException.class,
                () -> PropCodec.encode(1, properties, Map.of("ratio", Double.NaN)));
        assertEquals(ErrorCode.TYPE_MISMATCH, nan.code());
        CengyanException integer = assertThrows(CengyanException.class,
                () -> PropCodec.encode(1, properties, Map.of("ratio", 1)));
        assertEquals(ErrorCode.TYPE_MISMATCH, integer.code());
        CengyanException unknown = assertThrows(CengyanException.class,
                () -> PropCodec.encode(1, properties, Map.of("ratio", 0.5d, "extra", 1L)));
        assertEquals(ErrorCode.TYPE_MISMATCH, unknown.code());
    }

    @Test
    void 日期时间和布尔能往返() {
        long millis = 1_704_067_200_000L;
        List<PropDef> properties = List.of(
                new PropDef("open", ValueType.BOOL, false),
                new PropDef("at", ValueType.DATETIME, false));
        byte[] raw = PropCodec.encode(1, properties, Map.of("open", false, "at", millis));
        Map<String, Object> values = PropCodec.decode(properties, 1, raw);
        assertEquals(false, values.get("open"));
        assertEquals(millis, values.get("at"));
    }

    @Test
    void 字符串超过六十四千字节就失败() {
        List<PropDef> properties = List.of(new PropDef("note", ValueType.STRING, false));
        String tooLong = "a".repeat(PropCodec.MAX_STRING_BYTES + 1);
        assertEquals(PropCodec.MAX_STRING_BYTES + 1, tooLong.getBytes(StandardCharsets.UTF_8).length);
        CengyanException ex = assertThrows(CengyanException.class,
                () -> PropCodec.encode(1, properties, Map.of("note", tooLong)));
        assertEquals(ErrorCode.TYPE_MISMATCH, ex.code());
    }
}
