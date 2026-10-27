package io.cengyan.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyCodecTest {
    @Test
    void 边键往返且保留字节为零() {
        byte[] key = KeyCodec.edgeKey(7, 0, -3L, 9, Direction.OUT.code(), -11L, 42L);
        assertEquals(KeyCodec.EDGE_KEY_LENGTH, key.length);
        for (int i = 33; i < key.length; i++) {
            assertEquals(0, key[i]);
        }
        KeyCodec.DecodedEdge decoded = KeyCodec.decodeEdge(key);
        assertEquals(7, decoded.graphId());
        assertEquals(0, decoded.partId());
        assertEquals(-3L, decoded.src());
        assertEquals(9, decoded.edgeTypeId());
        assertEquals(0, decoded.direction());
        assertEquals(-11L, decoded.rank());
        assertEquals(42L, decoded.dst());
        EdgeRef logical = KeyCodec.toLogical(key);
        assertEquals(new EdgeRef(9, -11L, -3L, 42L), logical);
    }

    @Test
    void 入边逻辑端点换回出边方向() {
        byte[] key = KeyCodec.edgeKey(1, 0, 8L, 2, Direction.IN.code(), 4L, 3L);
        assertEquals(new EdgeRef(2, 4L, 3L, 8L), KeyCodec.toLogical(key));
    }

    @Test
    void 出边前缀的上界就是入边前缀() {
        byte[] outPrefix = KeyCodec.edgePrefix(1, 0, 7L, 3, Direction.OUT.code());
        byte[] inPrefix = KeyCodec.edgePrefix(1, 0, 7L, 3, Direction.IN.code());
        assertEquals(KeyCodec.EDGE_PREFIX_LENGTH, outPrefix.length);
        assertArrayEquals(inPrefix, KeyCodec.upperBound(outPrefix));
        byte[] inKey = KeyCodec.edgeKey(1, 0, 7L, 3, Direction.IN.code(), 9L, 8L);
        assertFalse(KeyCodec.startsWith(inKey, outPrefix));
        assertTrue(KeyCodec.compare(inKey, KeyCodec.upperBound(outPrefix)) >= 0);
    }

    @Test
    void 全FF前缀没有上界() {
        assertNull(KeyCodec.upperBound(new byte[] {(byte) 0xFF, (byte) 0xFF}));
    }

    @Test
    void 属性键和点键长度固定() {
        byte[] prop = KeyCodec.propKey(4, 6, 8L, 1L, 2L);
        assertEquals(KeyCodec.PROP_KEY_LENGTH, prop.length);
        assertEquals(KeyCodec.PROP_MARK, prop[0]);
        byte[] vertex = KeyCodec.vertexKey(4, 0, 99L, 1);
        assertEquals(KeyCodec.VERTEX_KEY_LENGTH, vertex.length);
        assertEquals(KeyCodec.VERTEX_PREFIX_LENGTH, KeyCodec.vertexPrefix(4, 0, 99L).length);
    }
}
