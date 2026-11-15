package io.cengyan.protocol;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import static org.junit.jupiter.api.Assertions.*;

class PartitionRouterTest {
    @Test
    void 全长度哈希与独立参考实现一致() {
        var reference = net.jpountz.xxhash.XXHashFactory.safeInstance().hash64();
        var random = new java.util.Random(601);
        for (int size = 0; size < 4096; size++) {
            byte[] data = new byte[size];
            random.nextBytes(data);
            assertEquals(reference.hash(data, 0, size, 0), PartitionRouter.hash(data), "length=" + size);
        }
    }
    @Test
    void 标准哈希向量与规范编码固定() {
        assertEquals("ef46db3751d8e999", Long.toUnsignedString(PartitionRouter.hash(new byte[0]), 16));
        assertEquals("d24ec4f1a98c6e5b", Long.toUnsignedString(PartitionRouter.hash("a".getBytes(StandardCharsets.UTF_8)), 16));
        assertEquals("44bc2cf5ad770999", Long.toUnsignedString(PartitionRouter.hash("abc".getBytes(StandardCharsets.UTF_8)), 16));
        assertEquals("01000000000000007b", HexFormat.of().formatHex(new EntityId.Int64(123).canonicalBytes()));
        assertEquals("02313233", HexFormat.of().formatHex(new EntityId.Text("123").canonicalBytes()));
    }

    @Test
    void 负数和长字符串路由稳定且有界() {
        for (long id = -1000; id < 1000; id++) {
            int part = PartitionRouter.partOf(new EntityId.Int64(id), 97);
            assertTrue(part >= 0 && part < 97);
            assertEquals(part, PartitionRouter.partOf(new EntityId.Int64(id), 97));
        }
        byte[] text = new EntityId.Text("随机分布的节点-ID-".repeat(20)).canonicalBytes();
        assertTrue(PartitionRouter.partOf(text, 97) < 97);
        assertEquals(0, PartitionRouter.partOf(text, 1));
        assertThrows(IllegalArgumentException.class, () -> PartitionRouter.partOf(text, 0));
        assertThrows(IllegalArgumentException.class, () -> new EntityId.Text("\uD800"));
        assertThrows(IllegalArgumentException.class, () -> new EntityId.Text("\uDC00"));
        assertDoesNotThrow(() -> new EntityId.Text("\uD83D\uDE00"));
    }
}
