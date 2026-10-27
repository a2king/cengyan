package io.cengyan.query;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryHandleTest {
    @Test
    void 取消后标志置位且钩子只跑一次() {
        QueryHandle handle = QueryHandle.start(1000);
        AtomicInteger hits = new AtomicInteger();
        handle.onCancel(hits::incrementAndGet);
        assertFalse(handle.cancelled());
        handle.cancel("测试");
        assertTrue(handle.cancelled());
        assertEquals("测试", handle.reason());
        handle.cancel("再次");
        assertEquals(1, hits.get());
        assertEquals("测试", handle.reason());
    }

    @Test
    void 已经取消后再登记钩子会立刻执行() {
        QueryHandle handle = QueryHandle.start(1000);
        handle.cancel("超时");
        AtomicInteger hits = new AtomicInteger();
        handle.onCancel(hits::incrementAndGet);
        assertEquals(1, hits.get());
    }
}
