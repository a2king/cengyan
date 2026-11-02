package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryMemoryTest {
    @Test
    void 中间结果超过预算返回内存超限() {
        try (QueryMemory.Scope ignored = QueryMemory.open(128)) {
            CengyanException error = assertThrows(CengyanException.class,
                    () -> QueryMemory.current().observeRows(List.of(
                            List.of("a very long value", "another value"),
                            List.of("more data", "and more data"))));
            assertEquals(ErrorCode.QUERY_MEMORY_EXCEEDED, error.code());
        }
    }

    @Test
    void 关闭作用域后恢复为不限预算() {
        try (QueryMemory.Scope ignored = QueryMemory.open(128)) {
            assertEquals(128, QueryMemory.current().limit());
        }
        assertEquals(0, QueryMemory.current().limit());
    }
}
