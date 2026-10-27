package io.cengyan.query;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DuckDbResolveTest {
    @Test
    void 内存库可以打开() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            assertFalse(connection.isClosed());
        }
    }
}
