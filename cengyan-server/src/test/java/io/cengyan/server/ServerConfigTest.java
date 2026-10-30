package io.cengyan.server;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerConfigTest {
    @Test
    void 监听地址会去掉引号() throws Exception {
        Path file = Files.createTempFile("cengyan-host-", ".properties");
        try {
            Files.writeString(file, "host='0.0.0.0'\nport=19669\n");
            ServerConfig config = ServerConfig.load(file);
            assertEquals("0.0.0.0", config.host());
            assertEquals(19669, config.port());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void 不写host时监听所有网卡() {
        assertEquals("0.0.0.0", ServerConfig.defaults().host());
    }
    @Test
    void rocksDbTuningDefaults() {
        ServerConfig config = ServerConfig.defaults();
        assertEquals(6, config.rocksdbBackgroundJobs());
        assertEquals(134217728L, config.rocksdbWriteBufferBytes());
    }
}
