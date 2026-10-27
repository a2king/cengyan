package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthTest {
    @Test
    void 密码不以明文保存并且至少留一个读写用户() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-auth-");
        GraphStore store = GraphStore.open(dir);
        try {
            Auth auth = new Auth(store);
            assertTrue(auth.ensureRoot("cengyan"));
            assertFalse(auth.ensureRoot("other"));
            try (ReadSession session = store.read()) {
                byte[] raw = session.get(Cf.CATALOG, KeyCodec.userKey("root"));
                assertEquals(49, raw.length);
                assertEquals(Auth.ROLE_WRITE, raw[48] & 0xFF);
                assertFalse(new String(raw, StandardCharsets.UTF_8).contains("cengyan"));
            }
            Auth.User root = auth.authenticate("root", "cengyan");
            assertEquals("root", root.name());
            assertEquals(Auth.ROLE_WRITE, root.role());
            assertNull(auth.authenticate("root", "wrong"));
            assertNull(auth.authenticate("missing", "cengyan"));
            auth.createUser("reader", "secret", Auth.ROLE_READ);
            CengyanException duplicate = assertThrows(CengyanException.class,
                    () -> auth.createUser("root", "again", Auth.ROLE_WRITE));
            assertEquals(ErrorCode.NAME_CONFLICT, duplicate.code());
            CengyanException empty = assertThrows(CengyanException.class,
                    () -> auth.createUser("alice", "", Auth.ROLE_READ));
            assertEquals(ErrorCode.SYNTAX, empty.code());
            assertTrue(empty.getMessage().contains("密码不能为空"));
            CengyanException drop = assertThrows(CengyanException.class, () -> auth.dropUser("root"));
            assertEquals(ErrorCode.FORBIDDEN, drop.code());
            assertTrue(drop.getMessage().contains("读写"));
            CengyanException demote = assertThrows(CengyanException.class,
                    () -> auth.setRole("root", Auth.ROLE_READ));
            assertEquals(ErrorCode.FORBIDDEN, demote.code());
            auth.setPassword("reader", "next");
            assertNull(auth.authenticate("reader", "secret"));
            assertEquals(Auth.ROLE_READ, auth.authenticate("reader", "next").role());
            auth.dropUser("reader");
            store.close();
            store = GraphStore.open(dir);
            Auth reopened = new Auth(store);
            assertEquals(Auth.ROLE_WRITE, reopened.authenticate("root", "cengyan").role());
            assertFalse(reopened.ensureRoot("other"));
            assertNull(reopened.authenticate("reader", "next"));
        } finally {
            store.close();
            deleteTree(dir);
        }
    }

    private static void deleteTree(Path root) throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                if (!Files.exists(root)) {
                    return;
                }
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(path);
                    }
                }
                return;
            } catch (Exception ex) {
                last = ex;
                System.gc();
                Thread.sleep(50L * (attempt + 1));
            }
        }
        if (last != null) {
            throw last;
        }
    }
}
