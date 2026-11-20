package io.cengyan.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTempsTest {
    @TempDir Path tmp;

    private Path attempt(String id, int bytes, long modifiedMillis) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("snapshot-" + id).resolve("snap-1_10"));
        Path file = Files.write(dir.resolve("000001.sst"), new byte[bytes]);
        for (Path p : new Path[] {file, dir, dir.getParent()}) Files.setLastModifiedTime(p, FileTime.fromMillis(modifiedMillis));
        return dir.getParent();
    }

    @Test void 只保留最近有写入的一次尝试() throws Exception {
        long now = 10_000_000;
        Path old1 = attempt("a", 100, now - 300_000);
        Path old2 = attempt("b", 200, now - 120_000);
        Path current = attempt("c", 50, now - 5_000);
        Path unrelated = Files.createDirectories(tmp.resolve("other"));

        var purged = SnapshotTemps.purge(tmp, 600_000, now);

        assertEquals(2, purged.directories());
        assertEquals(300, purged.bytes());
        assertFalse(Files.exists(old1));
        assertFalse(Files.exists(old2));
        assertTrue(Files.exists(current), "正在写的那次不能删");
        assertTrue(Files.exists(unrelated), "不是 snapshot- 开头的目录不动");
        assertEquals(50, SnapshotTemps.receiving(tmp));
    }

    @Test void 最近一次也空闲太久时一起删() throws Exception {
        long now = 10_000_000;
        attempt("a", 100, now - 700_000);
        var purged = SnapshotTemps.purge(tmp, 600_000, now);
        assertEquals(1, purged.directories());
        assertEquals(-1, SnapshotTemps.receiving(tmp));
    }

    @Test void 启动时全部删除() throws Exception {
        long now = 10_000_000;
        attempt("a", 10, now);
        attempt("b", 10, now - 1);
        assertEquals(2, SnapshotTemps.purge(tmp, 0, now).directories());
    }

    @Test void 没有临时目录时什么都不做() throws Exception {
        assertEquals(0, SnapshotTemps.purge(tmp.resolve("missing"), 600_000, 0).directories());
        assertEquals(-1, SnapshotTemps.receiving(tmp.resolve("missing")));
    }
}
