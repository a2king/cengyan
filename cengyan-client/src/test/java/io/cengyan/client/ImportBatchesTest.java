package io.cengyan.client;

import io.cengyan.raft.ImportSst;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImportBatchesTest {
    @Test void partitionFlushesWhenItReachesItsOwnLimit() {
        ImportBatches batches = new ImportBatches(10, 1_000);
        List<ImportBatches.Ready> ready = batches.add(3, row(4), true);
        ready.addAll(batches.add(3, row(4), true));
        ready.addAll(batches.add(3, row(4), false));
        assertEquals(1, ready.size());
        assertEquals(3, ready.get(0).part);
        assertEquals(2, ready.get(0).primaries);
        assertEquals(3, ready.get(0).rows.size());
        List<ImportBatches.Ready> rest = batches.finish();
        assertEquals(0, rest.size());
    }

    @Test void totalBudgetFlushesTheLargestPartition() {
        ImportBatches batches = new ImportBatches(1_000, 10);
        batches.add(1, row(6), true);
        List<ImportBatches.Ready> ready = batches.add(2, row(6), false);
        assertEquals(1, ready.size());
        assertEquals(1, ready.get(0).part);
        assertEquals(1, ready.get(0).primaries);
        List<ImportBatches.Ready> rest = batches.finish();
        assertEquals(1, rest.size());
        assertEquals(2, rest.get(0).part);
        assertEquals(0, rest.get(0).primaries);
    }

    @Test void seenIdsStayOnDisk() throws Exception {
        RocksDB.loadLibrary();
        var directory = Files.createTempDirectory("cengyan-ids");
        try (ImportIds ids = new ImportIds(directory.resolve("ids"))) {
            assertTrue(ids.add("e1"));
            assertFalse(ids.add("e1"));
            assertTrue(ids.add("e2"));
        }
    }

    private static ImportSst.Row row(int bytes) {
        return new ImportSst.Row(new byte[bytes], new byte[0]);
    }
}
