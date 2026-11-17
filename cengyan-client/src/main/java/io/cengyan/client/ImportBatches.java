package io.cengyan.client;

import io.cengyan.raft.ImportSst;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按分片攒一批行。单个分片攒到 partBytes，或全部分片合计超过 totalBytes 时交出一批，调用方上传后即可释放。
 * 批次大小只影响上传次数：副本在发布前会把一个分片的全部上传段归并成大文件，小批次不会进入分片库。
 */
final class ImportBatches {
    final long partBytes;
    final long totalBytes;
    private final Map<Integer, Pending> parts = new LinkedHashMap<>();
    private long buffered;

    ImportBatches(long partBytes, long totalBytes) {
        this.partBytes = partBytes;
        this.totalBytes = totalBytes;
    }

    List<Ready> add(int part, ImportSst.Row row, boolean primary) {
        Pending pending = parts.computeIfAbsent(part, Pending::new);
        long bytes = row.logicalKey().length + row.entity().length;
        pending.rows.add(row);
        pending.bytes += bytes;
        if (primary) pending.primaries++;
        buffered += bytes;
        List<Ready> ready = new ArrayList<>();
        if (pending.bytes >= partBytes) ready.add(detach(pending));
        while (buffered > totalBytes) {
            Pending largest = largest();
            if (largest == null) break;
            ready.add(detach(largest));
        }
        return ready;
    }

    List<Ready> finish() {
        List<Ready> ready = new ArrayList<>();
        for (Pending pending : new ArrayList<>(parts.values())) {
            if (!pending.rows.isEmpty()) ready.add(detach(pending));
        }
        return ready;
    }

    private Ready detach(Pending pending) {
        parts.remove(pending.part);
        buffered -= pending.bytes;
        return new Ready(pending.part, pending.rows, pending.primaries);
    }

    private Pending largest() {
        Pending largest = null;
        for (Pending pending : parts.values()) {
            if (largest == null || pending.bytes > largest.bytes) largest = pending;
        }
        return largest;
    }

    private static final class Pending {
        final int part;
        final List<ImportSst.Row> rows = new ArrayList<>();
        int primaries;
        long bytes;

        Pending(int part) { this.part = part; }
    }

    static final class Ready {
        final int part;
        final List<ImportSst.Row> rows;
        final int primaries;

        Ready(int part, List<ImportSst.Row> rows, int primaries) {
            this.part = part;
            this.rows = rows;
            this.primaries = primaries;
        }
    }
}
