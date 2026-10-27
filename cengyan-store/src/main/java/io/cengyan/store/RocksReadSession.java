package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class RocksReadSession implements ReadSession {
    private final RocksGraphStore store;
    private final Snapshot snapshot;
    private final AtomicBoolean closed = new AtomicBoolean();

    RocksReadSession(RocksGraphStore store, Snapshot snapshot) {
        this.store = store;
        this.snapshot = snapshot;
    }

    @Override
    public EdgeCursor scanEdges(
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel) {
        ensureOpen();
        return EdgeScanner.open(
                store.db(),
                store.handle(Cf.EDGE),
                snapshot,
                graphId,
                vid,
                edgeTypeId,
                direction,
                queryLimit,
                maxEdgesPerVertex,
                cancel);
    }

    @Override
    public byte[] get(Cf family, byte[] key) {
        ensureOpen();
        try (ReadOptions options = readOptions()) {
            return store.db().get(store.handle(family), options, key);
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "读取失败：" + ex.getMessage(), ex);
        }
    }

    @Override
    public List<byte[]> multiGet(Cf family, List<byte[]> keys) {
        ensureOpen();
        if (keys.isEmpty()) {
            return List.of();
        }
        List<ColumnFamilyHandle> families = new ArrayList<>(keys.size());
        ColumnFamilyHandle handle = store.handle(family);
        for (int i = 0; i < keys.size(); i++) {
            families.add(handle);
        }
        try (ReadOptions options = readOptions()) {
            return store.db().multiGetAsList(options, families, keys);
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "批量读取失败：" + ex.getMessage(), ex);
        }
    }

    private ReadOptions readOptions() {
        ReadOptions options = new ReadOptions();
        options.setSnapshot(snapshot);
        return options;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new CengyanException(ErrorCode.INTERNAL, "读取快照已关闭");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            snapshot.close();
        }
    }
}
