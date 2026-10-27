package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import java.util.ArrayList;
import java.util.Arrays;
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
        if (family == Cf.PROP) {
            store.addPropReads(1);
        }
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
        if (family == Cf.PROP) {
            store.addPropReads(keys.size());
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

    @Override
    public List<byte[]> keysWithPrefix(Cf family, byte[] prefix) {
        ensureOpen();
        if (prefix == null || prefix.length == 0) {
            throw new IllegalArgumentException("前缀不能为空");
        }
        byte[] upper = KeyCodec.upperBound(prefix);
        List<byte[]> found = new ArrayList<>();
        ReadOptions options = new ReadOptions();
        Slice upperSlice = null;
        RocksIterator iterator = null;
        try {
            options.setSnapshot(snapshot);
            options.setTotalOrderSeek(true);
            options.setPrefixSameAsStart(false);
            if (upper != null) {
                upperSlice = new Slice(upper);
                options.setIterateUpperBound(upperSlice);
            }
            iterator = store.db().newIterator(store.handle(family), options);
            iterator.seek(prefix);
            while (iterator.isValid()) {
                byte[] raw = iterator.key();
                byte[] key = Arrays.copyOf(raw, raw.length);
                if (!KeyCodec.startsWith(key, prefix)) {
                    break;
                }
                found.add(key);
                iterator.next();
            }
            iterator.status();
            return found;
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "前缀扫描失败：" + ex.getMessage(), ex);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
            if (upperSlice != null) {
                upperSlice.close();
            }
            options.close();
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
