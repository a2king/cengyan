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
import java.util.function.Consumer;
import java.util.function.Predicate;

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
                cancel,
                null,
                store::addEdgeExamined);
    }

    @Override
    public EdgeCursor scanEdges(
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            Predicate<EdgeRef> predicate,
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
                cancel,
                predicate,
                store::addEdgeExamined);
    }

    @Override
    public List<VertexId> scanVertices(
            int graphId, int tagId, int queryLimit, int maxExamined, CancelToken cancel) {
        ensureOpen();
        if (queryLimit == 0) {
            return List.of();
        }
        byte[] prefix = KeyCodec.graphIdPrefix(graphId);
        byte[] upper = KeyCodec.upperBound(prefix);
        List<VertexId> found = new ArrayList<>();
        ReadOptions options = new ReadOptions();
        Slice upperSlice = null;
        RocksIterator iterator = null;
        int examined = 0;
        try {
            options.setSnapshot(snapshot);
            options.setTotalOrderSeek(true);
            options.setPrefixSameAsStart(false);
            if (upper != null) {
                upperSlice = new Slice(upper);
                options.setIterateUpperBound(upperSlice);
            }
            iterator = store.db().newIterator(store.handle(Cf.VERTEX), options);
            iterator.seek(prefix);
            while (iterator.isValid()) {
                if ((examined & 63) == 0 && cancel != null && cancel.cancelled()) {
                    throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
                }
                byte[] raw = iterator.key();
                byte[] key = Arrays.copyOf(raw, raw.length);
                if (!KeyCodec.startsWith(key, prefix)) {
                    break;
                }
                iterator.next();
                examined++;
                store.addVerticesExamined();
                if (maxExamined >= 0 && examined > maxExamined) {
                    throw new CengyanException(ErrorCode.TRUNCATED,
                            "扫描起点已检查 " + examined + " 个点，超过上限 " + maxExamined);
                }
                if (key.length == KeyCodec.VERTEX_KEY_LENGTH && KeyCodec.tagIdOf(key) == tagId) {
                    found.add(new VertexId(KeyCodec.vidOf(key), tagId));
                    if (queryLimit >= 0 && found.size() == queryLimit) {
                        break;
                    }
                }
            }
            iterator.status();
            return found;
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "扫描点失败：" + ex.getMessage(), ex);
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

    @Override
    public void visitPrefix(Cf family, byte[] prefix, CancelToken cancel, Consumer<byte[]> consumer) {
        ensureOpen();
        if (prefix == null || prefix.length == 0) {
            throw new IllegalArgumentException("前缀不能为空");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("访问动作不能为空");
        }
        byte[] upper = KeyCodec.upperBound(prefix);
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
            IndexScan.visit(iterator, prefix, cancel, consumer);
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

    @Override
    public List<byte[]> scanIndex(byte[] startInclusive, byte[] endExclusive, int limit, CancelToken cancel) {
        ensureOpen();
        if (startInclusive == null || startInclusive.length < 4) {
            throw new IllegalArgumentException("索引扫描需要起点");
        }
        if (limit == 0) {
            return List.of();
        }
        byte[] indexPrefix = Arrays.copyOf(startInclusive, 4);
        ReadOptions options = new ReadOptions();
        Slice upperSlice = null;
        RocksIterator iterator = null;
        try {
            options.setSnapshot(snapshot);
            options.setTotalOrderSeek(true);
            options.setPrefixSameAsStart(false);
            if (endExclusive != null) {
                upperSlice = new Slice(endExclusive);
                options.setIterateUpperBound(upperSlice);
            }
            iterator = store.db().newIterator(store.handle(Cf.INDEX), options);
            return IndexScan.collect(
                    iterator, indexPrefix, startInclusive, endExclusive, limit, cancel, store::addIndexReads);
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
