package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * 按 17 字节前缀向后扫。每 64 条看一次取消；超过每点上限抛 {@code TRUNCATED}，而不是少返回一些。
 */
final class EdgeScanner implements EdgeCursor {
    private final CancelToken cancel;
    private final Runnable closeHook = this::closeForCancel;
    private final byte[] prefix;
    private final byte[] upper;
    private final int queryLimit;
    private final int maxEdgesPerVertex;
    private final long vid;
    private final Predicate<EdgeRef> predicate;
    private final Runnable onExamined;
    private ReadOptions readOptions;
    private Slice upperSlice;
    private RocksIterator iterator;
    private EdgeRef pending;
    private boolean pendingReady;
    private boolean stopAfterPending;
    private boolean finished;
    private boolean closedByCancel;
    private int examined;
    private int matched;

    private EdgeScanner(
            CancelToken cancel,
            byte[] prefix,
            byte[] upper,
            int queryLimit,
            int maxEdgesPerVertex,
            long vid,
            Predicate<EdgeRef> predicate,
            Runnable onExamined,
            ReadOptions readOptions,
            Slice upperSlice,
            RocksIterator iterator) {
        this.cancel = cancel;
        this.prefix = prefix;
        this.upper = upper;
        this.queryLimit = queryLimit;
        this.maxEdgesPerVertex = maxEdgesPerVertex;
        this.vid = vid;
        this.predicate = predicate;
        this.onExamined = onExamined;
        this.readOptions = readOptions;
        this.upperSlice = upperSlice;
        this.iterator = iterator;
    }

    static EdgeScanner open(
            RocksDB db,
            ColumnFamilyHandle edgeFamily,
            Snapshot snapshot,
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel) {
        return open(db, edgeFamily, snapshot, graphId, vid, edgeTypeId, direction,
                queryLimit, maxEdgesPerVertex, cancel, null, null);
    }

    static EdgeScanner open(
            RocksDB db,
            ColumnFamilyHandle edgeFamily,
            Snapshot snapshot,
            int graphId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel,
            Predicate<EdgeRef> predicate,
            Runnable onExamined) {
        if (direction == null) {
            throw new IllegalArgumentException("方向不能为空");
        }
        if (queryLimit == 0) {
            return new EdgeScanner(cancel, null, null, 0, maxEdgesPerVertex, vid, predicate, onExamined, null, null, null);
        }
        byte[] prefix = KeyCodec.edgePrefix(graphId, RocksGraphStore.partOf(vid), vid, edgeTypeId, direction.code());
        byte[] upper = KeyCodec.upperBound(prefix);
        ReadOptions readOptions = new ReadOptions();
        Slice upperSlice = null;
        RocksIterator iterator = null;
        try {
            readOptions.setSnapshot(snapshot);
            readOptions.setPrefixSameAsStart(true);
            readOptions.setFillCache(true);
            if (upper != null) {
                upperSlice = new Slice(upper);
                readOptions.setIterateUpperBound(upperSlice);
            }
            iterator = db.newIterator(edgeFamily, readOptions);
            iterator.seek(prefix);
            iterator.status();
            EdgeScanner scanner = new EdgeScanner(
                    cancel, prefix, upper, queryLimit, maxEdgesPerVertex, vid, predicate, onExamined,
                    readOptions, upperSlice, iterator);
            if (cancel != null) {
                cancel.onCancel(scanner.closeHook);
            }
            return scanner;
        } catch (RuntimeException | RocksDBException ex) {
            if (iterator != null) {
                iterator.close();
            }
            if (upperSlice != null) {
                upperSlice.close();
            }
            readOptions.close();
            if (ex instanceof CengyanException cengyan) {
                throw cengyan;
            }
            throw new CengyanException(ErrorCode.INTERNAL, "无法开始扫边：" + ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized boolean hasNext() {
        if (pendingReady) {
            return true;
        }
        if (closedByCancel) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
        if (finished) {
            return false;
        }
        pull();
        return pendingReady;
    }

    @Override
    public synchronized EdgeRef next() {
        if (!hasNext()) {
            throw new NoSuchElementException("没有下一条边");
        }
        EdgeRef edge = pending;
        pending = null;
        pendingReady = false;
        if (stopAfterPending) {
            finish();
        }
        return edge;
    }

    @Override
    public synchronized int examined() {
        return examined;
    }

    private void pull() {
        while (iterator != null && iterator.isValid()) {
            if ((examined & 63) == 0 && cancel != null && cancel.cancelled()) {
                closeForCancel();
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            byte[] raw = iterator.key();
            byte[] key = Arrays.copyOf(raw, raw.length);
            if (upper != null && KeyCodec.compare(key, upper) >= 0) {
                finish();
                return;
            }
            if (!KeyCodec.startsWith(key, prefix)) {
                finish();
                return;
            }
            try {
                iterator.next();
                iterator.status();
            } catch (RocksDBException ex) {
                finish();
                throw new CengyanException(ErrorCode.INTERNAL, "扫边失败：" + ex.getMessage(), ex);
            }
            examined++;
            if (onExamined != null) {
                onExamined.run();
            }
            if (maxEdgesPerVertex >= 0 && examined > maxEdgesPerVertex) {
                int seen = examined;
                finish();
                throw new CengyanException(
                        ErrorCode.TRUNCATED,
                        "点 " + vid + " 已检查 " + seen + " 条边，超过每点上限 " + maxEdgesPerVertex);
            }
            EdgeRef logical = KeyCodec.toLogical(key);
            if (predicate != null && !predicate.test(logical)) {
                continue;
            }
            matched++;
            pending = logical;
            pendingReady = true;
            if (queryLimit >= 0 && matched == queryLimit) {
                stopAfterPending = true;
            }
            return;
        }
        finish();
    }

    private void finish() {
        release();
        finished = true;
        if (cancel != null) {
            cancel.forget(closeHook);
        }
    }

    private synchronized void closeForCancel() {
        closedByCancel = true;
        release();
        finished = true;
        if (cancel != null) {
            cancel.forget(closeHook);
        }
    }

    private void release() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        if (upperSlice != null) {
            upperSlice.close();
            upperSlice = null;
        }
        if (readOptions != null) {
            readOptions.close();
            readOptions = null;
        }
    }

    @Override
    public synchronized void close() {
        release();
        finished = true;
        if (cancel != null) {
            cancel.forget(closeHook);
        }
    }
}
