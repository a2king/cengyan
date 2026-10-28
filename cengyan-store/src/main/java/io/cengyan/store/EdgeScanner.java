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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 按前缀向后扫。单类型走 17 字节前缀；多种类型或不限类型走 12 字节起点前缀，并打开全序定位。
 * 每 64 条看一次取消；超过每点上限抛 {@code TRUNCATED}，而不是少返回一些。
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
    /** 0 表示前缀里已经带了方向。1 是出边，2 是入边，3 是两个方向。 */
    private final int directionMask;
    /** 为空表示不按类型过滤。 */
    private final Set<Integer> typeIds;
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
            RocksIterator iterator,
            int directionMask,
            Set<Integer> typeIds) {
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
        this.directionMask = directionMask;
        this.typeIds = typeIds;
    }

    static EdgeScanner open(
            RocksDB db,
            ColumnFamilyHandle edgeFamily,
            Snapshot snapshot,
            int graphId,
            int partId,
            long vid,
            int edgeTypeId,
            Direction direction,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel) {
        return open(db, edgeFamily, snapshot, graphId, partId, vid, edgeTypeId, direction,
                queryLimit, maxEdgesPerVertex, cancel, null, null);
    }

    static EdgeScanner open(
            RocksDB db,
            ColumnFamilyHandle edgeFamily,
            Snapshot snapshot,
            int graphId,
            int partId,
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
            return new EdgeScanner(
                    cancel, null, null, 0, maxEdgesPerVertex, vid, predicate, onExamined, null, null, null, 0, null);
        }
        byte[] prefix = KeyCodec.edgePrefix(graphId, partId, vid, edgeTypeId, direction.code());
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
                    readOptions, upperSlice, iterator, 0, null);
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

    /**
     * 从一个点扫出若干类型。{@code edgeTypeIds} 为空表示任何类型。
     * 没被选中的类型和另一个方向的键也会计入每点上限，因为这次扫描确实读过它们。
     */
    static EdgeScanner openMany(
            RocksDB db,
            ColumnFamilyHandle edgeFamily,
            Snapshot snapshot,
            int graphId,
            int partId,
            long vid,
            Set<Integer> edgeTypeIds,
            List<Direction> directions,
            int queryLimit,
            int maxEdgesPerVertex,
            CancelToken cancel,
            Predicate<EdgeRef> predicate,
            Runnable onExamined) {
        if (directions == null || directions.isEmpty()) {
            throw new IllegalArgumentException("方向不能为空");
        }
        int mask = 0;
        for (Direction direction : directions) {
            if (direction == null) {
                throw new IllegalArgumentException("方向不能为空");
            }
            mask |= direction == Direction.OUT ? 1 : 2;
        }
        Set<Integer> types = edgeTypeIds == null ? null : Set.copyOf(edgeTypeIds);
        if (queryLimit == 0) {
            return new EdgeScanner(
                    cancel, null, null, 0, maxEdgesPerVertex, vid, predicate, onExamined, null, null, null, mask, types);
        }
        byte[] prefix = KeyCodec.vertexPrefix(graphId, partId, vid);
        byte[] upper = KeyCodec.upperBound(prefix);
        ReadOptions readOptions = new ReadOptions();
        Slice upperSlice = null;
        RocksIterator iterator = null;
        try {
            readOptions.setSnapshot(snapshot);
            readOptions.setTotalOrderSeek(true);
            readOptions.setPrefixSameAsStart(false);
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
                    readOptions, upperSlice, iterator, mask, types);
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
            if (!selected(key)) {
                continue;
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

    private boolean selected(byte[] key) {
        if (directionMask == 0 && typeIds == null) {
            return true;
        }
        if (key.length < KeyCodec.EDGE_KEY_LENGTH) {
            return false;
        }
        if (directionMask != 0) {
            int code = key[16] & 0xFF;
            int bit = code == Direction.OUT.code() ? 1 : code == Direction.IN.code() ? 2 : 0;
            if ((directionMask & bit) == 0) {
                return false;
            }
        }
        return typeIds == null || typeIds.contains(KeyCodec.decodeEdge(key).edgeTypeId());
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
