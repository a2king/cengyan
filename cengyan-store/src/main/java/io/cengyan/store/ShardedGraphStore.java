package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Keeps catalog/index metadata in the root RocksDB and places graph data in
 * data/graphs/&lt;graph&gt;/parts-&lt;n&gt;/part-&lt;id&gt;/db.
 *
 * The class deliberately keeps the public GraphStore API unchanged. A batch
 * touching more than one physical partition is split into per-part batches;
 * cross-part edge recovery is still handled by the catalog transaction log.
 */
public final class ShardedGraphStore implements GraphStore {
    private static final int MAX_MIGRATION_OPS = 8192;

    private final Path directory;
    private final StoreOptions options;
    private final GraphStore root;
    private final Map<Part, GraphStore> parts = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile BiFunction<Integer, Long, Integer> partResolver = (graphId, vid) -> 0;
    private volatile IntFunction<Integer> partCountResolver = graphId -> 1;
    private volatile BiFunction<Integer, Integer, Path> directoryResolver;
    private volatile WriteGate writeGate;

    private ShardedGraphStore(Path directory, StoreOptions options) {
        this.directory = directory.toAbsolutePath().normalize();
        this.options = options;
        this.root = GraphStore.open(this.directory, options);
    }

    public static ShardedGraphStore open(Path directory, StoreOptions options) {
        if (directory == null || options == null) {
            throw new IllegalArgumentException("directory and options are required");
        }
        return new ShardedGraphStore(directory, options);
    }

    /** Opens a sibling node store with the same routing configuration. */
    public ShardedGraphStore openSibling(Path siblingDirectory) {
        ShardedGraphStore sibling = open(siblingDirectory, options);
        sibling.setPartResolver(partResolver);
        sibling.setPartCountResolver(partCountResolver);
        BiFunction<Integer, Integer, Path> resolver = directoryResolver;
        if (resolver != null) {
            sibling.setPartitionDirectoryResolver((graphId, partId) -> {
                Path current = resolver.apply(graphId, partId);
                Path relative = directory.relativize(current);
                return siblingDirectory.resolve(relative);
            });
        }
        return sibling;
    }

    @Override
    public GraphStore openReplica(Path siblingDirectory) {
        return openSibling(siblingDirectory);
    }

    private GraphStore part(int graphId, int partId) {
        BiFunction<Integer, Integer, Path> resolver = directoryResolver;
        if (resolver == null) {
            return root;
        }
        Part key = new Part(graphId, partId);
        return parts.computeIfAbsent(key, ignored -> {
            Path path = resolver.apply(graphId, partId).toAbsolutePath().normalize();
            if (path == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "partition directory is not configured");
            }
            try {
                Files.createDirectories(path);
            } catch (IOException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "cannot create partition directory: " + path, ex);
            }
            GraphStore opened = GraphStore.open(path, options);
            opened.setPartResolver(partResolver);
            return opened;
        });
    }

    private int partId(int graphId, long vid) {
        return partResolver.apply(graphId, vid);
    }

    private GraphStore storeFor(Cf family, byte[] key) {
        if (directoryResolver == null) {
            return root;
        }
        int graphId;
        long vid;
        switch (family) {
            case VERTEX -> {
                if (key.length != KeyCodec.VERTEX_KEY_LENGTH) return root;
                graphId = uint16(key, 0);
                return part(graphId, uint16(key, 2));
            }
            case EDGE -> {
                if (key.length != KeyCodec.EDGE_KEY_LENGTH) return root;
                KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
                return part(edge.graphId(), edge.partId());
            }
            case TXN -> {
                if (key.length != 32) return root;
                graphId = uint16(key, 0);
                return part(graphId, KeyCodec.txnPartId(key));
            }
            case PROP -> {
                if (key.length != KeyCodec.PROP_KEY_LENGTH) return root;
                graphId = uint16(key, 1);
                vid = KeyCodec.propSrc(key);
                return part(graphId, partId(graphId, vid));
            }
            default -> {
                return root;
            }
        }
    }

    private List<GraphStore> graphParts(int graphId) {
        int count = Math.max(1, partCountResolver.apply(graphId));
        List<GraphStore> result = new ArrayList<>(count);
        for (int id = 0; id < count; id++) {
            result.add(part(graphId, id));
        }
        return result;
    }

    @Override
    public void mutate(StoreBatch batch) {
        if (batch == null) throw new IllegalArgumentException("batch is required");
        if (batch.isEmpty()) return;
        writeLock.lock();
        try {
            WriteGate gate = writeGate;
            if (gate != null) {
                gate.replicate(this, batch);
            } else {
                applyLocalUnlocked(batch);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void applyLocal(StoreBatch batch) {
        if (batch == null) throw new IllegalArgumentException("batch is required");
        if (batch.isEmpty()) return;
        writeLock.lock();
        try {
            applyLocalUnlocked(batch);
        } finally {
            writeLock.unlock();
        }
    }

    private void applyLocalUnlocked(StoreBatch batch) {
        Map<GraphStore, StoreBatch> grouped = new LinkedHashMap<>();
        for (StoreBatch.Op op : batch.operations()) {
            GraphStore target = storeFor(op.family(), op.key());
            grouped.computeIfAbsent(target, ignored -> new StoreBatch()).put(
                    op.family(), op.key(), op.value(), op.delete());
        }
        for (Map.Entry<GraphStore, StoreBatch> entry : grouped.entrySet()) {
            entry.getKey().applyLocal(entry.getValue());
        }
    }

    @Override
    public void commitEdges(StoreBatch batch) {
        // A cross-part edge has one direction in each physical store. The
        // catalog transaction log provides recovery; the stores must therefore
        // be allowed to receive their half independently.
        mutate(batch);
    }

    @Override
    public void insertEdge(int graphId, int edgeTypeId, long rank, long outSrc, long outDst) {
        StoreBatch batch = new StoreBatch();
        int outPart = partId(graphId, outSrc);
        int inPart = partId(graphId, outDst);
        batch.put(Cf.EDGE, KeyCodec.edgeKey(
                graphId, outPart, outSrc, edgeTypeId, Direction.OUT.code(), rank, outDst), new byte[0]);
        batch.put(Cf.EDGE, KeyCodec.edgeKey(
                graphId, inPart, outDst, edgeTypeId, Direction.IN.code(), rank, outSrc), new byte[0]);
        commitEdges(batch);
    }

    @Override
    public ReadSession read() {
        return new ShardedReadSession();
    }

    @Override
    public <T> T withWriteLock(Supplier<T> action) {
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void injectBeforeWrite(Runnable hook) {
        root.injectBeforeWrite(hook);
    }

    @Override public long propReads() { return root.propReads(); }
    @Override public long edgesExamined() { return root.edgesExamined(); }
    @Override public long indexReads() { return root.indexReads(); }
    @Override public long verticesExamined() { return root.verticesExamined(); }

    @Override
    public long storageSizeBytes() {
        long size = root.storageSizeBytes();
        for (GraphStore store : parts.values()) {
            long partSize = store.storageSizeBytes();
            if (partSize >= 0) size += partSize;
        }
        return size;
    }

    @Override public void flush() { root.flush(); parts.values().forEach(GraphStore::flush); }
    @Override public void compact() { root.compact(); parts.values().forEach(GraphStore::compact); }
    @Override public Path directory() { return directory; }

    @Override
    public void checkpoint(Path target) {
        root.checkpoint(target);
        for (GraphStore store : parts.values()) {
            Path relative = directory.relativize(store.directory());
            store.checkpoint(target.resolve(relative));
        }
    }

    @Override public void setWriteGate(WriteGate gate) { writeGate = gate; }
    @Override public void setPartResolver(BiFunction<Integer, Long, Integer> resolver) {
        partResolver = resolver == null ? (graphId, vid) -> 0 : resolver;
        root.setPartResolver(partResolver);
        parts.values().forEach(store -> store.setPartResolver(partResolver));
    }
    @Override public void setPartCountResolver(IntFunction<Integer> resolver) {
        partCountResolver = resolver == null ? graphId -> 1 : resolver;
    }
    @Override public void setPartitionDirectoryResolver(BiFunction<Integer, Integer, Path> resolver) {
        directoryResolver = resolver;
    }

    @Override
    public void migrateLegacy(int[] graphIds) {
        if (directoryResolver == null || graphIds == null) return;
        for (int graphId : graphIds) {
            migrateFamily(graphId, Cf.VERTEX, KeyCodec.graphIdPrefix(graphId));
            migrateFamily(graphId, Cf.EDGE, KeyCodec.graphIdPrefix(graphId));
            migrateFamily(graphId, Cf.TXN, KeyCodec.graphIdPrefix(graphId));
            migrateFamily(graphId, Cf.PROP, KeyCodec.propGraphPrefix(graphId));
        }
    }

    @Override
    public void preparePartitions(int[] graphIds) {
        if (graphIds == null || directoryResolver == null) return;
        for (int graphId : graphIds) graphParts(graphId);
    }

    @Override
    public void deletePartition(int graphId, int partId) {
        if (directoryResolver == null) return;
        GraphStore opened = parts.remove(new Part(graphId, partId));
        if (opened != null) opened.close();
        Path path = directoryResolver.apply(graphId, partId);
        deleteTree(path);
    }

    @Override
    public boolean repartitionGraph(int graphId, int oldPartCount, int newPartCount) {
        if (directoryResolver == null || oldPartCount == newPartCount) return directoryResolver != null;
        List<GraphStore> oldStores = new ArrayList<>(oldPartCount);
        List<GraphStore> newStores = new ArrayList<>(newPartCount);
        List<Path> oldPaths = new ArrayList<>(oldPartCount);
        List<Path> newPaths = new ArrayList<>(newPartCount);
        Path graphRoot = null;
        for (int partId = 0; partId < oldPartCount; partId++) {
            Path path = directoryResolver.apply(graphId, partId).toAbsolutePath().normalize();
            oldPaths.add(path);
            if (graphRoot == null) graphRoot = path.getParent().getParent();
            oldStores.add(Files.isDirectory(path) ? GraphStore.open(path, options) : null);
        }
        if (graphRoot == null) {
            Path path = directoryResolver.apply(graphId, 0).toAbsolutePath().normalize();
            graphRoot = path.getParent().getParent();
        }
        Path stagingRoot = graphRoot.resolve(".reshard-" + UUID.randomUUID());
        for (int partId = 0; partId < newPartCount; partId++) {
            Path path = stagingRoot.resolve("part-" + partId).resolve("db")
                    .toAbsolutePath().normalize();
            newPaths.add(path);
            try {
                Files.createDirectories(path);
            } catch (IOException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "cannot create repartition directory: " + path, ex);
            }
            newStores.add(GraphStore.open(path, options));
        }
        try {
            for (GraphStore oldStore : oldStores) {
                if (oldStore == null) continue;
                copyRepartition(oldStore, newStores, graphId, Cf.VERTEX, oldPartCount, newPartCount);
                copyRepartition(oldStore, newStores, graphId, Cf.EDGE, oldPartCount, newPartCount);
                copyRepartition(oldStore, newStores, graphId, Cf.TXN, oldPartCount, newPartCount);
                copyRepartition(oldStore, newStores, graphId, Cf.PROP, oldPartCount, newPartCount);
            }
        } finally {
            oldStores.forEach(store -> { if (store != null) store.close(); });
            newStores.forEach(GraphStore::close);
        }
        for (Map.Entry<Part, GraphStore> entry : new ArrayList<>(parts.entrySet())) {
            if (entry.getKey().graphId() == graphId && parts.remove(entry.getKey(), entry.getValue())) {
                entry.getValue().close();
            }
        }
        for (Path path : oldPaths) deleteTree(path.getParent());
        for (int partId = 0; partId < newPartCount; partId++) {
            Path staged = stagingRoot.resolve("part-" + partId);
            Path target = graphRoot.resolve("part-" + partId);
            try {
                Files.createDirectories(target.getParent());
                Files.move(staged, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                deleteTree(stagingRoot);
                throw new CengyanException(ErrorCode.INTERNAL, "cannot install repartitioned data: " + target, ex);
            }
        }
        deleteTree(stagingRoot);
        return true;
    }

    private void copyRepartition(
            GraphStore oldStore, List<GraphStore> newStores, int graphId, Cf family,
            int oldPartCount, int newPartCount) {
        byte[] prefix = family == Cf.PROP ? KeyCodec.propGraphPrefix(graphId) : KeyCodec.graphIdPrefix(graphId);
        try (ReadSession read = oldStore.read()) {
            for (byte[] key : read.keysWithPrefix(family, prefix)) {
                byte[] value = read.get(family, key);
                if (value == null) continue;
                long source;
                int currentPart;
                byte[] targetKey = key;
                if (family == Cf.VERTEX) {
                    source = KeyCodec.vidOf(key);
                    currentPart = KeyCodec.partIdOfVertex(key);
                    int targetPart = Math.floorMod(source, newPartCount);
                    targetKey = KeyCodec.vertexKey(graphId, targetPart, source, KeyCodec.tagIdOf(key));
                } else if (family == Cf.EDGE) {
                    KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
                    source = edge.src();
                    currentPart = edge.partId();
                    int targetPart = Math.floorMod(source, newPartCount);
                    targetKey = KeyCodec.edgeKey(graphId, targetPart, edge.src(), edge.edgeTypeId(),
                            edge.direction(), edge.rank(), edge.dst());
                } else if (family == Cf.TXN) {
                    source = KeyCodec.txnSrc(key);
                    currentPart = KeyCodec.txnPartId(key);
                    int targetPart = Math.floorMod(source, newPartCount);
                    targetKey = KeyCodec.txnKey(graphId, targetPart, KeyCodec.txnEdgeTypeId(key),
                            KeyCodec.txnRank(key), KeyCodec.txnSrc(key), KeyCodec.txnDst(key));
                } else {
                    source = KeyCodec.propSrc(key);
                    currentPart = Math.floorMod(source, oldPartCount);
                }
                int targetPart = Math.floorMod(source, newPartCount);
                if (currentPart != targetPart || family == Cf.PROP || family == Cf.VERTEX
                        || family == Cf.EDGE || family == Cf.TXN) {
                    StoreBatch batch = new StoreBatch();
                    batch.put(family, targetKey, value);
                    newStores.get(targetPart).applyLocal(batch);
                }
            }
        }
    }

    private static void deleteTree(Path path) {
        if (path == null || !Files.exists(path)) return;
        try (var walk = Files.walk(path)) {
            for (Path item : walk.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(item);
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "cannot delete old partition directory: " + path, ex);
        }
    }

    private void migrateFamily(int graphId, Cf family, byte[] prefix) {
        List<byte[]> keys;
        try (ReadSession session = root.read()) {
            keys = session.keysWithPrefix(family, prefix);
        }
        if (keys.isEmpty()) return;
        Map<GraphStore, StoreBatch> copies = new LinkedHashMap<>();
        Map<GraphStore, StoreBatch> deletes = new LinkedHashMap<>();
        try (ReadSession session = root.read()) {
            for (byte[] key : keys) {
                byte[] value = session.get(family, key);
                if (value == null) continue;
                GraphStore target = storeFor(family, key);
                copies.computeIfAbsent(target, ignored -> new StoreBatch()).put(family, key, value);
                deletes.computeIfAbsent(root, ignored -> new StoreBatch()).delete(family, key);
                if (copies.get(target).operations().size() >= MAX_MIGRATION_OPS) {
                    target.applyLocal(copies.remove(target));
                }
            }
        }
        for (Map.Entry<GraphStore, StoreBatch> entry : copies.entrySet()) {
            if (!entry.getValue().isEmpty()) entry.getKey().applyLocal(entry.getValue());
        }
        for (StoreBatch.Op op : deletes.getOrDefault(root, new StoreBatch()).operations()) {
            // Keep deletion batches bounded so a large upgrade does not create one giant WriteBatch.
            StoreBatch one = new StoreBatch();
            one.delete(op.family(), op.key());
            root.applyLocal(one);
        }
    }

    @Override
    public void close() {
        for (GraphStore store : parts.values()) store.close();
        parts.clear();
        root.close();
    }

    private final class ShardedReadSession implements ReadSession {
        private final Map<GraphStore, ReadSession> sessions = new HashMap<>();
        private ReadSession session(GraphStore store) { return sessions.computeIfAbsent(store, GraphStore::read); }

        @Override public EdgeCursor scanEdges(int graphId, long vid, int edgeTypeId, Direction direction,
                int queryLimit, int maxEdgesPerVertex, CancelToken cancel) {
            return session(part(graphId, partId(graphId, vid))).scanEdges(
                    graphId, vid, edgeTypeId, direction, queryLimit, maxEdgesPerVertex, cancel);
        }
        @Override public EdgeCursor scanEdges(int graphId, long vid, int edgeTypeId, Direction direction,
                int queryLimit, int maxEdgesPerVertex, java.util.function.Predicate<EdgeRef> predicate, CancelToken cancel) {
            return session(part(graphId, partId(graphId, vid))).scanEdges(
                    graphId, vid, edgeTypeId, direction, queryLimit, maxEdgesPerVertex, predicate, cancel);
        }
        @Override public EdgeCursor scanEdges(int graphId, long vid, Set<Integer> types, List<Direction> dirs,
                int queryLimit, int maxEdgesPerVertex, java.util.function.Predicate<EdgeRef> predicate, CancelToken cancel) {
            return session(part(graphId, partId(graphId, vid))).scanEdges(
                    graphId, vid, types, dirs, queryLimit, maxEdgesPerVertex, predicate, cancel);
        }
        @Override public List<VertexId> scanVertices(int graphId, int tagId, int queryLimit, int maxExamined, CancelToken cancel) {
            List<VertexId> result = new ArrayList<>();
            for (GraphStore store : graphParts(graphId)) {
                int remaining = queryLimit < 0 ? -1 : queryLimit - result.size();
                if (remaining == 0) break;
                result.addAll(session(store).scanVertices(graphId, tagId, remaining, maxExamined, cancel));
            }
            return result;
        }
        @Override public byte[] get(Cf family, byte[] key) { return session(storeFor(family, key)).get(family, key); }
        @Override public List<byte[]> multiGet(Cf family, List<byte[]> keys) {
            List<byte[]> result = new ArrayList<>(keys.size());
            for (byte[] key : keys) result.add(get(family, key));
            return result;
        }
        @Override public List<byte[]> keysWithPrefix(Cf family, byte[] prefix) {
            if (directoryResolver == null || family == Cf.CATALOG || family == Cf.INDEX || family == Cf.DEFAULT) {
                return session(root).keysWithPrefix(family, prefix);
            }
            List<GraphStore> targets = storesForPrefix(family, prefix);
            List<byte[]> result = new ArrayList<>();
            for (GraphStore target : targets) result.addAll(session(target).keysWithPrefix(family, prefix));
            return result;
        }
        @Override public void visitPrefix(Cf family, byte[] prefix, CancelToken cancel, java.util.function.Consumer<byte[]> consumer) {
            for (byte[] key : keysWithPrefix(family, prefix)) {
                if (cancel != null && cancel.cancelled()) throw new CengyanException(ErrorCode.QUERY_CANCELED, "query canceled");
                consumer.accept(key);
            }
        }
        @Override public List<byte[]> scanIndex(byte[] start, byte[] end, int limit, CancelToken cancel) {
            return session(root).scanIndex(start, end, limit, cancel);
        }
        private List<GraphStore> storesForPrefix(Cf family, byte[] prefix) {
            int graphId = family == Cf.PROP ? (prefix.length >= 3 ? uint16(prefix, 1) : -1) :
                    (prefix.length >= 2 ? uint16(prefix, 0) : -1);
            if (graphId < 0) return List.of(root);
            if ((family == Cf.VERTEX || family == Cf.EDGE || family == Cf.TXN) && prefix.length >= 4) {
                return List.of(part(graphId, uint16(prefix, 2)));
            }
            return graphParts(graphId);
        }
        @Override public void close() { sessions.values().forEach(ReadSession::close); sessions.clear(); }
    }

    private static int uint16(byte[] key, int offset) {
        return ((key[offset] & 0xff) << 8) | (key[offset + 1] & 0xff);
    }

    private record Part(int graphId, int partId) {}
}
