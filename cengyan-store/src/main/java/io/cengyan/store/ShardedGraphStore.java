package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * 系统目录放在 {@code <data>/_system}。某个图产生的数据都在 {@code <data>/<图名>/} 下面：
 * 点、边、索引按类型和分片各开一个 RocksDB，元数据在 {@code meta}。
 * WAL 和刷盘文件跟各自的库走，不同类型可以同时写。
 */
public final class ShardedGraphStore implements GraphStore {
    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final StoreOptions options;
    private final Cache cache;
    private final RocksGraphStore system;
    private final Map<Path, GraphStore> stores = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
    private final Map<String, ReentrantLock> stripeLocks = new ConcurrentHashMap<>();
    private volatile BiFunction<Integer, Long, Integer> partResolver = (graphId, vid) -> 0;
    private volatile IntFunction<Integer> partCountResolver = graphId -> 1;
    private volatile DataPlacement placement;
    private volatile WriteGate writeGate;
    private volatile Runnable beforeWrite;

    private ShardedGraphStore(Path directory, StoreOptions options) {
        this.directory = directory.toAbsolutePath().normalize();
        this.options = options;
        this.cache = new LRUCache(options.blockCacheBytes());
        try {
            Env env = Env.getDefault();
            int jobs = options.maxBackgroundJobs();
            env.setBackgroundThreads(Math.max(1, jobs), Priority.LOW);
            env.setBackgroundThreads(Math.max(1, Math.min(jobs, 2)), Priority.HIGH);
            this.system = RocksGraphStore.open(
                    GraphPaths.system(this.directory), options, GraphPaths.CATALOG_FAMILIES, cache);
        } catch (RuntimeException ex) {
            this.cache.close();
            throw ex;
        }
    }

    public static ShardedGraphStore open(Path directory, StoreOptions options) {
        if (directory == null || options == null) {
            throw new IllegalArgumentException("directory and options are required");
        }
        return new ShardedGraphStore(directory, options);
    }

    /** 副本使用同一套摆放规则，路径改到副本目录下。 */
    public ShardedGraphStore openSibling(Path siblingDirectory) {
        ShardedGraphStore sibling = open(siblingDirectory, options);
        sibling.setPartResolver(partResolver);
        sibling.setPartCountResolver(partCountResolver);
        DataPlacement current = placement;
        if (current != null) {
            sibling.setDataPlacement(new RemappedPlacement(current, directory, sibling.directory()));
        }
        return sibling;
    }

    @Override
    public GraphStore openReplica(Path siblingDirectory) {
        return openSibling(siblingDirectory);
    }

    public void setDataPlacement(DataPlacement placement) {
        this.placement = placement;
    }

    /** 直接读一个已经摆好的目录，不再次按键路由。 */
    public byte[] readAt(Path path, Cf family, byte[] key) {
        GraphStore opened = openAt(path, false);
        if (opened == null) {
            return null;
        }
        try (ReadSession session = opened.read()) {
            return session.get(family, key);
        }
    }

    private GraphStore storeFor(Cf family, byte[] key, boolean create) {
        DataPlacement current = placement;
        if (current == null) {
            if (family != Cf.CATALOG && family != Cf.DEFAULT) {
                throw new CengyanException(ErrorCode.INTERNAL, "数据目录还没有按图摆好");
            }
            return system;
        }
        return openAt(current.place(family, key), create);
    }

    private GraphStore openAt(Path path, boolean create) {
        if (path == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "数据目录还没有按图摆好");
        }
        Path normal = path.toAbsolutePath().normalize();
        if (normal.equals(system.directory())) {
            return system;
        }
        GraphStore existing = stores.get(normal);
        if (existing != null) {
            return existing;
        }
        if (!create && !Files.exists(normal.resolve("CURRENT"))) {
            return null;
        }
        return stores.computeIfAbsent(normal, this::openNew);
    }

    private GraphStore openNew(Path path) {
        RocksGraphStore opened = RocksGraphStore.open(path, options, GraphPaths.families(path), cache);
        opened.setPartResolver(partResolver);
        return opened;
    }

    private void runWrite(Runnable action) {
        if (gate.getReadHoldCount() > 0 || gate.isWriteLockedByCurrentThread()) {
            action.run();
            return;
        }
        gate.writeLock().lock();
        try {
            action.run();
        } finally {
            gate.writeLock().unlock();
        }
    }

    @Override
    public void mutate(StoreBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch is required");
        }
        if (batch.isEmpty()) {
            return;
        }
        runWrite(() -> {
            WriteGate gateNow = writeGate;
            if (gateNow != null) {
                gateNow.replicate(this, batch);
            } else {
                applyLocalUnlocked(batch);
            }
        });
    }

    @Override
    public void applyLocal(StoreBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch is required");
        }
        if (batch.isEmpty()) {
            return;
        }
        runWrite(() -> applyLocalUnlocked(batch));
    }

    private void applyLocalUnlocked(StoreBatch batch) {
        Map<GraphStore, StoreBatch> grouped = new LinkedHashMap<>();
        for (StoreBatch.Op op : batch.operations()) {
            GraphStore target = storeFor(op.family(), op.key(), true);
            grouped.computeIfAbsent(target, ignored -> new StoreBatch())
                    .put(op.family(), op.key(), op.value(), op.delete());
        }
        List<Map.Entry<GraphStore, StoreBatch>> ordered = new ArrayList<>(grouped.entrySet());
        ordered.sort(Comparator.comparingInt(entry -> GraphPaths.writeOrder(entry.getKey().directory())));
        boolean hooked = false;
        for (Map.Entry<GraphStore, StoreBatch> entry : ordered) {
            if (!hooked && beforeWrite != null) {
                entry.getKey().injectBeforeWrite(beforeWrite);
                beforeWrite = null;
                hooked = true;
            }
            entry.getKey().applyLocal(entry.getValue());
        }
    }

    @Override
    public void commitEdges(StoreBatch batch) {
        mutate(batch);
    }

    @Override
    public void insertEdge(int graphId, int edgeTypeId, long rank, long outSrc, long outDst) {
        StoreBatch batch = new StoreBatch();
        int outPart = partResolver.apply(graphId, outSrc);
        int inPart = partResolver.apply(graphId, outDst);
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
        if (action == null) {
            throw new IllegalArgumentException("写入动作不能为空");
        }
        gate.writeLock().lock();
        try {
            return action.get();
        } finally {
            gate.writeLock().unlock();
        }
    }

    @Override
    public <T> T withStripes(List<String> stripes, Supplier<T> action) {
        if (action == null) {
            throw new IllegalArgumentException("写入动作不能为空");
        }
        if (stripes == null || stripes.isEmpty()) {
            return withWriteLock(action);
        }
        List<String> ordered = stripes.stream().filter(stripe -> stripe != null && !stripe.isBlank())
                .distinct().sorted().toList();
        if (ordered.isEmpty()) {
            return withWriteLock(action);
        }
        gate.readLock().lock();
        List<ReentrantLock> locked = new ArrayList<>();
        try {
            for (String stripe : ordered) {
                ReentrantLock lock = stripeLocks.computeIfAbsent(stripe, ignored -> new ReentrantLock());
                lock.lock();
                locked.add(lock);
            }
            return action.get();
        } finally {
            for (int i = locked.size() - 1; i >= 0; i--) {
                locked.get(i).unlock();
            }
            gate.readLock().unlock();
        }
    }

    @Override
    public void injectBeforeWrite(Runnable hook) {
        beforeWrite = hook;
    }

    @Override
    public long propReads() {
        return sum(GraphStore::propReads);
    }

    @Override
    public long edgesExamined() {
        return sum(GraphStore::edgesExamined);
    }

    @Override
    public long indexReads() {
        return sum(GraphStore::indexReads);
    }

    @Override
    public long verticesExamined() {
        return sum(GraphStore::verticesExamined);
    }

    private long sum(ToLongFunction<GraphStore> reader) {
        long total = reader.applyAsLong(system);
        for (GraphStore store : stores.values()) {
            total += reader.applyAsLong(store);
        }
        return total;
    }

    @Override
    public long storageSizeBytes() {
        try (var files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (IOException ignored) {
                    return 0L;
                }
            }).sum();
        } catch (IOException ignored) {
            return -1L;
        }
    }

    @Override
    public void flush() {
        system.flush();
        for (GraphStore store : List.copyOf(stores.values())) {
            store.flush();
        }
    }

    @Override
    public void compact() {
        for (Path dir : findDatabases()) {
            openAt(dir, true).compact();
        }
    }

    @Override
    public void compactOpen() {
        system.compact();
        for (GraphStore store : List.copyOf(stores.values())) {
            store.compact();
        }
    }

    @Override
    public Path directory() {
        return directory;
    }

    @Override
    public void checkpoint(Path target) {
        for (Path dir : findDatabases()) {
            Path relative = directory.relativize(dir.toAbsolutePath().normalize());
            openAt(dir, true).checkpoint(target.resolve(relative));
        }
    }

    private List<Path> findDatabases() {
        List<Path> found = new ArrayList<>();
        if (Files.exists(system.directory().resolve("CURRENT"))) {
            found.add(system.directory());
        }
        if (!Files.isDirectory(directory)) {
            return found;
        }
        try (var walk = Files.walk(directory)) {
            for (Path current : walk.filter(path -> "CURRENT".equals(fileName(path))).toList()) {
                Path dir = current.getParent();
                if (dir.equals(system.directory()) || dir.toString().contains(".reshard-")) {
                    continue;
                }
                found.add(dir);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法枚举数据目录：" + ex.getMessage(), ex);
        }
        return found;
    }

    @Override
    public void setWriteGate(WriteGate gate) {
        writeGate = gate;
    }

    @Override
    public void setPartResolver(BiFunction<Integer, Long, Integer> resolver) {
        partResolver = resolver == null ? (graphId, vid) -> 0 : resolver;
        system.setPartResolver(partResolver);
        stores.values().forEach(store -> store.setPartResolver(partResolver));
    }

    @Override
    public void setPartCountResolver(IntFunction<Integer> resolver) {
        partCountResolver = resolver == null ? graphId -> 1 : resolver;
    }

    @Override
    public void noteGraph(int graphId, String name) {
        DataPlacement current = placement;
        if (current != null && name != null) {
            current.rememberGraph(graphId, name);
        }
    }

    @Override
    public void deleteGraphDirectory(String name) {
        if (name == null || name.isBlank() || GraphPaths.SYSTEM_DIRECTORY.equals(name)) {
            return;
        }
        Path root = directory.resolve(name).toAbsolutePath().normalize();
        closeUnder(root);
        deleteTree(root);
    }

    @Override
    public void deletePartition(int graphId, int partId) {
        DataPlacement current = placement;
        if (current == null) {
            return;
        }
        Path graphRoot = directory.resolve(current.graphName(graphId));
        String shard = Integer.toString(partId);
        deleteShard(graphRoot.resolve("data").resolve("node"), shard);
        deleteShard(graphRoot.resolve("data").resolve("edge"), shard);
        deleteShard(graphRoot.resolve("index"), shard);
    }

    @Override
    public boolean repartitionGraph(int graphId, int oldPartCount, int newPartCount) {
        DataPlacement current = placement;
        if (current == null || oldPartCount == newPartCount) {
            return current != null;
        }
        Path graphRoot = directory.resolve(current.graphName(graphId)).toAbsolutePath().normalize();
        Path staging = graphRoot.resolve(".reshard-" + UUID.randomUUID());
        Map<Path, GraphStore> staged = new HashMap<>();
        Map<Path, StoreBatch> batches = new HashMap<>();
        boolean liveRemoved = false;
        try {
            for (int typeId : current.nodeTypeIds(graphId)) {
                for (int part = 0; part < oldPartCount; part++) {
                    copyFamily(current.nodeShard(graphId, typeId, part), Cf.VERTEX, KeyCodec.graphIdPrefix(graphId),
                            (key, value) -> {
                                long vid = KeyCodec.vidOf(key);
                                int target = KeyCodec.partOf(vid, newPartCount);
                                byte[] rewritten = KeyCodec.vertexKey(
                                        graphId, target, vid, KeyCodec.tagIdOf(key));
                                stage(batches, staged, current.nodeShard(graphId, typeId, target),
                                        graphRoot, staging, Cf.VERTEX, rewritten, value);
                            });
                }
            }
            for (int typeId : current.edgeTypeIds(graphId)) {
                for (int part = 0; part < oldPartCount; part++) {
                    Path source = current.edgeShard(graphId, typeId, part);
                    copyFamily(source, Cf.EDGE, KeyCodec.graphIdPrefix(graphId), (key, value) -> {
                        KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
                        int target = KeyCodec.partOf(edge.src(), newPartCount);
                        byte[] rewritten = KeyCodec.edgeKey(
                                graphId, target, edge.src(), edge.edgeTypeId(), edge.direction(),
                                edge.rank(), edge.dst());
                        stage(batches, staged, current.edgeShard(graphId, typeId, target),
                                graphRoot, staging, Cf.EDGE, rewritten, value);
                    });
                    copyFamily(source, Cf.TXN, KeyCodec.graphIdPrefix(graphId), (key, value) -> {
                        int target = KeyCodec.partOf(KeyCodec.txnSrc(key), newPartCount);
                        byte[] rewritten = KeyCodec.txnKey(
                                graphId, target, KeyCodec.txnEdgeTypeId(key), KeyCodec.txnRank(key),
                                KeyCodec.txnSrc(key), KeyCodec.txnDst(key));
                        stage(batches, staged, current.edgeShard(graphId, typeId, target),
                                graphRoot, staging, Cf.TXN, rewritten, value);
                    });
                    copyFamily(source, Cf.PROP, KeyCodec.propGraphPrefix(graphId), (key, value) -> {
                        int target = KeyCodec.partOf(KeyCodec.propSrc(key), newPartCount);
                        stage(batches, staged, current.edgeShard(graphId, typeId, target),
                                graphRoot, staging, Cf.PROP, key, value);
                    });
                }
            }
            for (int indexId : current.indexIds(graphId)) {
                byte[] prefix = KeyCodec.indexDataPrefix(indexId);
                for (int part = 0; part < oldPartCount; part++) {
                    copyFamily(current.indexShard(indexId, part), Cf.INDEX, prefix, (key, value) -> {
                        int target = current.indexPart(key, newPartCount);
                        stage(batches, staged, current.indexShard(indexId, target),
                                graphRoot, staging, Cf.INDEX, key, value);
                    });
                }
            }
            flushStaged(batches, staged, true);
            closeAll(staged);
            closeUnder(graphRoot.resolve("data"));
            closeUnder(graphRoot.resolve("index"));
            deleteTree(graphRoot.resolve("data"));
            deleteTree(graphRoot.resolve("index"));
            liveRemoved = true;
            moveIfExists(staging.resolve("data"), graphRoot.resolve("data"));
            moveIfExists(staging.resolve("index"), graphRoot.resolve("index"));
        } catch (RuntimeException ex) {
            closeAll(staged);
            if (!liveRemoved) {
                deleteTree(staging);
            }
            throw ex;
        }
        deleteTree(staging);
        return true;
    }

    @FunctionalInterface
    private interface ShardWriter {
        void accept(byte[] key, byte[] value);
    }

    private void copyFamily(Path source, Cf family, byte[] prefix, ShardWriter writer) {
        GraphStore opened = openAt(source, false);
        if (opened == null) {
            return;
        }
        try (ReadSession read = opened.read()) {
            read.visitPrefix(family, prefix, null, key -> {
                byte[] value = read.get(family, key);
                if (value != null) {
                    writer.accept(key, value);
                }
            });
        }
    }

    private void stage(
            Map<Path, StoreBatch> batches,
            Map<Path, GraphStore> opened,
            Path finalPath,
            Path graphRoot,
            Path staging,
            Cf family,
            byte[] key,
            byte[] value) {
        Path relative = graphRoot.relativize(finalPath.toAbsolutePath().normalize());
        Path stagedPath = staging.resolve(relative).toAbsolutePath().normalize();
        StoreBatch batch = batches.computeIfAbsent(stagedPath, ignored -> new StoreBatch());
        batch.put(family, key, value);
        if (batch.operations().size() >= 512) {
            flushStaged(batches, opened, false);
        }
    }

    private void flushStaged(Map<Path, StoreBatch> batches, Map<Path, GraphStore> opened, boolean force) {
        List<Path> ready = new ArrayList<>();
        for (Map.Entry<Path, StoreBatch> entry : batches.entrySet()) {
            int size = entry.getValue().operations().size();
            if (size > 0 && (force || size >= 512)) {
                ready.add(entry.getKey());
            }
        }
        for (Path path : ready) {
            StoreBatch batch = batches.get(path);
            if (batch == null || batch.isEmpty()) {
                continue;
            }
            GraphStore target = opened.computeIfAbsent(path, this::openStaged);
            target.applyLocal(batch);
            batches.put(path, new StoreBatch());
        }
    }

    private GraphStore openStaged(Path path) {
        return RocksGraphStore.open(path, options, GraphPaths.families(path), cache);
    }

    private static void closeAll(Map<Path, GraphStore> opened) {
        for (GraphStore store : opened.values()) {
            store.close();
        }
        opened.clear();
    }

    private static void moveIfExists(Path source, Path target) {
        if (!Files.exists(source)) {
            return;
        }
        try {
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicFailed) {
            try {
                Files.move(source, target);
            } catch (IOException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "无法安装分片目录：" + target, ex);
            }
        }
    }

    private void closeUnder(Path root) {
        if (root == null) {
            return;
        }
        Path normal = root.toAbsolutePath().normalize();
        List<Path> matched = new ArrayList<>();
        for (Path path : stores.keySet()) {
            if (path.startsWith(normal)) {
                matched.add(path);
            }
        }
        for (Path path : matched) {
            GraphStore removed = stores.remove(path);
            if (removed != null) {
                removed.close();
            }
        }
    }

    private void deleteShard(Path parent, String shard) {
        if (!Files.isDirectory(parent)) {
            return;
        }
        try (var types = Files.list(parent)) {
            for (Path typeDir : types.toList()) {
                if (!Files.isDirectory(typeDir)) {
                    continue;
                }
                Path shardDir = typeDir.resolve(shard);
                closeUnder(shardDir);
                deleteTree(shardDir);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法删除分片目录：" + parent, ex);
        }
    }

    private static void deleteTree(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            for (Path item : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(item);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法删除目录：" + path, ex);
        }
    }

    private int partsOf(int graphId) {
        DataPlacement current = placement;
        if (current != null) {
            try {
                return current.partCount(graphId);
            } catch (UnsupportedOperationException ignored) {
                // 迁移过程中的摆放规则不提供分片数。
            }
        }
        return partCountResolver.apply(graphId);
    }

    private static String fileName(Path path) {
        Path name = path.getFileName();
        return name == null ? "" : name.toString();
    }

    @Override
    public void close() {
        for (GraphStore store : stores.values()) {
            store.close();
        }
        stores.clear();
        system.close();
        cache.close();
    }

    private static List<byte[]> mergeKeys(List<List<byte[]>> parts, int limit) {
        record Node(byte[] key, int list, int index) {
        }
        PriorityQueue<Node> queue = new PriorityQueue<>(Comparator.comparing(Node::key, KeyCodec::compare));
        for (int i = 0; i < parts.size(); i++) {
            if (!parts.get(i).isEmpty()) {
                queue.add(new Node(parts.get(i).get(0), i, 0));
            }
        }
        List<byte[]> merged = new ArrayList<>();
        while (!queue.isEmpty() && (limit < 0 || merged.size() < limit)) {
            Node node = queue.poll();
            merged.add(node.key());
            int next = node.index() + 1;
            List<byte[]> list = parts.get(node.list());
            if (next < list.size()) {
                queue.add(new Node(list.get(next), node.list(), next));
            }
        }
        return merged;
    }

    private final class ShardedReadSession implements ReadSession {
        private final Map<GraphStore, ReadSession> sessions = new HashMap<>();

        private ReadSession session(GraphStore store) {
            return sessions.computeIfAbsent(store, GraphStore::read);
        }

        @Override
        public EdgeCursor scanEdges(
                int graphId, long vid, int edgeTypeId, Direction direction,
                int queryLimit, int maxEdgesPerVertex, CancelToken cancel) {
            return scanEdges(graphId, vid, Set.of(edgeTypeId), List.of(direction),
                    queryLimit, maxEdgesPerVertex, null, cancel);
        }

        @Override
        public EdgeCursor scanEdges(
                int graphId, long vid, int edgeTypeId, Direction direction,
                int queryLimit, int maxEdgesPerVertex,
                java.util.function.Predicate<EdgeRef> predicate, CancelToken cancel) {
            return scanEdges(graphId, vid, Set.of(edgeTypeId), List.of(direction),
                    queryLimit, maxEdgesPerVertex, predicate, cancel);
        }

        @Override
        public EdgeCursor scanEdges(
                int graphId, long vid, Set<Integer> edgeTypeIds, List<Direction> directions,
                int queryLimit, int maxEdgesPerVertex,
                java.util.function.Predicate<EdgeRef> predicate, CancelToken cancel) {
            DataPlacement current = placement;
            if (current == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "数据目录还没有按图摆好");
            }
            if (directions == null || directions.isEmpty()) {
                throw new IllegalArgumentException("方向不能为空");
            }
            Set<Integer> types = edgeTypeIds == null || edgeTypeIds.isEmpty()
                    ? current.edgeTypeIds(graphId) : edgeTypeIds;
            if (types.isEmpty() || queryLimit == 0) {
                return ChainEdgeCursor.empty();
            }
            int part = partResolver.apply(graphId, vid);
            if (types.size() == 1) {
                int typeId = types.iterator().next();
                GraphStore child = openAt(current.edgeShard(graphId, typeId, part), false);
                if (child == null) {
                    return ChainEdgeCursor.empty();
                }
                if (directions.size() == 1) {
                    return session(child).scanEdges(
                            graphId, vid, typeId, directions.get(0), queryLimit, maxEdgesPerVertex, predicate, cancel);
                }
                return session(child).scanEdges(
                        graphId, vid, Set.of(typeId), directions, queryLimit, maxEdgesPerVertex, predicate, cancel);
            }
            List<Integer> ordered = new ArrayList<>(types);
            ordered.sort(Integer::compareTo);
            List<ChainEdgeCursor.Opener> openers = new ArrayList<>();
            for (int typeId : ordered) {
                openers.add((examined, matched) -> {
                    int leftQuery = queryLimit < 0 ? -1 : queryLimit - matched;
                    int leftMax = maxEdgesPerVertex < 0 ? -1 : maxEdgesPerVertex - examined;
                    if (leftQuery == 0) {
                        return null;
                    }
                    GraphStore child = openAt(current.edgeShard(graphId, typeId, part), false);
                    if (child == null) {
                        return null;
                    }
                    return session(child).scanEdges(
                            graphId, vid, Set.of(typeId), directions, leftQuery, leftMax, predicate, cancel);
                });
            }
            return new ChainEdgeCursor(openers, queryLimit, maxEdgesPerVertex, vid);
        }

        @Override
        public List<VertexId> scanVertices(
                int graphId, int tagId, int queryLimit, int maxExamined, CancelToken cancel) {
            DataPlacement current = placement;
            if (current == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "数据目录还没有按图摆好");
            }
            if (queryLimit == 0) {
                return List.of();
            }
            List<VertexId> result = new ArrayList<>();
            int remainingQuery = queryLimit;
            int remainingExamined = maxExamined;
            int parts = Math.max(1, partsOf(graphId));
            for (int part = 0; part < parts; part++) {
                if (remainingQuery == 0) {
                    break;
                }
                GraphStore child = openAt(current.nodeShard(graphId, tagId, part), false);
                if (child == null) {
                    continue;
                }
                long before = child.verticesExamined();
                List<VertexId> found = session(child).scanVertices(
                        graphId, tagId, remainingQuery, remainingExamined, cancel);
                int used = (int) (child.verticesExamined() - before);
                if (remainingExamined >= 0) {
                    remainingExamined = Math.max(0, remainingExamined - used);
                }
                result.addAll(found);
                if (remainingQuery >= 0) {
                    remainingQuery -= found.size();
                }
            }
            return result;
        }

        @Override
        public byte[] get(Cf family, byte[] key) {
            GraphStore target = storeFor(family, key, false);
            if (target == null) {
                return null;
            }
            return session(target).get(family, key);
        }

        @Override
        public List<byte[]> multiGet(Cf family, List<byte[]> keys) {
            List<byte[]> result = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                result.add(get(family, key));
            }
            return result;
        }

        @Override
        public List<byte[]> keysWithPrefix(Cf family, byte[] prefix) {
            DataPlacement current = placement;
            if (current == null) {
                if (family != Cf.CATALOG && family != Cf.DEFAULT) {
                    return List.of();
                }
                return session(system).keysWithPrefix(family, prefix);
            }
            List<byte[]> result = new ArrayList<>();
            for (Path path : current.placePrefix(family, prefix)) {
                GraphStore target = openAt(path, false);
                if (target == null) {
                    continue;
                }
                result.addAll(session(target).keysWithPrefix(family, prefix));
            }
            return result;
        }

        @Override
        public void visitPrefix(
                Cf family, byte[] prefix, CancelToken cancel, java.util.function.Consumer<byte[]> consumer) {
            for (byte[] key : keysWithPrefix(family, prefix)) {
                if (cancel != null && cancel.cancelled()) {
                    throw new CengyanException(ErrorCode.QUERY_CANCELED, "query canceled");
                }
                consumer.accept(key);
            }
        }

        @Override
        public List<byte[]> scanIndex(byte[] start, byte[] end, int limit, CancelToken cancel) {
            DataPlacement current = placement;
            if (current == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "数据目录还没有按图摆好");
            }
            List<List<byte[]>> parts = new ArrayList<>();
            for (Path path : current.placePrefix(Cf.INDEX, start)) {
                GraphStore target = openAt(path, false);
                if (target == null) {
                    continue;
                }
                parts.add(session(target).scanIndex(start, end, limit, cancel));
            }
            if (parts.isEmpty()) {
                return List.of();
            }
            if (parts.size() == 1) {
                return parts.get(0);
            }
            return mergeKeys(parts, limit);
        }

        @Override
        public void close() {
            sessions.values().forEach(ReadSession::close);
            sessions.clear();
        }
    }

    private static final class RemappedPlacement implements DataPlacement {
        private final DataPlacement delegate;
        private final Path from;
        private final Path to;

        private RemappedPlacement(DataPlacement delegate, Path from, Path to) {
            this.delegate = delegate;
            this.from = from.toAbsolutePath().normalize();
            this.to = to.toAbsolutePath().normalize();
        }

        private Path map(Path path) {
            Path absolute = path.toAbsolutePath().normalize();
            if (absolute.startsWith(from)) {
                return to.resolve(from.relativize(absolute));
            }
            return absolute;
        }

        private List<Path> mapAll(List<Path> paths) {
            List<Path> mapped = new ArrayList<>(paths.size());
            for (Path path : paths) {
                mapped.add(map(path));
            }
            return mapped;
        }

        @Override
        public Path place(Cf family, byte[] key) {
            return map(delegate.place(family, key));
        }

        @Override
        public List<Path> placePrefix(Cf family, byte[] prefix) {
            return mapAll(delegate.placePrefix(family, prefix));
        }

        @Override
        public void rememberGraph(int graphId, String name) {
            delegate.rememberGraph(graphId, name);
        }

        @Override
        public String graphName(int graphId) {
            return delegate.graphName(graphId);
        }

        @Override
        public int partCount(int graphId) {
            return delegate.partCount(graphId);
        }

        @Override
        public Set<Integer> nodeTypeIds(int graphId) {
            return delegate.nodeTypeIds(graphId);
        }

        @Override
        public Set<Integer> edgeTypeIds(int graphId) {
            return delegate.edgeTypeIds(graphId);
        }

        @Override
        public List<Integer> indexIds(int graphId) {
            return delegate.indexIds(graphId);
        }

        @Override
        public Path nodeShard(int graphId, int typeId, int part) {
            return map(delegate.nodeShard(graphId, typeId, part));
        }

        @Override
        public Path edgeShard(int graphId, int typeId, int part) {
            return map(delegate.edgeShard(graphId, typeId, part));
        }

        @Override
        public Path indexShard(int indexId, int part) {
            return map(delegate.indexShard(indexId, part));
        }

        @Override
        public int indexPart(byte[] key, int partCount) {
            return delegate.indexPart(key, partCount);
        }
    }
}
