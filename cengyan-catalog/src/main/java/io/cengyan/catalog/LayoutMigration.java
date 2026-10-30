package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.DataPlacement;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphPaths;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.ShardedGraphStore;
import io.cengyan.store.StoreBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 把原先整库或 {@code <图名>/part-<分片>/db} 里的数据拆到按图、类型、分片分开的目录。
 * 复制完成并确认写入后，才删除旧库文件。
 */
public final class LayoutMigration implements DataPlacement {
    private static final Logger log = LoggerFactory.getLogger(LayoutMigration.class);
    private static final byte[][] CATALOG_PREFIXES = {
            {0x01}, {0x10}, {0x11}, {0x12}, {0x20}, {0x21}, {0x22},
            {0x30}, {0x31}, {0x40}, {0x50}, {0x51}, {0x52}, {0x53}, {0x60}
    };

    private final Path dataDir;
    private final ShardedGraphStore store;
    private final Path legacy;
    private final Map<Integer, String> graphs = new HashMap<>();
    private final Map<Integer, Integer> parts = new HashMap<>();
    private final Map<Integer, Map<Integer, TypeInfo>> nodeTypes = new HashMap<>();
    private final Map<Integer, Map<Integer, TypeInfo>> relTypes = new HashMap<>();
    private final Map<Integer, IndexInfo> indexes = new HashMap<>();
    private StoreBatch batch = new StoreBatch();

    private LayoutMigration(Path dataDir, ShardedGraphStore store, Path legacy) {
        this.dataDir = dataDir;
        this.store = store;
        this.legacy = legacy;
    }

    public static void migrate(Path configured, ShardedGraphStore store) {
        if (store == null) {
            return;
        }
        Path dataDir = store.directory();
        Path legacy = locate(configured, dataDir);
        if (legacy == null) {
            return;
        }
        LayoutMigration migration = new LayoutMigration(dataDir, store, legacy);
        migration.run();
    }

    private void run() {
        log.info("开始把旧库 {} 拆到按图分开的目录", legacy);
        try (GraphStore legacyStore = GraphStore.open(legacy)) {
            loadMaps(legacyStore);
            store.setDataPlacement(this);
            copyCatalogAndData(legacyStore);
            copyPartDatabases();
            flush();
        }
        cleanup();
        log.info("旧库 {} 已拆到各图目录", legacy);
    }

    private static Path locate(Path configured, Path dataDir) {
        Path system = GraphPaths.system(dataDir).toAbsolutePath().normalize();
        Path[] candidates = {
                configured,
                configured == null ? null : configured.resolve("data"),
                dataDir,
                dataDir.resolve("data")
        };
        for (Path candidate : candidates) {
            if (candidate == null || !isDb(candidate)) {
                continue;
            }
            Path absolute = candidate.toAbsolutePath().normalize();
            if (!absolute.equals(system)) {
                return absolute;
            }
        }
        return null;
    }

    private static boolean isDb(Path dir) {
        return Files.isRegularFile(dir.resolve("CURRENT"));
    }

    private void loadMaps(GraphStore legacyStore) {
        try (ReadSession session = legacyStore.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.graphRecordPrefix())) {
                int graphId = KeyCodec.graphIdFromMarked(key);
                graphs.put(graphId, CatalogCodec.graphRecordName(session.get(Cf.CATALOG, key)));
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.allTypePrefix())) {
                if (key.length != 8) {
                    continue;
                }
                KeyCodec.TypeKey typeKey = KeyCodec.decodeTypeKey(key);
                CatalogCodec.DecodedType decoded = CatalogCodec.decodeType(session.get(Cf.CATALOG, key));
                TypeInfo info = new TypeInfo(decoded.name(), decoded.properties());
                Map<Integer, Map<Integer, TypeInfo>> target =
                        typeKey.kind() == Catalog.NODE_KIND ? nodeTypes : relTypes;
                target.computeIfAbsent(typeKey.graphId(), ignored -> new HashMap<>()).put(typeKey.typeId(), info);
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.spacePrefix())) {
                if (key.length != 3) {
                    continue;
                }
                byte[] value = session.get(Cf.CATALOG, key);
                if (value != null) {
                    parts.put(KeyCodec.graphIdFromMarked(key), ClusterCodec.spaceParts(value));
                }
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.allIndexRecordPrefix())) {
                if (key.length != 7) {
                    continue;
                }
                int graphId = KeyCodec.graphIdFromMarked(key);
                int indexId = KeyCodec.indexIdFromRecord(key);
                IndexCodec.Decoded decoded = IndexCodec.decode(session.get(Cf.CATALOG, key));
                TypeInfo type = (decoded.kind() == Catalog.NODE_KIND ? nodeTypes : relTypes)
                        .getOrDefault(graphId, Map.of())
                        .get(decoded.typeId());
                if (type == null || graphs.get(graphId) == null) {
                    continue;
                }
                List<String> fields = new ArrayList<>();
                for (int position : decoded.propertyIndexes()) {
                    fields.add(type.properties().get(position).name());
                }
                indexes.put(indexId, new IndexInfo(
                        graphs.get(graphId),
                        decoded.kind() == Catalog.NODE_KIND,
                        GraphPaths.indexFolder(type.name(), fields),
                        Math.max(1, parts.getOrDefault(graphId, 1))));
            }
        }
    }

    private void copyCatalogAndData(GraphStore source) {
        for (byte[] prefix : CATALOG_PREFIXES) {
            copyPrefix(source, Cf.CATALOG, prefix);
        }
        copyData(source);
        for (int indexId : indexes.keySet()) {
            copyPrefix(source, Cf.INDEX, KeyCodec.indexDataPrefix(indexId));
        }
    }

    private void copyData(GraphStore source) {
        for (int graphId : graphs.keySet()) {
            copyPrefix(source, Cf.VERTEX, KeyCodec.graphIdPrefix(graphId));
            copyPrefix(source, Cf.EDGE, KeyCodec.graphIdPrefix(graphId));
            copyPrefix(source, Cf.TXN, KeyCodec.graphIdPrefix(graphId));
            copyPrefix(source, Cf.PROP, KeyCodec.propGraphPrefix(graphId));
        }
    }

    private void copyPartDatabases() {
        if (!Files.isDirectory(legacy)) {
            return;
        }
        try (var children = Files.list(legacy)) {
            for (Path child : children.toList()) {
                if (!Files.isDirectory(child) || GraphPaths.SYSTEM_DIRECTORY.equals(fileName(child))) {
                    continue;
                }
                try (var kids = Files.list(child)) {
                    for (Path kid : kids.toList()) {
                        if (!fileName(kid).startsWith("part-")) {
                            continue;
                        }
                        Path db = kid.resolve("db");
                        if (!isDb(db)) {
                            continue;
                        }
                        try (GraphStore part = GraphStore.open(db)) {
                            copyData(part);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法读取旧分片目录：" + ex.getMessage(), ex);
        }
    }

    private void copyPrefix(GraphStore source, Cf family, byte[] prefix) {
        try (ReadSession session = source.read()) {
            session.visitPrefix(family, prefix, null, key -> {
                byte[] value = session.get(family, key);
                if (value != null) {
                    add(family, key, value);
                }
            });
        }
    }

    private void add(Cf family, byte[] key, byte[] value) {
        batch.put(family, key, value);
        if (batch.operations().size() >= 512) {
            flush();
        }
    }

    private void flush() {
        if (!batch.isEmpty()) {
            store.mutate(batch);
            batch = new StoreBatch();
        }
    }

    @Override
    public Path place(Cf family, byte[] key) {
        return switch (family) {
            case VERTEX -> placeVertex(key);
            case EDGE -> placeEdge(key);
            case PROP -> placeProp(key);
            case TXN -> placeTxn(key);
            case INDEX -> placeIndex(key);
            case CATALOG -> placeCatalog(key);
            case DEFAULT -> GraphPaths.system(dataDir);
        };
    }

    private Path placeVertex(byte[] key) {
        int graphId = uint16(key, 0);
        TypeInfo type = nodeTypes.getOrDefault(graphId, Map.of()).get(KeyCodec.tagIdOf(key));
        if (type == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "迁移时找不到节点类型：" + KeyCodec.tagIdOf(key));
        }
        return GraphPaths.node(dataDir, graphName(graphId), type.name(), uint16(key, 2));
    }

    private Path placeEdge(byte[] key) {
        KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
        TypeInfo type = relTypes.getOrDefault(edge.graphId(), Map.of()).get(edge.edgeTypeId());
        if (type == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "迁移时找不到边类型：" + edge.edgeTypeId());
        }
        return GraphPaths.edge(dataDir, graphName(edge.graphId()), type.name(), edge.partId());
    }

    private Path placeProp(byte[] key) {
        int graphId = uint16(key, 1);
        int typeId = KeyCodec.propEdgeType(key);
        TypeInfo type = relTypes.getOrDefault(graphId, Map.of()).get(typeId);
        if (type == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "迁移时找不到边类型：" + typeId);
        }
        int part = KeyCodec.partOf(KeyCodec.propSrc(key), Math.max(1, parts.getOrDefault(graphId, 1)));
        return GraphPaths.edge(dataDir, graphName(graphId), type.name(), part);
    }

    private Path placeTxn(byte[] key) {
        int graphId = KeyCodec.txnGraphId(key);
        TypeInfo type = relTypes.getOrDefault(graphId, Map.of()).get(KeyCodec.txnEdgeTypeId(key));
        if (type == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "迁移时找不到边类型：" + KeyCodec.txnEdgeTypeId(key));
        }
        return GraphPaths.edge(dataDir, graphName(graphId), type.name(), KeyCodec.txnPartId(key));
    }

    private Path placeIndex(byte[] key) {
        IndexInfo info = indexes.get(uint32(key, 0));
        if (info == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "迁移时找不到索引：" + uint32(key, 0));
        }
        int part;
        if (info.node()) {
            long vid = KeyCodec.identityVid(key, key.length - KeyCodec.VERTEX_IDENTITY_LENGTH);
            part = KeyCodec.partOf(vid, info.parts());
        } else {
            EdgeRef edge = KeyCodec.identityEdge(key, key.length - KeyCodec.EDGE_IDENTITY_LENGTH);
            part = KeyCodec.partOf(edge.outSrc(), info.parts());
        }
        return GraphPaths.index(dataDir, info.graph(), info.folder(), part);
    }

    private Path placeCatalog(byte[] key) {
        if (key.length == 0) {
            return GraphPaths.system(dataDir);
        }
        int mark = key[0] & 0xFF;
        if (!graphCatalog(mark)) {
            return GraphPaths.system(dataDir);
        }
        int graphId = mark == 0x60 ? uint16(key, 2) : uint16(key, 1);
        return GraphPaths.meta(dataDir, graphName(graphId));
    }

    @Override
    public String graphName(int graphId) {
        String name = graphs.get(graphId);
        if (name == null) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "迁移时找不到图：" + graphId);
        }
        return name;
    }

    private void cleanup() {
        deleteRocksFiles(legacy);
        if (!Files.isDirectory(legacy)) {
            return;
        }
        try (var children = Files.list(legacy)) {
            for (Path child : children.toList()) {
                if (!Files.isDirectory(child) || GraphPaths.SYSTEM_DIRECTORY.equals(fileName(child))) {
                    continue;
                }
                try (var kids = Files.list(child)) {
                    for (Path kid : kids.toList()) {
                        if (fileName(kid).startsWith("part-")) {
                            deleteTree(kid);
                        }
                    }
                }
                deleteIfEmpty(child);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法清理旧库：" + ex.getMessage(), ex);
        }
        if (!legacy.equals(dataDir.toAbsolutePath().normalize())) {
            deleteIfEmpty(legacy);
        }
    }

    private static void deleteRocksFiles(Path dir) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var files = Files.list(dir)) {
            for (Path file : files.toList()) {
                if (Files.isRegularFile(file) && isRocksFile(fileName(file))) {
                    Files.deleteIfExists(file);
                }
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法删除旧库文件：" + dir, ex);
        }
    }

    private static boolean isRocksFile(String name) {
        return "CURRENT".equals(name)
                || "IDENTITY".equals(name)
                || "LOCK".equals(name)
                || "LOG".equals(name)
                || name.startsWith("LOG.old")
                || name.startsWith("MANIFEST-")
                || name.startsWith("OPTIONS-")
                || name.endsWith(".sst")
                || name.endsWith(".log");
    }

    private static void deleteIfEmpty(Path dir) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var kids = Files.list(dir)) {
            if (kids.findAny().isEmpty()) {
                Files.deleteIfExists(dir);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法删除空目录：" + dir, ex);
        }
    }

    private static void deleteTree(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            for (Path item : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(item);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法删除旧分片：" + path, ex);
        }
    }

    private static boolean graphCatalog(int mark) {
        return mark == 0x12 || mark == 0x20 || mark == 0x21 || mark == 0x22
                || mark == 0x30 || mark == 0x51 || mark == 0x52 || mark == 0x53 || mark == 0x60;
    }

    private static int uint16(byte[] key, int offset) {
        return ((key[offset] & 0xFF) << 8) | (key[offset + 1] & 0xFF);
    }

    private static int uint32(byte[] key, int offset) {
        return ((key[offset] & 0xFF) << 24)
                | ((key[offset + 1] & 0xFF) << 16)
                | ((key[offset + 2] & 0xFF) << 8)
                | (key[offset + 3] & 0xFF);
    }

    private static String fileName(Path path) {
        Path name = path.getFileName();
        return name == null ? "" : name.toString();
    }

    private record TypeInfo(String name, List<io.cengyan.catalog.PropDef> properties) {
    }

    private record IndexInfo(String graph, boolean node, String folder, int parts) {
    }
}
