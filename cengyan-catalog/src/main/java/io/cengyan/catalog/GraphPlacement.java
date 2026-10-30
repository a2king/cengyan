package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.DataPlacement;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphPaths;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ShardedGraphStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 把图数据放到 {@code <data>/<图名>/} 下的点、边、索引和元数据目录。
 */
public final class GraphPlacement implements DataPlacement {
    private final ShardedGraphStore store;
    private final Path dataDir;
    private final ConcurrentHashMap<Integer, String> graphNames = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> typeNames = new ConcurrentHashMap<>();
    private volatile Catalog catalog;

    public GraphPlacement(ShardedGraphStore store) {
        if (store == null) {
            throw new IllegalArgumentException("存储不能为空");
        }
        this.store = store;
        this.dataDir = store.directory();
    }

    public void bind(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void rememberGraph(int graphId, String name) {
        if (name != null && !name.isBlank()) {
            graphNames.put(graphId, name);
        }
    }

    @Override
    public String graphName(int graphId) {
        String remembered = graphNames.get(graphId);
        if (remembered != null) {
            return remembered;
        }
        Catalog current = catalog;
        if (current != null) {
            String loaded = current.graphNameOrNull(graphId);
            if (loaded != null) {
                graphNames.put(graphId, loaded);
                return loaded;
            }
        }
        byte[] raw = store.readAt(GraphPaths.system(dataDir), Cf.CATALOG, KeyCodec.graphRecordKey(graphId));
        if (raw == null) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + graphId);
        }
        String loaded = CatalogCodec.graphRecordName(raw);
        graphNames.put(graphId, loaded);
        return loaded;
    }

    @Override
    public int partCount(int graphId) {
        Catalog current = catalog;
        if (current == null) {
            return 1;
        }
        return Math.max(1, current.partCount(graphId));
    }

    @Override
    public Set<Integer> nodeTypeIds(int graphId) {
        Catalog current = catalog;
        return current == null ? Set.of() : current.nodeTypeIds(graphId);
    }

    @Override
    public Set<Integer> edgeTypeIds(int graphId) {
        Catalog current = catalog;
        return current == null ? Set.of() : current.relTypeIds(graphId);
    }

    @Override
    public List<Integer> indexIds(int graphId) {
        Catalog current = catalog;
        if (current == null) {
            return List.of();
        }
        List<Integer> ids = new ArrayList<>();
        for (IndexDef index : current.indexesOf(graphId)) {
            ids.add(index.id());
        }
        return ids;
    }

    @Override
    public Path nodeShard(int graphId, int typeId, int part) {
        return GraphPaths.node(dataDir, graphName(graphId), typeName(graphId, Catalog.NODE_KIND, typeId), part);
    }

    @Override
    public Path edgeShard(int graphId, int typeId, int part) {
        return GraphPaths.edge(dataDir, graphName(graphId), typeName(graphId, Catalog.REL_KIND, typeId), part);
    }

    @Override
    public Path indexShard(int indexId, int part) {
        return indexPath(requireIndex(indexId), part);
    }

    @Override
    public int indexPart(byte[] key, int partCount) {
        Catalog.IndexLocation location = requireIndex(uint32(key, 0));
        if (location.index().node()) {
            long vid = KeyCodec.identityVid(key, key.length - KeyCodec.VERTEX_IDENTITY_LENGTH);
            return KeyCodec.partOf(vid, partCount);
        }
        EdgeRef edge = KeyCodec.identityEdge(key, key.length - KeyCodec.EDGE_IDENTITY_LENGTH);
        return KeyCodec.partOf(edge.outSrc(), partCount);
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

    @Override
    public List<Path> placePrefix(Cf family, byte[] prefix) {
        return switch (family) {
            case VERTEX -> vertexPrefix(prefix);
            case EDGE, TXN -> edgePrefix(prefix);
            case PROP -> propPrefix(prefix);
            case INDEX -> indexPrefix(prefix);
            case CATALOG -> catalogPrefix(prefix);
            case DEFAULT -> List.of(GraphPaths.system(dataDir));
        };
    }

    private Path placeVertex(byte[] key) {
        if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
            throw new CengyanException(ErrorCode.INTERNAL, "点键长度不正确");
        }
        int graphId = uint16(key, 0);
        return GraphPaths.node(
                dataDir, graphName(graphId), typeName(graphId, Catalog.NODE_KIND, KeyCodec.tagIdOf(key)), uint16(key, 2));
    }

    private Path placeEdge(byte[] key) {
        KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
        return GraphPaths.edge(
                dataDir,
                graphName(edge.graphId()),
                typeName(edge.graphId(), Catalog.REL_KIND, edge.edgeTypeId()),
                edge.partId());
    }

    private Path placeProp(byte[] key) {
        int graphId = uint16(key, 1);
        int part = catalog == null ? 0 : catalog.partOf(graphId, KeyCodec.propSrc(key));
        return GraphPaths.edge(
                dataDir, graphName(graphId), typeName(graphId, Catalog.REL_KIND, KeyCodec.propEdgeType(key)), part);
    }

    private Path placeTxn(byte[] key) {
        return GraphPaths.edge(
                dataDir,
                graphName(KeyCodec.txnGraphId(key)),
                typeName(KeyCodec.txnGraphId(key), Catalog.REL_KIND, KeyCodec.txnEdgeTypeId(key)),
                KeyCodec.txnPartId(key));
    }

    private Path placeIndex(byte[] key) {
        Catalog.IndexLocation location = requireIndex(uint32(key, 0));
        return indexPath(location, indexPart(key, partCount(location.graphId())));
    }

    private Path placeCatalog(byte[] key) {
        if (key.length == 0) {
            return GraphPaths.system(dataDir);
        }
        int mark = key[0] & 0xFF;
        if (!graphCatalog(mark)) {
            return GraphPaths.system(dataDir);
        }
        return GraphPaths.meta(dataDir, graphName(catalogGraphId(key)));
    }

    private List<Path> vertexPrefix(byte[] prefix) {
        if (prefix.length >= KeyCodec.VERTEX_KEY_LENGTH) {
            return List.of(placeVertex(prefix));
        }
        if (prefix.length >= 4) {
            return nodeShards(uint16(prefix, 0), uint16(prefix, 2));
        }
        if (prefix.length >= 2) {
            return nodeShards(uint16(prefix, 0), -1);
        }
        return List.of();
    }

    private List<Path> edgePrefix(byte[] prefix) {
        if (prefix.length >= KeyCodec.EDGE_KEY_LENGTH) {
            return List.of(placeEdge(prefix));
        }
        if (prefix.length == 32) {
            return List.of(placeTxn(prefix));
        }
        if (prefix.length >= 4) {
            return edgeShards(uint16(prefix, 0), uint16(prefix, 2));
        }
        if (prefix.length >= 2) {
            return edgeShards(uint16(prefix, 0), -1);
        }
        return List.of();
    }

    private List<Path> propPrefix(byte[] prefix) {
        if (prefix.length >= KeyCodec.PROP_KEY_LENGTH) {
            return List.of(placeProp(prefix));
        }
        if (prefix.length >= 3) {
            return edgeShards(uint16(prefix, 1), -1);
        }
        return List.of();
    }

    private List<Path> indexPrefix(byte[] prefix) {
        if (prefix.length >= 4 && catalog != null) {
            Catalog.IndexLocation location = catalog.locateIndex(uint32(prefix, 0));
            if (location != null) {
                int count = partCount(location.graphId());
                List<Path> paths = new ArrayList<>(count);
                for (int part = 0; part < count; part++) {
                    paths.add(indexPath(location, part));
                }
                return paths;
            }
        }
        return existingShards(dataDir, "index", -1);
    }

    private List<Path> catalogPrefix(byte[] prefix) {
        if (prefix.length == 0) {
            return List.of(GraphPaths.system(dataDir));
        }
        int mark = prefix[0] & 0xFF;
        if (!graphCatalog(mark)) {
            return List.of(GraphPaths.system(dataDir));
        }
        if (mark == 0x60 && prefix.length >= 4) {
            return List.of(GraphPaths.meta(dataDir, graphName(uint16(prefix, 2))));
        }
        if (mark != 0x60 && prefix.length >= 3) {
            return List.of(GraphPaths.meta(dataDir, graphName(uint16(prefix, 1))));
        }
        return allMetas();
    }

    private List<Path> nodeShards(int graphId, int onlyPart) {
        LinkedHashSet<Path> paths = new LinkedHashSet<>();
        String name = graphName(graphId);
        collectShards(GraphPaths.graph(dataDir, name).resolve("data").resolve("node"), onlyPart, paths);
        Catalog current = catalog;
        if (current != null) {
            int from = onlyPart >= 0 ? onlyPart : 0;
            int to = onlyPart >= 0 ? onlyPart + 1 : partCount(graphId);
            for (int typeId : current.nodeTypeIds(graphId)) {
                String type = typeName(graphId, Catalog.NODE_KIND, typeId);
                for (int part = from; part < to; part++) {
                    paths.add(GraphPaths.node(dataDir, name, type, part).toAbsolutePath().normalize());
                }
            }
        }
        return List.copyOf(paths);
    }

    private List<Path> edgeShards(int graphId, int onlyPart) {
        LinkedHashSet<Path> paths = new LinkedHashSet<>();
        String name = graphName(graphId);
        collectShards(GraphPaths.graph(dataDir, name).resolve("data").resolve("edge"), onlyPart, paths);
        Catalog current = catalog;
        if (current != null) {
            int from = onlyPart >= 0 ? onlyPart : 0;
            int to = onlyPart >= 0 ? onlyPart + 1 : partCount(graphId);
            for (int typeId : current.relTypeIds(graphId)) {
                String type = typeName(graphId, Catalog.REL_KIND, typeId);
                for (int part = from; part < to; part++) {
                    paths.add(GraphPaths.edge(dataDir, name, type, part).toAbsolutePath().normalize());
                }
            }
        }
        return List.copyOf(paths);
    }

    private List<Path> existingShards(Path root, String kind, int onlyPart) {
        LinkedHashSet<Path> paths = new LinkedHashSet<>();
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (var graphs = Files.list(root)) {
            for (Path graph : graphs.toList()) {
                if (!Files.isDirectory(graph) || GraphPaths.SYSTEM_DIRECTORY.equals(fileName(graph))) {
                    continue;
                }
                collectShards(graph.resolve(kind), onlyPart, paths);
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法列出索引目录：" + root, ex);
        }
        return List.copyOf(paths);
    }

    private List<Path> allMetas() {
        LinkedHashSet<Path> paths = new LinkedHashSet<>();
        if (Files.isDirectory(dataDir)) {
            try (var children = Files.list(dataDir)) {
                for (Path child : children.toList()) {
                    if (!Files.isDirectory(child) || GraphPaths.SYSTEM_DIRECTORY.equals(fileName(child))) {
                        continue;
                    }
                    Path meta = child.resolve("meta");
                    if (Files.isDirectory(meta)) {
                        paths.add(meta.toAbsolutePath().normalize());
                    }
                }
            } catch (IOException ex) {
                throw new CengyanException(ErrorCode.INTERNAL, "无法列出图目录：" + ex.getMessage(), ex);
            }
        }
        for (String name : graphNames.values()) {
            paths.add(GraphPaths.meta(dataDir, name).toAbsolutePath().normalize());
        }
        return List.copyOf(paths);
    }

    private static void collectShards(Path root, int onlyPart, Set<Path> into) {
        if (!Files.isDirectory(root)) {
            return;
        }
        try (var types = Files.list(root)) {
            for (Path typeDir : types.toList()) {
                if (!Files.isDirectory(typeDir)) {
                    continue;
                }
                try (var parts = Files.list(typeDir)) {
                    for (Path partDir : parts.toList()) {
                        if (!Files.isDirectory(partDir)) {
                            continue;
                        }
                        if (onlyPart >= 0 && !Integer.toString(onlyPart).equals(fileName(partDir))) {
                            continue;
                        }
                        into.add(partDir.toAbsolutePath().normalize());
                    }
                }
            }
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "无法列出分片目录：" + root, ex);
        }
    }

    private Path indexPath(Catalog.IndexLocation location, int part) {
        String owner = location.index().node()
                ? typeName(location.graphId(), Catalog.NODE_KIND, location.index().typeId())
                : typeName(location.graphId(), Catalog.REL_KIND, location.index().typeId());
        return GraphPaths.index(
                dataDir, graphName(location.graphId()), GraphPaths.indexFolder(owner, location.index().properties()), part);
    }

    private Catalog.IndexLocation requireIndex(int indexId) {
        Catalog current = catalog;
        Catalog.IndexLocation location = current == null ? null : current.locateIndex(indexId);
        if (location == null) {
            throw new CengyanException(ErrorCode.INTERNAL, "索引丢失：" + indexId);
        }
        return location;
    }

    private String typeName(int graphId, int kind, int typeId) {
        long cacheKey = ((long) graphId << 40) | ((long) kind << 32) | (typeId & 0xffffffffL);
        String cached = typeNames.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        Catalog current = catalog;
        if (current != null) {
            String loaded = kind == Catalog.NODE_KIND
                    ? current.nodeTypeNameOrNull(graphId, typeId)
                    : current.relTypeNameOrNull(graphId, typeId);
            if (loaded != null) {
                typeNames.put(cacheKey, loaded);
                return loaded;
            }
        }
        byte[] raw = store.readAt(
                GraphPaths.meta(dataDir, graphName(graphId)),
                Cf.CATALOG,
                KeyCodec.typeKey(graphId, kind, typeId));
        if (raw == null) {
            String what = kind == Catalog.NODE_KIND ? "节点类型丢失：" : "边类型丢失：";
            throw new CengyanException(ErrorCode.INTERNAL, what + typeId);
        }
        String loaded = CatalogCodec.decodeType(raw).name();
        typeNames.put(cacheKey, loaded);
        return loaded;
    }

    private static int catalogGraphId(byte[] key) {
        int mark = key[0] & 0xFF;
        if (mark == 0x60) {
            if (key.length < 4) {
                throw new CengyanException(ErrorCode.INTERNAL, "目录键太短");
            }
            return uint16(key, 2);
        }
        if (key.length < 3) {
            throw new CengyanException(ErrorCode.INTERNAL, "目录键太短");
        }
        return uint16(key, 1);
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
}
