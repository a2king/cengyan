package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 图、别名、节点类型和边类型。名字表在打开时读入内存，只有写入成功后才更新。
 * 创建名字在写锁里先读再写，避免两个会话抢到同一个名字。
 */
public final class Catalog {
    public static final int NODE_KIND = 1;
    public static final int REL_KIND = 2;

    private static final Logger log = LoggerFactory.getLogger(Catalog.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");
    private static final byte[] EMPTY = new byte[0];

    private final GraphStore store;
    private final ConcurrentHashMap<String, NameEntry> names = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> canonicals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, NodeType>> nodeTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, RelType>> relTypes = new ConcurrentHashMap<>();

    private Catalog(GraphStore store) {
        this.store = store;
    }

    public static Catalog load(GraphStore store) {
        if (store == null) {
            throw new IllegalArgumentException("存储不能为空");
        }
        Catalog catalog = new Catalog(store);
        catalog.reload();
        return catalog;
    }

    /** 只查一次名字主键。图名和别名都返回同一个 graphId，再读规范名。 */
    public GraphName graph(String name) {
        checkIdentifier(name);
        try (ReadSession session = store.read()) {
            byte[] value = session.get(Cf.CATALOG, KeyCodec.nameKey(name));
            if (value == null) {
                throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + name);
            }
            int kind = CatalogCodec.nameKind(value);
            int graphId = CatalogCodec.nameGraphId(value);
            if (kind != GraphName.GRAPH && kind != GraphName.ALIAS) {
                throw new CengyanException(ErrorCode.INTERNAL, "名字种类无效：" + name);
            }
            byte[] record = session.get(Cf.CATALOG, KeyCodec.graphRecordKey(graphId));
            if (record == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "图记录丢失：" + graphId);
            }
            return new GraphName(graphId, CatalogCodec.graphRecordName(record), name, kind);
        }
    }

    public List<String> graphs() {
        return names.values().stream()
                .filter(entry -> entry.kind == GraphName.GRAPH)
                .map(NameEntry::canonical)
                .distinct()
                .sorted()
                .toList();
    }

    public List<AliasRow> aliases() {
        return names.entrySet().stream()
                .filter(entry -> entry.getValue().kind == GraphName.ALIAS)
                .map(entry -> new AliasRow(entry.getKey(), entry.getValue().canonical, entry.getValue().graphId))
                .sorted(Comparator.comparing(AliasRow::alias))
                .toList();
    }

    public int createGraph(String name) {
        checkIdentifier(name);
        return store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                if (session.get(Cf.CATALOG, KeyCodec.nameKey(name)) != null) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "名字已存在：" + name);
                }
                int graphId = allocate(session.get(Cf.CATALOG, KeyCodec.nextGraphIdKey()), false);
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.nameKey(name), CatalogCodec.nameValue(GraphName.GRAPH, graphId));
                batch.put(Cf.CATALOG, KeyCodec.graphRecordKey(graphId), CatalogCodec.graphRecord(name));
                batch.put(Cf.CATALOG, KeyCodec.nextGraphIdKey(), counterAfter(graphId, false));
                store.mutate(batch);
                names.put(name, new NameEntry(GraphName.GRAPH, graphId, name));
                canonicals.put(graphId, name);
                log.info("已创建图 {}，编号 {}", name, graphId);
                return graphId;
            }
        });
    }

    public void createAlias(String alias, String target) {
        checkIdentifier(alias);
        checkIdentifier(target);
        store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                if (session.get(Cf.CATALOG, KeyCodec.nameKey(alias)) != null) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "名字已存在：" + alias);
                }
                byte[] targetValue = session.get(Cf.CATALOG, KeyCodec.nameKey(target));
                if (targetValue == null) {
                    throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + target);
                }
                if (CatalogCodec.nameKind(targetValue) != GraphName.GRAPH) {
                    throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "别名只能指向图，不能指向别名");
                }
                int graphId = CatalogCodec.nameGraphId(targetValue);
                byte[] record = session.get(Cf.CATALOG, KeyCodec.graphRecordKey(graphId));
                if (record == null) {
                    throw new CengyanException(ErrorCode.INTERNAL, "图记录丢失：" + graphId);
                }
                String canonical = CatalogCodec.graphRecordName(record);
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.nameKey(alias), CatalogCodec.nameValue(GraphName.ALIAS, graphId));
                batch.put(Cf.CATALOG, KeyCodec.aliasRecordKey(graphId, alias), EMPTY);
                store.mutate(batch);
                names.put(alias, new NameEntry(GraphName.ALIAS, graphId, canonical));
                log.info("已创建别名 {}，指向 {}", alias, canonical);
                return null;
            }
        });
    }

    public void dropAlias(String alias) {
        checkIdentifier(alias);
        store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                byte[] value = session.get(Cf.CATALOG, KeyCodec.nameKey(alias));
                if (value == null) {
                    throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "别名不存在：" + alias);
                }
                if (CatalogCodec.nameKind(value) != GraphName.ALIAS) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "这个名字是图，不是别名");
                }
                int graphId = CatalogCodec.nameGraphId(value);
                StoreBatch batch = new StoreBatch();
                batch.delete(Cf.CATALOG, KeyCodec.nameKey(alias));
                batch.delete(Cf.CATALOG, KeyCodec.aliasRecordKey(graphId, alias));
                store.mutate(batch);
                names.remove(alias);
                log.info("已删除别名 {}", alias);
                return null;
            }
        });
    }

    public void dropGraph(String name) {
        checkIdentifier(name);
        store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                byte[] value = session.get(Cf.CATALOG, KeyCodec.nameKey(name));
                if (value == null) {
                    throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + name);
                }
                if (CatalogCodec.nameKind(value) != GraphName.GRAPH) {
                    throw new CengyanException(ErrorCode.DROP_ALIAS_AS_GRAPH, "不能用别名删除图");
                }
                int graphId = CatalogCodec.nameGraphId(value);
                StoreBatch batch = new StoreBatch();
                List<String> aliasNames = new ArrayList<>();
                for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.aliasPrefix(graphId))) {
                    aliasNames.add(KeyCodec.nameFromKey(key, 3));
                }
                for (String alias : aliasNames) {
                    batch.delete(Cf.CATALOG, KeyCodec.nameKey(alias));
                }
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.aliasPrefix(graphId));
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.typePrefix(graphId));
                for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.indexRecordPrefix(graphId))) {
                    deletePrefix(batch, session, Cf.INDEX, KeyCodec.indexDataPrefix(KeyCodec.indexIdFromRecord(key)));
                    batch.delete(Cf.CATALOG, key);
                }
                batch.delete(Cf.CATALOG, KeyCodec.nameKey(name));
                batch.delete(Cf.CATALOG, KeyCodec.graphRecordKey(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.nextNodeTypeKey(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.nextRelTypeKey(graphId));
                byte[] dataPrefix = KeyCodec.graphIdPrefix(graphId);
                deletePrefix(batch, session, Cf.VERTEX, dataPrefix);
                deletePrefix(batch, session, Cf.EDGE, dataPrefix);
                deletePrefix(batch, session, Cf.TXN, dataPrefix);
                deletePrefix(batch, session, Cf.PROP, KeyCodec.propGraphPrefix(graphId));
                store.mutate(batch);
                names.entrySet().removeIf(entry -> entry.getValue().graphId == graphId);
                canonicals.remove(graphId);
                nodeTypes.remove(graphId);
                relTypes.remove(graphId);
                log.info("已删除图 {}，编号 {}", name, graphId);
                return null;
            }
        });
    }

    public NodeType createNodeType(int graphId, String name, List<PropDef> properties) {
        return createType(graphId, NODE_KIND, name, properties).asNode();
    }

    public RelType createRelType(int graphId, String name, List<PropDef> properties) {
        return createType(graphId, REL_KIND, name, properties).asRel();
    }

    public NodeType addNodeProperty(int graphId, String typeName, PropDef property) {
        if (property == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        return addProperty(graphId, NODE_KIND, typeName, property.name(), property.type(), property.nullable()).asNode();
    }

    public RelType addRelProperty(int graphId, String typeName, PropDef property) {
        if (property == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        return addProperty(graphId, REL_KIND, typeName, property.name(), property.type(), property.nullable()).asRel();
    }

    public List<NodeType> nodeTypes(int graphId) {
        requireGraph(graphId);
        return nodeTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values().stream()
                .sorted(Comparator.comparing(NodeType::name))
                .toList();
    }

    public List<RelType> relTypes(int graphId) {
        requireGraph(graphId);
        return relTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values().stream()
                .sorted(Comparator.comparing(RelType::name))
                .toList();
    }

    public NodeType nodeType(int graphId, String name) {
        checkIdentifier(name);
        requireGraph(graphId);
        NodeType type = nodeTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).get(name);
        if (type == null) {
            throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "节点类型不存在：" + name);
        }
        return type;
    }

    public RelType relType(int graphId, String name) {
        checkIdentifier(name);
        requireGraph(graphId);
        RelType type = relTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).get(name);
        if (type == null) {
            throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "边类型不存在：" + name);
        }
        return type;
    }

    public boolean insertVertex(
            int graphId, String typeName, long vid, Map<String, Object> properties, boolean ifNotExists) {
        checkIdentifier(typeName);
        return store.withWriteLock(() -> {
            requireGraph(graphId);
            NodeType type = nodeType(graphId, typeName);
            byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), properties);
            int part = KeyCodec.partOf(vid);
            byte[] key = KeyCodec.vertexKey(graphId, part, vid, type.id());
            if (ifNotExists) {
                try (ReadSession session = store.read()) {
                    if (session.get(Cf.VERTEX, key) != null) {
                        return false;
                    }
                }
            }
            StoreBatch batch = new StoreBatch();
            batch.put(Cf.VERTEX, key, encoded);
            store.mutate(batch);
            return true;
        });
    }

    public boolean insertRel(
            int graphId,
            String relType,
            long rank,
            VertexWrite source,
            VertexWrite target,
            Map<String, Object> properties,
            boolean ifNotExists) {
        if (source == null || target == null) {
            throw new IllegalArgumentException("边的两端不能为空");
        }
        checkIdentifier(relType);
        int outPart = KeyCodec.partOf(source.vid());
        int inPart = KeyCodec.partOf(target.vid());
        if (outPart != inPart) {
            throw new CengyanException(ErrorCode.CROSS_PART_UNSUPPORTED, "边的两端落在不同分片");
        }
        return store.withWriteLock(() -> {
            requireGraph(graphId);
            RelType type = relType(graphId, relType);
            byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), properties);
            byte[] propKey = KeyCodec.propKey(graphId, type.id(), rank, source.vid(), target.vid());
            List<VertexPut> vertices = vertexPuts(graphId, source, target);
            if (ifNotExists) {
                try (ReadSession session = store.read()) {
                    if (session.get(Cf.PROP, propKey) != null) {
                        return false;
                    }
                }
            }
            StoreBatch batch = new StoreBatch();
            batch.put(Cf.EDGE, KeyCodec.edgeKey(
                    graphId, outPart, source.vid(), type.id(), Direction.OUT.code(), rank, target.vid()), EMPTY);
            batch.put(Cf.EDGE, KeyCodec.edgeKey(
                    graphId, inPart, target.vid(), type.id(), Direction.IN.code(), rank, source.vid()), EMPTY);
            batch.put(Cf.PROP, propKey, encoded);
            for (VertexPut vertex : vertices) {
                int part = KeyCodec.partOf(vertex.vid());
                batch.put(Cf.VERTEX, KeyCodec.vertexKey(graphId, part, vertex.vid(), vertex.tagId()), vertex.value());
            }
            store.commitEdges(batch);
            return true;
        });
    }

    public Map<String, Object> readVertex(int graphId, String typeName, long vid) {
        NodeType type = nodeType(graphId, typeName);
        byte[] key = KeyCodec.vertexKey(graphId, KeyCodec.partOf(vid), vid, type.id());
        try (ReadSession session = store.read()) {
            byte[] value = session.get(Cf.VERTEX, key);
            if (value == null) {
                return null;
            }
            return PropCodec.decode(type.properties(), type.schemaVersion(), value);
        }
    }

    public Map<String, Object> readRel(int graphId, String relType, long rank, long outSrc, long outDst) {
        RelType type = relType(graphId, relType);
        byte[] key = KeyCodec.propKey(graphId, type.id(), rank, outSrc, outDst);
        try (ReadSession session = store.read()) {
            byte[] value = session.get(Cf.PROP, key);
            if (value == null) {
                return null;
            }
            return PropCodec.decode(type.properties(), type.schemaVersion(), value);
        }
    }

    public List<Map<String, Object>> multiGetProps(int graphId, String relType, List<EdgeRef> edges) {
        if (edges == null) {
            throw new IllegalArgumentException("边列表不能为空");
        }
        RelType type = relType(graphId, relType);
        List<byte[]> keys = new ArrayList<>(edges.size());
        for (EdgeRef edge : edges) {
            if (edge.edgeTypeId() != type.id()) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "边类型和属性类型不一致");
            }
            keys.add(KeyCodec.propKey(graphId, type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        try (ReadSession session = store.read()) {
            List<byte[]> values = session.multiGet(Cf.PROP, keys);
            List<Map<String, Object>> rows = new ArrayList<>(values.size());
            for (byte[] value : values) {
                rows.add(value == null
                        ? null
                        : PropCodec.decode(type.properties(), type.schemaVersion(), value));
            }
            return rows;
        }
    }

    /**
     * 在同一把写锁里修改或删除。点的边数超过上限时整句不提交。
     * 只有边键的批次走成对提交，只改点或属性时走普通写入。
     */
    public int apply(int graphId, List<Mutation> mutations, int maxEdgesPerVertex) {
        if (mutations == null) {
            throw new IllegalArgumentException("修改不能为空");
        }
        if (maxEdgesPerVertex <= 0) {
            throw new IllegalArgumentException("点的边数上限必须为正");
        }
        if (mutations.isEmpty()) {
            return 0;
        }
        return store.withWriteLock(() -> applyLocked(graphId, mutations, maxEdgesPerVertex));
    }

    private int applyLocked(int graphId, List<Mutation> mutations, int maxEdgesPerVertex) {
        requireGraph(graphId);
        StoreBatch batch = new StoreBatch();
        boolean edgeKeys = false;
        int affected = 0;
        try (ReadSession session = store.read()) {
            for (Mutation mutation : mutations) {
                switch (mutation) {
                    case Mutation.SetVertex set -> affected += writeVertex(session, batch, graphId, set);
                    case Mutation.SetEdge set -> affected += writeEdge(session, batch, graphId, set);
                    case Mutation.DeleteEdge delete -> {
                        if (removeEdge(session, batch, graphId, delete)) {
                            affected++;
                            edgeKeys = true;
                        }
                    }
                    case Mutation.DeleteVertex delete -> {
                        int removed = removeVertex(session, batch, graphId, delete.vid(), maxEdgesPerVertex);
                        if (removed > 0) {
                            affected++;
                        }
                        if (removed == 2) {
                            edgeKeys = true;
                        }
                    }
                }
            }
        }
        if (batch.isEmpty()) {
            return 0;
        }
        if (edgeKeys) {
            store.commitEdges(batch);
        } else {
            store.mutate(batch);
        }
        return affected;
    }

    private int writeVertex(ReadSession session, StoreBatch batch, int graphId, Mutation.SetVertex set) {
        NodeType type = nodeType(graphId, set.typeName());
        Map<String, Object> updates = checkedUpdates(type.properties(), set.updates());
        byte[] key = KeyCodec.vertexKey(graphId, KeyCodec.partOf(set.vid()), set.vid(), type.id());
        byte[] raw = session.get(Cf.VERTEX, key);
        if (raw == null) {
            return 0;
        }
        Map<String, Object> merged = new LinkedHashMap<>(PropCodec.decode(type.properties(), type.schemaVersion(), raw));
        merged.putAll(updates);
        batch.put(Cf.VERTEX, key, PropCodec.encode(type.schemaVersion(), type.properties(), merged));
        return 1;
    }

    private int writeEdge(ReadSession session, StoreBatch batch, int graphId, Mutation.SetEdge set) {
        samePart(set.outSrc(), set.outDst());
        RelType type = relType(graphId, set.typeName());
        Map<String, Object> updates = checkedUpdates(type.properties(), set.updates());
        byte[] key = KeyCodec.propKey(graphId, type.id(), set.rank(), set.outSrc(), set.outDst());
        byte[] raw = session.get(Cf.PROP, key);
        if (raw == null) {
            return 0;
        }
        Map<String, Object> merged = new LinkedHashMap<>(PropCodec.decode(type.properties(), type.schemaVersion(), raw));
        merged.putAll(updates);
        batch.put(Cf.PROP, key, PropCodec.encode(type.schemaVersion(), type.properties(), merged));
        return 1;
    }

    private boolean removeEdge(ReadSession session, StoreBatch batch, int graphId, Mutation.DeleteEdge delete) {
        samePart(delete.outSrc(), delete.outDst());
        RelType type = relType(graphId, delete.typeName());
        byte[] prop = KeyCodec.propKey(graphId, type.id(), delete.rank(), delete.outSrc(), delete.outDst());
        byte[] out = edgeKey(graphId, type.id(), Direction.OUT, delete.outSrc(), delete.rank(), delete.outDst());
        byte[] in = edgeKey(graphId, type.id(), Direction.IN, delete.outDst(), delete.rank(), delete.outSrc());
        if (session.get(Cf.PROP, prop) == null && session.get(Cf.EDGE, out) == null) {
            return false;
        }
        batch.delete(Cf.EDGE, out);
        batch.delete(Cf.EDGE, in);
        batch.delete(Cf.PROP, prop);
        return true;
    }

    /** @return 0 没有这个点，1 只删了点，2 还删了边 */
    private int removeVertex(
            ReadSession session, StoreBatch batch, int graphId, long vid, int maxEdgesPerVertex) {
        int part = KeyCodec.partOf(vid);
        byte[] prefix = KeyCodec.vertexPrefix(graphId, part, vid);
        LinkedHashSet<EdgeRef> edges = new LinkedHashSet<>();
        for (byte[] key : session.keysWithPrefix(Cf.EDGE, prefix)) {
            if (key.length != KeyCodec.EDGE_KEY_LENGTH) {
                continue;
            }
            edges.add(KeyCodec.toLogical(key));
            if (edges.size() > maxEdgesPerVertex) {
                throw new CengyanException(ErrorCode.VERTEX_DEGREE_EXCEEDED,
                        "点 " + vid + " 的边数为 " + edges.size() + "，超过上限 " + maxEdgesPerVertex);
            }
        }
        List<byte[]> vertices = session.keysWithPrefix(Cf.VERTEX, prefix);
        if (edges.isEmpty() && vertices.isEmpty()) {
            return 0;
        }
        for (EdgeRef edge : edges) {
            samePart(edge.outSrc(), edge.outDst());
            batch.delete(Cf.EDGE, edgeKey(graphId, edge.edgeTypeId(), Direction.OUT, edge.outSrc(), edge.rank(), edge.outDst()));
            batch.delete(Cf.EDGE, edgeKey(graphId, edge.edgeTypeId(), Direction.IN, edge.outDst(), edge.rank(), edge.outSrc()));
            batch.delete(Cf.PROP, KeyCodec.propKey(graphId, edge.edgeTypeId(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        for (byte[] key : vertices) {
            batch.delete(Cf.VERTEX, key);
        }
        return edges.isEmpty() ? 1 : 2;
    }

    private static Map<String, Object> checkedUpdates(List<PropDef> properties, Map<String, Object> updates) {
        for (String name : updates.keySet()) {
            if (!hasProperty(properties, name)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + name);
            }
        }
        return updates;
    }

    private static boolean hasProperty(List<PropDef> properties, String name) {
        for (PropDef property : properties) {
            if (property.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void samePart(long left, long right) {
        if (KeyCodec.partOf(left) != KeyCodec.partOf(right)) {
            throw new CengyanException(ErrorCode.CROSS_PART_UNSUPPORTED, "边的两端落在不同分片");
        }
    }

    private static byte[] edgeKey(int graphId, int edgeTypeId, Direction direction, long src, long rank, long dst) {
        return KeyCodec.edgeKey(graphId, KeyCodec.partOf(src), src, edgeTypeId, direction.code(), rank, dst);
    }

    private void reload() {
        names.clear();
        canonicals.clear();
        nodeTypes.clear();
        relTypes.clear();
        try (ReadSession session = store.read()) {
            Map<Integer, String> loaded = new HashMap<>();
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.graphRecordPrefix())) {
                int graphId = KeyCodec.graphIdFromMarked(key);
                loaded.put(graphId, CatalogCodec.graphRecordName(session.get(Cf.CATALOG, key)));
            }
            canonicals.putAll(loaded);
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.namePrefix())) {
                String name = KeyCodec.nameFromKey(key, 1);
                byte[] value = session.get(Cf.CATALOG, key);
                int kind = CatalogCodec.nameKind(value);
                int graphId = CatalogCodec.nameGraphId(value);
                String canonical = loaded.get(graphId);
                if (canonical == null) {
                    throw new CengyanException(ErrorCode.INTERNAL, "名字 " + name + " 指向的图记录不存在");
                }
                names.put(name, new NameEntry(kind, graphId, canonical));
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.allTypePrefix())) {
                KeyCodec.TypeKey typeKey = KeyCodec.decodeTypeKey(key);
                CatalogCodec.DecodedType decoded = CatalogCodec.decodeType(session.get(Cf.CATALOG, key));
                remember(typeKey, decoded);
            }
        }
    }

    private StoredType createType(int graphId, int kind, String name, List<PropDef> properties) {
        checkIdentifier(name);
        List<PropDef> copied = copyProperties(properties);
        return store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                requireGraphRecord(session, graphId);
                if (typeNameTaken(session, graphId, name)) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "类型名已存在：" + name);
                }
                byte[] counterKey = kind == NODE_KIND
                        ? KeyCodec.nextNodeTypeKey(graphId)
                        : KeyCodec.nextRelTypeKey(graphId);
                int typeId = allocate(session.get(Cf.CATALOG, counterKey), true);
                CatalogCodec.DecodedType decoded = new CatalogCodec.DecodedType(1, name, copied);
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.typeKey(graphId, kind, typeId),
                        CatalogCodec.typeRecord(1, name, copied));
                batch.put(Cf.CATALOG, counterKey, counterAfter(typeId, true));
                store.mutate(batch);
                StoredType stored = remember(new KeyCodec.TypeKey(graphId, kind, typeId), decoded);
                log.info("已创建类型 {}，编号 {}", name, typeId);
                return stored;
            }
        });
    }

    private StoredType addProperty(
            int graphId, int kind, String typeName, String property, ValueType type, boolean nullable) {
        checkIdentifier(typeName);
        checkIdentifier(property);
        if ("id".equals(property)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "id 不是普通属性");
        }
        if (type == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        if (!nullable) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "只能在末尾增加可空列");
        }
        return store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                requireGraphRecord(session, graphId);
                KeyCodec.TypeKey typeKey = findTypeKey(session, graphId, kind, typeName);
                if (typeKey == null) {
                    throw typeMissing(kind, typeName);
                }
                CatalogCodec.DecodedType current = CatalogCodec.decodeType(
                        session.get(Cf.CATALOG, KeyCodec.typeKey(graphId, kind, typeKey.typeId())));
                for (PropDef existing : current.properties()) {
                    if (existing.name().equals(property)) {
                        throw new CengyanException(ErrorCode.NAME_CONFLICT, "属性名已存在：" + property);
                    }
                }
                if (current.schemaVersion() == Integer.MAX_VALUE) {
                    throw new CengyanException(ErrorCode.INTERNAL, "类型版本已经用完");
                }
                List<PropDef> next = new ArrayList<>(current.properties());
                next.add(new PropDef(property, type, true));
                int schemaVersion = current.schemaVersion() + 1;
                CatalogCodec.DecodedType decoded = new CatalogCodec.DecodedType(schemaVersion, typeName, List.copyOf(next));
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.typeKey(graphId, kind, typeKey.typeId()),
                        CatalogCodec.typeRecord(schemaVersion, typeName, decoded.properties()));
                store.mutate(batch);
                return remember(typeKey, decoded);
            }
        });
    }

    private List<VertexPut> vertexPuts(int graphId, VertexWrite source, VertexWrite target) {
        List<VertexPut> puts = new ArrayList<>(2);
        putVertex(puts, graphId, source);
        putVertex(puts, graphId, target);
        return puts;
    }

    private void putVertex(List<VertexPut> puts, int graphId, VertexWrite vertex) {
        if (vertex.typeName() == null) {
            return;
        }
        NodeType type = nodeType(graphId, vertex.typeName());
        byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), vertex.properties());
        for (VertexPut existing : puts) {
            if (existing.tagId() == type.id() && existing.vid() == vertex.vid()) {
                if (!Arrays.equals(existing.value(), encoded)) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "同一个点写出了两份不同的属性");
                }
                return;
            }
        }
        puts.add(new VertexPut(type.id(), vertex.vid(), encoded));
    }

    private void requireGraph(int graphId) {
        if (!canonicals.containsKey(graphId)) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + graphId);
        }
    }

    private static void requireGraphRecord(ReadSession session, int graphId) {
        if (session.get(Cf.CATALOG, KeyCodec.graphRecordKey(graphId)) == null) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "图不存在：" + graphId);
        }
    }

    private static boolean typeNameTaken(ReadSession session, int graphId, String name) {
        for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.typePrefix(graphId))) {
            CatalogCodec.DecodedType decoded = CatalogCodec.decodeType(session.get(Cf.CATALOG, key));
            if (name.equals(decoded.name())) {
                return true;
            }
        }
        return false;
    }

    private static KeyCodec.TypeKey findTypeKey(ReadSession session, int graphId, int kind, String name) {
        for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.typePrefix(graphId))) {
            KeyCodec.TypeKey typeKey = KeyCodec.decodeTypeKey(key);
            if (typeKey.kind() != kind) {
                continue;
            }
            CatalogCodec.DecodedType decoded = CatalogCodec.decodeType(session.get(Cf.CATALOG, key));
            if (name.equals(decoded.name())) {
                return typeKey;
            }
        }
        return null;
    }

    private StoredType remember(KeyCodec.TypeKey typeKey, CatalogCodec.DecodedType decoded) {
        List<PropDef> properties = List.copyOf(decoded.properties());
        if (typeKey.kind() == NODE_KIND) {
            NodeType type = new NodeType(typeKey.typeId(), decoded.name(), decoded.schemaVersion(), properties);
            nodeTypes.computeIfAbsent(typeKey.graphId(), id -> new ConcurrentHashMap<>()).put(type.name(), type);
            return new StoredType(type, null);
        }
        if (typeKey.kind() == REL_KIND) {
            RelType type = new RelType(typeKey.typeId(), decoded.name(), decoded.schemaVersion(), properties);
            relTypes.computeIfAbsent(typeKey.graphId(), id -> new ConcurrentHashMap<>()).put(type.name(), type);
            return new StoredType(null, type);
        }
        throw new CengyanException(ErrorCode.INTERNAL, "类型种类无效");
    }

    private static void deletePrefix(StoreBatch batch, ReadSession session, Cf family, byte[] prefix) {
        for (byte[] key : session.keysWithPrefix(family, prefix)) {
            batch.delete(family, key);
        }
    }

    private static int allocate(byte[] raw, boolean wide) {
        int next = raw == null
                ? 1
                : (wide ? CatalogCodec.readUint32(raw) : CatalogCodec.readUint16(raw));
        if (next <= 0) {
            throw new CengyanException(ErrorCode.INTERNAL, "编号已经用完");
        }
        return next;
    }

    private static byte[] counterAfter(int used, boolean wide) {
        int following = used == Integer.MAX_VALUE || (!wide && used == 0xFFFF) ? 0 : used + 1;
        return wide ? CatalogCodec.writeUint32(following) : CatalogCodec.writeUint16(following);
    }

    private static List<PropDef> copyProperties(List<PropDef> properties) {
        if (properties == null) {
            throw new IllegalArgumentException("属性列表不能为空");
        }
        List<PropDef> copied = new ArrayList<>(properties.size());
        Map<String, Boolean> seen = new HashMap<>();
        for (PropDef property : properties) {
            if (property == null || property.type() == null) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
            }
            checkIdentifier(property.name());
            if ("id".equals(property.name())) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "id 不是普通属性");
            }
            if (seen.put(property.name(), true) != null) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "属性名重复：" + property.name());
            }
            copied.add(new PropDef(property.name(), property.type(), property.nullable()));
        }
        return List.copyOf(copied);
    }

    static void checkIdentifier(String name) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new CengyanException(ErrorCode.SYNTAX, "名字只能是英文、数字和下划线，并且不能以数字或下划线开头");
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > 128) {
            throw new CengyanException(ErrorCode.SYNTAX, "名字最长 128 字节");
        }
    }

    private static CengyanException typeMissing(int kind, String name) {
        String what = kind == NODE_KIND ? "节点类型不存在：" : "边类型不存在：";
        return new CengyanException(ErrorCode.TYPE_NOT_FOUND, what + name);
    }

    private record NameEntry(int kind, int graphId, String canonical) {
    }

    private record VertexPut(int tagId, long vid, byte[] value) {
    }

    private record StoredType(NodeType node, RelType rel) {
        NodeType asNode() {
            return node;
        }

        RelType asRel() {
            return rel;
        }
    }
}
