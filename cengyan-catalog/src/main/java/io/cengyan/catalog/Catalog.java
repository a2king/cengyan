package io.cengyan.catalog;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphPaths;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ShardedGraphStore;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 图、别名、节点类型和边类型。名字表在打开时读入内存，只有写入成功后才更新。
 * 创建名字在写锁里先读再写，避免两个会话抢到同一个名字。
 */
public final class Catalog {
    public static final int NODE_KIND = 1;
    public static final int REL_KIND = 2;
    /** 查询未注册的字符串 ID 时使用的内部哨兵；字符串映射从 MIN_VALUE + 1 开始。 */
    public static final long MISSING_VERTEX_ID = Long.MIN_VALUE;

    private static final Logger log = LoggerFactory.getLogger(Catalog.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");
    private static final byte[] EMPTY = new byte[0];

    private final GraphStore store;
    private final ConcurrentHashMap<String, NameEntry> names = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> canonicals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, NodeType>> nodeTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, RelType>> relTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, IndexDef>> indexes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Space> spaces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Set<Integer>> moving = new ConcurrentHashMap<>();

    private Catalog(GraphStore store) {
        this.store = store;
    }

    public static Catalog load(GraphStore store) {
        if (store == null) {
            throw new IllegalArgumentException("存储不能为空");
        }
        Catalog catalog = new Catalog(store);
        if (store instanceof ShardedGraphStore sharded) {
            GraphPlacement placement = new GraphPlacement(sharded);
            placement.bind(catalog);
            sharded.setDataPlacement(placement);
        }
        catalog.reload();
        catalog.healIncomplete();
        catalog.recoverTxn();
        store.setPartResolver(catalog::partOf);
        store.setPartCountResolver(catalog::partCount);
        return catalog;
    }

    /** 分片数为 1 时结果是 0，已有数据不用改键。 */
    public int partOf(int graphId, long vid) {
        return KeyCodec.partOf(vid, partCount(graphId));
    }

    public int partCount(int graphId) {
        Space space = spaces.get(graphId);
        if (space == null || space.parts <= 0) {
            return 1;
        }
        return space.parts;
    }

    /** 把 GQL 中的外部 ID 转成存储层使用的 long。字符串 ID 使用图级目录映射。 */
    public long resolveVertexId(int graphId, NodeType type, Object value) {
        if (type == null) {
            return resolveUnqualifiedVertexId(graphId, value);
        }
        if (type.idType() == IdType.INT) {
            return resolveIntegerId(type, value);
        }
        if (!(value instanceof String text)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是字符串");
        }
        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
        if (text.isEmpty() || bytes > type.idLength()) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串节点 ID 长度必须不超过 " + type.idLength());
        }
        return resolveStringId(graphId, text);
    }

    /** 只查找已经存在的 ID，不会在查询、SET 或 DELETE 中写入字符串映射。 */
    public long findVertexId(int graphId, NodeType type, Object value) {
        if (type == null) {
            return findUnqualifiedVertexId(graphId, value);
        }
        if (type.idType() == IdType.INT) {
            return resolveIntegerId(type, value);
        }
        if (!(value instanceof String text)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是字符串");
        }
        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
        if (text.isEmpty() || bytes > type.idLength()) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串节点 ID 长度必须不超过 " + type.idLength());
        }
        return findStringId(graphId, text);
    }

    /** 未指定节点类型时用于边端点。字符串 ID 必须已经在图中注册。 */
    public long resolveUnqualifiedVertexId(int graphId, Object value) {
        if (value instanceof Number number) {
            return resolveIntegerId(null, number);
        }
        if (!(value instanceof String text) || text.isEmpty()) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是整数或字符串");
        }
        try (ReadSession session = store.read()) {
            byte[] raw = session.get(Cf.CATALOG, KeyCodec.stringIdForwardKey(graphId, text));
            if (raw == null) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串节点 ID 尚未注册：" + text);
            }
            return decodeLong(raw);
        }
    }

    /** 查询结果中的外部 ID。没有节点类型时也能识别已经注册的字符串 ID。 */
    public Object externalVertexId(int graphId, NodeType type, long internalId) {
        if (type != null && type.idType() == IdType.INT) {
            return internalId;
        }
        try (ReadSession session = store.read()) {
            byte[] raw = session.get(Cf.CATALOG, KeyCodec.stringIdReverseKey(graphId, internalId));
            if (raw != null) {
                return new String(raw, StandardCharsets.UTF_8);
            }
        }
        return internalId;
    }

    private long resolveStringId(int graphId, String text) {
        return store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                byte[] forwardKey = KeyCodec.stringIdForwardKey(graphId, text);
                byte[] existing = session.get(Cf.CATALOG, forwardKey);
                if (existing != null) {
                    return decodeLong(existing);
                }
                byte[] counterKey = KeyCodec.stringIdCounterKey(graphId);
                byte[] counter = session.get(Cf.CATALOG, counterKey);
                long sequence = counter == null ? 0L : decodeLong(counter);
                if (sequence >= Long.MAX_VALUE - 1L) {
                    throw new CengyanException(ErrorCode.INTERNAL, "字符串节点 ID 已耗尽");
                }
                long internalId = Long.MIN_VALUE + 1L + sequence;
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, forwardKey, encodeLong(internalId));
                batch.put(Cf.CATALOG, KeyCodec.stringIdReverseKey(graphId, internalId),
                        text.getBytes(StandardCharsets.UTF_8));
                batch.put(Cf.CATALOG, counterKey, encodeLong(sequence + 1));
                store.mutate(batch);
                return internalId;
            }
        });
    }

    private long findUnqualifiedVertexId(int graphId, Object value) {
        if (value instanceof Number number) {
            return resolveIntegerId(null, number);
        }
        if (!(value instanceof String text) || text.isEmpty()) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是整数或字符串");
        }
        return findStringId(graphId, text);
    }

    private long findStringId(int graphId, String text) {
        try (ReadSession session = store.read()) {
            byte[] raw = session.get(Cf.CATALOG, KeyCodec.stringIdForwardKey(graphId, text));
            return raw == null ? MISSING_VERTEX_ID : decodeLong(raw);
        }
    }

    private static long resolveIntegerId(NodeType type, Object value) {
        if (!(value instanceof Number number)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是整数");
        }
        long id = number.longValue();
        if (number instanceof Float || number instanceof Double) {
            double decimal = number.doubleValue();
            if (!Double.isFinite(decimal) || decimal != id) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "整数节点 ID 必须是非负整数");
            }
        }
        if (id < 0 || id == MISSING_VERTEX_ID) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "整数节点 ID 必须是非负整数");
        }
        if (type != null && decimalLength(id) > type.idLength()) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 超过定义的长度 " + type.idLength());
        }
        return id;
    }

    private static int decimalLength(long value) {
        return Long.toString(value).length();
    }

    private static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static long decodeLong(byte[] raw) {
        if (raw == null || raw.length != Long.BYTES) {
            throw new CengyanException(ErrorCode.INTERNAL, "字符串 ID 映射记录损坏");
        }
        return ByteBuffer.wrap(raw).getLong();
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
            return new GraphName(graphId, CatalogCodec.graphRecordName(record), name, kind,
                    CatalogCodec.graphRecordComment(record));
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
        return createGraph(name, 1, null);
    }

    public int createGraph(String name, String comment) {
        return createGraph(name, 32, comment);
    }

    public int createGraph(String name, int parts, String comment) {
        checkIdentifier(name);
        if (GraphPaths.RESERVED_GRAPH_NAME.equals(name)) {
            throw new CengyanException(ErrorCode.SYNTAX, "cache 是系统目录，不能作为图名");
        }
        if (parts <= 0 || parts > 4096) {
            throw new CengyanException(ErrorCode.SYNTAX, "分片数量必须在 1 到 4096 之间");
        }
        return store.withWriteLock(() -> {
            try (ReadSession session = store.read()) {
                if (session.get(Cf.CATALOG, KeyCodec.nameKey(name)) != null) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "名字已存在：" + name);
                }
                int graphId = allocate(session.get(Cf.CATALOG, KeyCodec.nextGraphIdKey()), false);
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.nameKey(name), CatalogCodec.nameValue(GraphName.GRAPH, graphId));
                batch.put(Cf.CATALOG, KeyCodec.graphRecordKey(graphId), CatalogCodec.graphRecord(name, comment));
                batch.put(Cf.CATALOG, KeyCodec.spaceKey(graphId), ClusterCodec.space(ClusterCodec.MODE_NORMAL, 1, parts));
                batch.put(Cf.CATALOG, KeyCodec.nextGraphIdKey(), counterAfter(graphId, false));
                store.noteGraph(graphId, name);
                store.mutate(batch);
                names.put(name, new NameEntry(GraphName.GRAPH, graphId, name));
                canonicals.put(graphId, name);
                spaces.put(graphId, new Space(ClusterCodec.MODE_NORMAL, 1, parts));
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
                requireWritable(graphId);
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
                requireWritable(graphId);
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
                requireWritable(graphId);
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
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.stringIdForwardPrefix(graphId));
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.stringIdReversePrefix(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.stringIdCounterKey(graphId));
                for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.indexRecordPrefix(graphId))) {
                    deletePrefix(batch, session, Cf.INDEX, KeyCodec.indexDataPrefix(KeyCodec.indexIdFromRecord(key)));
                    batch.delete(Cf.CATALOG, key);
                }
                batch.delete(Cf.CATALOG, KeyCodec.nameKey(name));
                batch.delete(Cf.CATALOG, KeyCodec.graphRecordKey(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.nextNodeTypeKey(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.nextRelTypeKey(graphId));
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.partVotePrefix(graphId));
                deletePrefix(batch, session, Cf.CATALOG, KeyCodec.movePrefix(graphId));
                batch.delete(Cf.CATALOG, KeyCodec.spaceKey(graphId));
                byte[] dataPrefix = KeyCodec.graphIdPrefix(graphId);
                deletePrefix(batch, session, Cf.VERTEX, dataPrefix);
                deletePrefix(batch, session, Cf.EDGE, dataPrefix);
                deletePrefix(batch, session, Cf.TXN, dataPrefix);
                deletePrefix(batch, session, Cf.PROP, KeyCodec.propGraphPrefix(graphId));
                store.mutate(batch);
                store.deleteGraphDirectory(name);
                names.entrySet().removeIf(entry -> entry.getValue().graphId == graphId);
                canonicals.remove(graphId);
                nodeTypes.remove(graphId);
                relTypes.remove(graphId);
                indexes.remove(graphId);
                spaces.remove(graphId);
                moving.remove(graphId);
                log.info("已删除图 {}，编号 {}", name, graphId);
                return null;
            }
        });
    }

    public NodeType createNodeType(
            int graphId, String name, IdType idType, int idLength, List<PropDef> properties) {
        return createType(graphId, NODE_KIND, name, properties, idType, idLength).asNode();
    }

    public NodeType createNodeType(int graphId, String name, IdType idType, int idLength,
                                   List<PropDef> properties, String comment) {
        return createType(graphId, NODE_KIND, name, properties, idType, idLength, comment).asNode();
    }

    public NodeType createNodeType(int graphId, String name, List<PropDef> properties) {
        return createNodeType(graphId, name, IdType.INT, 19, properties);
    }

    public RelType createRelType(int graphId, String name, List<PropDef> properties) {
        return createType(graphId, REL_KIND, name, properties, IdType.INT, 19).asRel();
    }

    public RelType createRelType(int graphId, String name, List<PropDef> properties, String comment) {
        return createType(graphId, REL_KIND, name, properties, IdType.INT, 19, comment).asRel();
    }

    public NodeType addNodeProperty(int graphId, String typeName, PropDef property) {
        if (property == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        return addProperty(graphId, NODE_KIND, typeName, property.name(), property.type(), property.nullable(), property.comment()).asNode();
    }

    public RelType addRelProperty(int graphId, String typeName, PropDef property) {
        if (property == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "列类型不能为空");
        }
        return addProperty(graphId, REL_KIND, typeName, property.name(), property.type(), property.nullable(), property.comment()).asRel();
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
        return store.withStripes(List.of(nodeStripe(graphId, typeName)), () -> {
            requireGraph(graphId);
            requireWritable(graphId);
            requirePart(graphId, partOf(graphId, vid));
            NodeType type = nodeType(graphId, typeName);
            byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), properties);
            int part = partOf(graphId, vid);
            byte[] key = KeyCodec.vertexKey(graphId, part, vid, type.id());
            try (ReadSession session = store.read()) {
                byte[] existing = session.get(Cf.VERTEX, key);
                if (ifNotExists && existing != null) {
                    return false;
                }
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.VERTEX, key, encoded);
                IndexMaintenance.sync(
                        session,
                        batch,
                        indexesFor(graphId, true, type.id()),
                        type.properties(),
                        existing == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), existing),
                        PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                        graphId,
                        KeyCodec.vertexIdentity(type.id(), vid));
                store.mutate(batch);
                return true;
            }
        });
    }

    /** 将一批外部节点 ID 映射为存储层 ID。字符串 ID 的目录映射也只提交一次。 */
    public List<Long> resolveBulkVertexIds(int graphId, NodeType type, List<?> values) {
        if (type == null || values == null) {
            throw new IllegalArgumentException("节点类型和 ID 列表不能为空");
        }
        return store.withStripes(List.of(metaStripe(graphId)), () -> {
            List<Long> result = new ArrayList<>(values.size());
            if (type.idType() == IdType.INT) {
                for (Object value : values) {
                    result.add(resolveIntegerId(type, value));
                }
                return result;
            }
            StoreBatch batch = new StoreBatch();
            Map<String, Long> pending = new LinkedHashMap<>();
            try (ReadSession session = store.read()) {
                byte[] counterKey = KeyCodec.stringIdCounterKey(graphId);
                byte[] counter = session.get(Cf.CATALOG, counterKey);
                long sequence = counter == null ? 0L : decodeLong(counter);
                for (Object value : values) {
                    if (!(value instanceof String text)) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是字符串");
                    }
                    int bytes = text.getBytes(StandardCharsets.UTF_8).length;
                    if (text.isEmpty() || bytes > type.idLength()) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH,
                                "字符串节点 ID 长度必须不超过 " + type.idLength());
                    }
                    Long existingPending = pending.get(text);
                    if (existingPending != null) {
                        result.add(existingPending);
                        continue;
                    }
                    byte[] forwardKey = KeyCodec.stringIdForwardKey(graphId, text);
                    byte[] raw = session.get(Cf.CATALOG, forwardKey);
                    long internal;
                    if (raw != null) {
                        internal = decodeLong(raw);
                    } else {
                        if (sequence >= Long.MAX_VALUE - 1L) {
                            throw new CengyanException(ErrorCode.INTERNAL, "字符串节点 ID 已耗尽");
                        }
                        internal = Long.MIN_VALUE + 1L + sequence++;
                        batch.put(Cf.CATALOG, forwardKey, encodeLong(internal));
                        batch.put(Cf.CATALOG, KeyCodec.stringIdReverseKey(graphId, internal),
                                text.getBytes(StandardCharsets.UTF_8));
                    }
                    pending.put(text, internal);
                    result.add(internal);
                }
                long oldSequence = counter == null ? 0L : decodeLong(counter);
                if (sequence != oldSequence) {
                    batch.put(Cf.CATALOG, counterKey, encodeLong(sequence));
                }
                if (!batch.isEmpty()) {
                    store.mutate(batch);
                }
            }
            return result;
        });
    }

    /** 批量查找无类型端点 ID；不会为字符串 ID 创建新的目录映射。 */
    public List<Long> findBulkUnqualifiedVertexIds(int graphId, List<?> values) {
        if (values == null) {
            throw new IllegalArgumentException("ID 列表不能为空");
        }
        List<Long> result = new ArrayList<>(values.size());
        try (ReadSession session = store.read()) {
            for (Object value : values) {
                if (value instanceof Number number) {
                    result.add(resolveIntegerId(null, number));
                    continue;
                }
                if (!(value instanceof String text) || text.isEmpty()) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是整数或字符串");
                }
                byte[] raw = session.get(Cf.CATALOG, KeyCodec.stringIdForwardKey(graphId, text));
                if (raw == null) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "字符串节点 ID 尚未注册：" + text);
                }
                result.add(decodeLong(raw));
            }
        }
        return result;
    }

    /**
     * 批量写入节点。调用方应按网络和内存大小分块。
     * 同一批内重复的 (type, vid) 按最后一行覆盖，唯一索引冲突或任意一行校验失败时整批失败。
     */
    public int bulkInsertVertices(
            int graphId, String typeName, List<BulkVertex> rows, boolean ifNotExists) {
        checkIdentifier(typeName);
        if (rows == null) {
            throw new IllegalArgumentException("批量节点不能为空");
        }
        return store.withStripes(List.of(nodeStripe(graphId, typeName)), () -> {
            requireGraph(graphId);
            requireWritable(graphId);
            NodeType type = nodeType(graphId, typeName);
            StoreBatch batch = new StoreBatch();
            Map<Long, byte[]> previous = new LinkedHashMap<>();
            Map<Long, Map<String, Object>> nextValues = new LinkedHashMap<>();
            try (ReadSession session = store.read()) {
                for (BulkVertex row : rows) {
                    long vid = row.vid();
                    requirePart(graphId, partOf(graphId, vid));
                    byte[] key = KeyCodec.vertexKey(graphId, partOf(graphId, vid), vid, type.id());
                    byte[] old = previous.containsKey(vid) ? previous.get(vid) : session.get(Cf.VERTEX, key);
                    if (ifNotExists && old != null) {
                        continue;
                    }
                    Map<String, Object> next = normalizeProperties(type.properties(), row.properties());
                    byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), next);
                    IndexMaintenance.sync(
                            session,
                            batch,
                            indexesFor(graphId, true, type.id()),
                            type.properties(),
                            old == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), old),
                            PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                            graphId,
                            KeyCodec.vertexIdentity(type.id(), vid));
                    batch.put(Cf.VERTEX, key, encoded);
                    previous.put(vid, encoded);
                    nextValues.put(vid, next);
                }
                if (!batch.isEmpty()) {
                    store.mutate(batch);
                }
            }
            return nextValues.size();
        });
    }

    /**
     * 批量写入带外部 ID 的节点。字符串 ID 映射、节点属性、索引在同一个 WriteBatch 中提交，
     * 因此唯一索引冲突或属性校验失败不会留下半批 ID 映射。
     */
    public int bulkInsertVerticesExternal(
            int graphId, String typeName, List<BulkVertexValue> rows, boolean ifNotExists) {
        checkIdentifier(typeName);
        if (rows == null) {
            throw new IllegalArgumentException("批量节点不能为空");
        }
        NodeType planned = nodeType(graphId, typeName);
        List<String> stripes = new ArrayList<>();
        stripes.add(nodeStripe(graphId, typeName));
        if (planned.idType() == IdType.STRING) {
            stripes.add(metaStripe(graphId));
        }
        return store.withStripes(stripes, () -> {
            requireGraph(graphId);
            requireWritable(graphId);
            NodeType type = nodeType(graphId, typeName);
            StoreBatch batch = new StoreBatch();
            Map<Long, byte[]> previous = new LinkedHashMap<>();
            Map<String, Long> pendingIds = new LinkedHashMap<>();
            long sequence = -1L;
            long originalSequence = -1L;
            int written = 0;
            try (ReadSession session = store.read()) {
                byte[] counterKey = KeyCodec.stringIdCounterKey(graphId);
                if (type.idType() == IdType.STRING) {
                    byte[] counter = session.get(Cf.CATALOG, counterKey);
                    originalSequence = counter == null ? 0L : decodeLong(counter);
                    sequence = originalSequence;
                }
                for (BulkVertexValue row : rows) {
                    long vid;
                    if (type.idType() == IdType.INT) {
                        vid = resolveIntegerId(type, row.id());
                    } else {
                        if (!(row.id() instanceof String text)) {
                            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "节点 ID 必须是字符串");
                        }
                        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
                        if (text.isEmpty() || bytes > type.idLength()) {
                            throw new CengyanException(ErrorCode.TYPE_MISMATCH,
                                    "字符串节点 ID 长度必须不超过 " + type.idLength());
                        }
                        Long pending = pendingIds.get(text);
                        if (pending != null) {
                            vid = pending;
                        } else {
                            byte[] forwardKey = KeyCodec.stringIdForwardKey(graphId, text);
                            byte[] raw = session.get(Cf.CATALOG, forwardKey);
                            if (raw != null) {
                                vid = decodeLong(raw);
                            } else {
                                if (sequence >= Long.MAX_VALUE - 1L) {
                                    throw new CengyanException(ErrorCode.INTERNAL, "字符串节点 ID 已耗尽");
                                }
                                vid = Long.MIN_VALUE + 1L + sequence++;
                                batch.put(Cf.CATALOG, forwardKey, encodeLong(vid));
                                batch.put(Cf.CATALOG, KeyCodec.stringIdReverseKey(graphId, vid),
                                        text.getBytes(StandardCharsets.UTF_8));
                            }
                            pendingIds.put(text, vid);
                        }
                    }
                    requirePart(graphId, partOf(graphId, vid));
                    byte[] key = KeyCodec.vertexKey(graphId, partOf(graphId, vid), vid, type.id());
                    byte[] old = previous.containsKey(vid) ? previous.get(vid) : session.get(Cf.VERTEX, key);
                    if (ifNotExists && old != null) {
                        continue;
                    }
                    Map<String, Object> next = normalizeProperties(type.properties(), row.properties());
                    byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), next);
                    IndexMaintenance.sync(
                            session,
                            batch,
                            indexesFor(graphId, true, type.id()),
                            type.properties(),
                            old == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), old),
                            PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                            graphId,
                            KeyCodec.vertexIdentity(type.id(), vid));
                    batch.put(Cf.VERTEX, key, encoded);
                    previous.put(vid, encoded);
                    written++;
                }
                if (type.idType() == IdType.STRING && sequence != originalSequence) {
                    batch.put(Cf.CATALOG, counterKey, encodeLong(sequence));
                }
                if (!batch.isEmpty()) {
                    store.mutate(batch);
                }
            }
            return written;
        });
    }

    /**
     * 插入一条边。两端在同一分片时一次提交。
     * 跨分片时先提交出边、属性和补偿标记，再提交入边，最后删掉标记。
     * 补偿未完成时查询可能只看见出边。
     */
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
        List<String> stripes = new ArrayList<>();
        stripes.add(edgeStripe(graphId, relType));
        if (source.typeName() != null) {
            stripes.add(nodeStripe(graphId, source.typeName()));
        }
        if (target.typeName() != null) {
            stripes.add(nodeStripe(graphId, target.typeName()));
        }
        return store.withStripes(stripes, () -> {
            requireGraph(graphId);
            requireWritable(graphId);
            int outPart = partOf(graphId, source.vid());
            int inPart = partOf(graphId, target.vid());
            requirePart(graphId, outPart);
            requirePart(graphId, inPart);
            RelType type = relType(graphId, relType);
            byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), properties);
            byte[] propKey = KeyCodec.propKey(graphId, type.id(), rank, source.vid(), target.vid());
            List<VertexPut> vertices = vertexPuts(graphId, source, target);
            List<IndexDef> edgeIndexes = indexesFor(graphId, false, type.id());
            try (ReadSession session = store.read()) {
                byte[] existingProp = null;
                if (ifNotExists || !edgeIndexes.isEmpty()) {
                    existingProp = session.get(Cf.PROP, propKey);
                    if (ifNotExists && existingProp != null) {
                        return false;
                    }
                }
                if (outPart != inPart) {
                    return insertAcross(session, graphId, type, rank, source, target, encoded, propKey,
                            outPart, inPart, vertices, edgeIndexes, existingProp);
                }
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.EDGE, KeyCodec.edgeKey(
                        graphId, outPart, source.vid(), type.id(), Direction.OUT.code(), rank, target.vid()), EMPTY);
                batch.put(Cf.EDGE, KeyCodec.edgeKey(
                        graphId, inPart, target.vid(), type.id(), Direction.IN.code(), rank, source.vid()), EMPTY);
                batch.put(Cf.PROP, propKey, encoded);
                if (!edgeIndexes.isEmpty()) {
                    IndexMaintenance.sync(
                            session,
                            batch,
                            edgeIndexes,
                            type.properties(),
                            existingProp == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), existingProp),
                            PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                            graphId,
                            KeyCodec.edgeIdentity(type.id(), rank, source.vid(), target.vid()));
                }
                for (VertexPut vertex : vertices) {
                    byte[] vertexKey = KeyCodec.vertexKey(
                            graphId, partOf(graphId, vertex.vid()), vertex.vid(), vertex.tagId());
                    batch.put(Cf.VERTEX, vertexKey, vertex.value());
                    List<IndexDef> nodeIndexes = indexesFor(graphId, true, vertex.tagId());
                    if (nodeIndexes.isEmpty()) {
                        continue;
                    }
                    NodeType node = nodeTypeById(graphId, vertex.tagId());
                    byte[] existingVertex = session.get(Cf.VERTEX, vertexKey);
                    IndexMaintenance.sync(
                            session,
                            batch,
                            nodeIndexes,
                            node.properties(),
                            existingVertex == null
                                    ? null
                                    : PropCodec.decode(node.properties(), node.schemaVersion(), existingVertex),
                            PropCodec.decode(node.properties(), node.schemaVersion(), vertex.value()),
                            graphId,
                            KeyCodec.vertexIdentity(vertex.tagId(), vertex.vid()));
                }
                store.commitEdges(batch);
                return true;
            }
        });
    }

    /**
     * 批量写入边。当前要求边两端在同一分片；单机和单分片图可直接使用。
     * 每个逻辑边的出边、入边、属性和索引都进入同一个 RocksDB WriteBatch。
     */
    public int bulkInsertRels(
            int graphId, String relTypeName, List<BulkEdge> rows, boolean ifNotExists) {
        checkIdentifier(relTypeName);
        if (rows == null) {
            throw new IllegalArgumentException("批量边不能为空");
        }
        return store.withStripes(List.of(edgeStripe(graphId, relTypeName)), () -> {
            requireGraph(graphId);
            requireWritable(graphId);
            RelType type = relType(graphId, relTypeName);
            StoreBatch batch = new StoreBatch();
            Map<EdgeIdentity, byte[]> previous = new LinkedHashMap<>();
            try (ReadSession session = store.read()) {
                for (BulkEdge row : rows) {
                    int outPart = partOf(graphId, row.src());
                    int inPart = partOf(graphId, row.dst());
                    requirePart(graphId, outPart);
                    requirePart(graphId, inPart);
                    if (outPart != inPart) {
                        throw new CengyanException(ErrorCode.CROSS_PART_UNSUPPORTED,
                                "批量边的两端必须位于同一分片");
                    }
                    EdgeIdentity identity = new EdgeIdentity(row.rank(), row.src(), row.dst());
                    byte[] propKey = KeyCodec.propKey(graphId, type.id(), row.rank(), row.src(), row.dst());
                    byte[] old = previous.containsKey(identity) ? previous.get(identity) : session.get(Cf.PROP, propKey);
                    if (ifNotExists && old != null) {
                        continue;
                    }
                    Map<String, Object> next = normalizeProperties(type.properties(), row.properties());
                    byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), next);
                    batch.put(Cf.EDGE, KeyCodec.edgeKey(
                            graphId, outPart, row.src(), type.id(), Direction.OUT.code(), row.rank(), row.dst()), EMPTY);
                    batch.put(Cf.EDGE, KeyCodec.edgeKey(
                            graphId, inPart, row.dst(), type.id(), Direction.IN.code(), row.rank(), row.src()), EMPTY);
                    batch.put(Cf.PROP, propKey, encoded);
                    IndexMaintenance.sync(
                            session,
                            batch,
                            indexesFor(graphId, false, type.id()),
                            type.properties(),
                            old == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), old),
                            PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                            graphId,
                            KeyCodec.edgeIdentity(type.id(), row.rank(), row.src(), row.dst()));
                    previous.put(identity, encoded);
                }
                if (!batch.isEmpty()) {
                    store.commitEdges(batch);
                }
            }
            return previous.size();
        });
    }

    public Map<String, Object> readVertex(int graphId, String typeName, long vid) {
        NodeType type = nodeType(graphId, typeName);
        byte[] key = KeyCodec.vertexKey(graphId, partOf(graphId, vid), vid, type.id());
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
        requireWritable(graphId);
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
        byte[] key = KeyCodec.vertexKey(graphId, partOf(graphId, set.vid()), set.vid(), type.id());
        byte[] raw = session.get(Cf.VERTEX, key);
        if (raw == null) {
            return 0;
        }
        Map<String, Object> merged = new LinkedHashMap<>(PropCodec.decode(type.properties(), type.schemaVersion(), raw));
        merged.putAll(updates);
        byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), merged);
        batch.put(Cf.VERTEX, key, encoded);
        IndexMaintenance.sync(
                session,
                batch,
                indexesFor(graphId, true, type.id()),
                type.properties(),
                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                graphId,
                KeyCodec.vertexIdentity(type.id(), set.vid()));
        return 1;
    }

    private int writeEdge(ReadSession session, StoreBatch batch, int graphId, Mutation.SetEdge set) {
        ensureParts(graphId, set.outSrc(), set.outDst());
        RelType type = relType(graphId, set.typeName());
        Map<String, Object> updates = checkedUpdates(type.properties(), set.updates());
        byte[] key = KeyCodec.propKey(graphId, type.id(), set.rank(), set.outSrc(), set.outDst());
        byte[] raw = session.get(Cf.PROP, key);
        if (raw == null) {
            return 0;
        }
        Map<String, Object> merged = new LinkedHashMap<>(PropCodec.decode(type.properties(), type.schemaVersion(), raw));
        merged.putAll(updates);
        byte[] encoded = PropCodec.encode(type.schemaVersion(), type.properties(), merged);
        batch.put(Cf.PROP, key, encoded);
        IndexMaintenance.sync(
                session,
                batch,
                indexesFor(graphId, false, type.id()),
                type.properties(),
                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                graphId,
                KeyCodec.edgeIdentity(type.id(), set.rank(), set.outSrc(), set.outDst()));
        return 1;
    }

    private boolean removeEdge(ReadSession session, StoreBatch batch, int graphId, Mutation.DeleteEdge delete) {
        ensureParts(graphId, delete.outSrc(), delete.outDst());
        RelType type = relType(graphId, delete.typeName());
        byte[] prop = KeyCodec.propKey(graphId, type.id(), delete.rank(), delete.outSrc(), delete.outDst());
        byte[] out = edgeKey(graphId, type.id(), Direction.OUT, delete.outSrc(), delete.rank(), delete.outDst());
        byte[] in = edgeKey(graphId, type.id(), Direction.IN, delete.outDst(), delete.rank(), delete.outSrc());
        byte[] raw = session.get(Cf.PROP, prop);
        if (raw == null && session.get(Cf.EDGE, out) == null) {
            return false;
        }
        if (raw != null) {
            IndexMaintenance.sync(
                    session,
                    batch,
                    indexesFor(graphId, false, type.id()),
                    type.properties(),
                    PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                    null,
                    graphId,
                    KeyCodec.edgeIdentity(type.id(), delete.rank(), delete.outSrc(), delete.outDst()));
        }
        batch.delete(Cf.EDGE, out);
        batch.delete(Cf.EDGE, in);
        batch.delete(Cf.PROP, prop);
        return true;
    }

    /** @return 0 没有这个点，1 只删了点，2 还删了边 */
    private int removeVertex(
            ReadSession session, StoreBatch batch, int graphId, long vid, int maxEdgesPerVertex) {
        int part = partOf(graphId, vid);
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
            ensureParts(graphId, edge.outSrc(), edge.outDst());
            dropEdgeIndex(session, batch, graphId, edge);
            batch.delete(Cf.EDGE, edgeKey(graphId, edge.edgeTypeId(), Direction.OUT, edge.outSrc(), edge.rank(), edge.outDst()));
            batch.delete(Cf.EDGE, edgeKey(graphId, edge.edgeTypeId(), Direction.IN, edge.outDst(), edge.rank(), edge.outSrc()));
            batch.delete(Cf.PROP, KeyCodec.propKey(graphId, edge.edgeTypeId(), edge.rank(), edge.outSrc(), edge.outDst()));
        }
        for (byte[] key : vertices) {
            dropVertexIndex(session, batch, graphId, key);
            batch.delete(Cf.VERTEX, key);
        }
        return edges.isEmpty() ? 1 : 2;
    }

    public List<IndexDef> indexes(int graphId) {
        requireGraph(graphId);
        return indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).values().stream()
                .sorted(Comparator.comparing(IndexDef::name))
                .toList();
    }

    public IndexDef createIndex(
            int graphId, boolean unique, String name, boolean node, String typeName, List<String> columns) {
        checkIdentifier(name);
        if (columns == null || columns.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "索引至少要有一列");
        }
        return store.withWriteLock(() -> {
            requireWritable(graphId);
            try (ReadSession session = store.read()) {
                requireGraphRecord(session, graphId);
                if (indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).containsKey(name)) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "索引名已存在：" + name);
                }
                int typeId = node ? nodeType(graphId, typeName).id() : relType(graphId, typeName).id();
                List<PropDef> properties = node
                        ? nodeType(graphId, typeName).properties()
                        : relType(graphId, typeName).properties();
                int[] positions = new int[columns.size()];
                Set<String> seen = new HashSet<>();
                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    checkIdentifier(column);
                    if (!seen.add(column)) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "属性名重复：" + column);
                    }
                    int position = propertyPosition(properties, column);
                    if (position < 0) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + column);
                    }
                    positions[i] = position;
                }
                byte[] counterKey = KeyCodec.nextIndexIdKey();
                int indexId = allocate(session.get(Cf.CATALOG, counterKey), true);
                int kind = node ? NODE_KIND : REL_KIND;
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.indexRecordKey(graphId, indexId),
                        IndexCodec.record(name, kind, typeId, positions, unique, false));
                batch.put(Cf.CATALOG, counterKey, counterAfter(indexId, true));
                store.mutate(batch);
                IndexDef created = rememberIndex(graphId, indexId, name, node, typeId, positions, unique, false);
                log.info("已创建索引 {}，编号 {}", name, indexId);
                return created;
            }
        });
    }

    public void dropIndex(int graphId, String name) {
        checkIdentifier(name);
        store.withWriteLock(() -> {
            requireWritable(graphId);
            IndexDef index = requireIndex(graphId, name);
            try (ReadSession session = store.read()) {
                StoreBatch batch = new StoreBatch();
                batch.delete(Cf.CATALOG, KeyCodec.indexRecordKey(graphId, index.id()));
                for (byte[] key : session.keysWithPrefix(Cf.INDEX, KeyCodec.indexDataPrefix(index.id()))) {
                    batch.delete(Cf.INDEX, key);
                }
                store.mutate(batch);
            }
            indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).remove(name);
            log.info("已删除索引 {}", name);
            return null;
        });
    }

    /**
     * 先把索引标成未就绪，再在同一把写锁里扫完数据并一次换上新键。
     * 取消或发现重复时保持未就绪，查询继续走原来的扫描。
     */
    public int rebuildIndex(int graphId, String name, CancelToken cancel) {
        checkIdentifier(name);
        return store.withWriteLock(() -> {
            requireWritable(graphId);
            IndexDef index = requireIndex(graphId, name);
            storeReady(graphId, index, false);
            try (ReadSession session = store.read()) {
                List<byte[]> fresh = new ArrayList<>();
                Set<ByteBuffer> seen = new HashSet<>();
                if (index.node()) {
                    NodeType type = nodeTypeById(graphId, index.typeId());
                    List<byte[]> keys = new ArrayList<>();
                    session.visitPrefix(Cf.VERTEX, KeyCodec.graphIdPrefix(graphId), cancel, keys::add);
                    int examined = 0;
                    for (byte[] key : keys) {
                        if ((examined++ & 63) == 0 && cancel != null && cancel.cancelled()) {
                            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
                        }
                        if (key.length != KeyCodec.VERTEX_KEY_LENGTH || KeyCodec.tagIdOf(key) != type.id()) {
                            continue;
                        }
                        byte[] raw = session.get(Cf.VERTEX, key);
                        if (raw == null) {
                            continue;
                        }
                        addRebuildKey(
                                fresh,
                                seen,
                                index,
                                type.properties(),
                                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                                KeyCodec.vertexIdentity(type.id(), KeyCodec.vidOf(key)),
                                graphId);
                    }
                } else {
                    RelType type = relTypeById(graphId, index.typeId());
                    List<byte[]> keys = new ArrayList<>();
                    session.visitPrefix(Cf.PROP, KeyCodec.propGraphPrefix(graphId), cancel, keys::add);
                    int examined = 0;
                    for (byte[] key : keys) {
                        if ((examined++ & 63) == 0 && cancel != null && cancel.cancelled()) {
                            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
                        }
                        if (key.length != KeyCodec.PROP_KEY_LENGTH || KeyCodec.propEdgeType(key) != type.id()) {
                            continue;
                        }
                        byte[] raw = session.get(Cf.PROP, key);
                        if (raw == null) {
                            continue;
                        }
                        addRebuildKey(
                                fresh,
                                seen,
                                index,
                                type.properties(),
                                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                                KeyCodec.edgeIdentity(
                                        type.id(), KeyCodec.propRank(key), KeyCodec.propSrc(key), KeyCodec.propDst(key)),
                                graphId);
                    }
                }
                if (cancel != null && cancel.cancelled()) {
                    throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
                }
                StoreBatch batch = new StoreBatch();
                for (byte[] old : session.keysWithPrefix(Cf.INDEX, KeyCodec.indexDataPrefix(index.id()))) {
                    batch.delete(Cf.INDEX, old);
                }
                for (byte[] key : fresh) {
                    batch.put(Cf.INDEX, key, EMPTY);
                }
                IndexCodec.Decoded decoded = IndexCodec.decode(
                        session.get(Cf.CATALOG, KeyCodec.indexRecordKey(graphId, index.id())));
                batch.put(Cf.CATALOG, KeyCodec.indexRecordKey(graphId, index.id()),
                        IndexCodec.record(
                                decoded.name(), decoded.kind(), decoded.typeId(), decoded.propertyIndexes(),
                                decoded.unique(), true));
                store.mutate(batch);
                rememberIndex(
                        graphId, index.id(), index.name(), index.node(), index.typeId(),
                        decoded.propertyIndexes(), index.unique(), true);
                log.info("已重建索引 {}，写入 {} 条", name, fresh.size());
                return fresh.size();
            }
        });
    }

    public List<StatsRow> stats(int graphId, CancelToken cancel) {
        requireGraph(graphId);
        Map<Integer, Long> relCount = new HashMap<>();
        Map<Integer, Set<Long>> vids = new HashMap<>();
        try (ReadSession session = store.read()) {
            session.visitPrefix(Cf.EDGE, KeyCodec.graphIdPrefix(graphId), cancel, key -> {
                if (key.length != KeyCodec.EDGE_KEY_LENGTH) {
                    return;
                }
                KeyCodec.DecodedEdge edge = KeyCodec.decodeEdge(key);
                if (edge.direction() != Direction.OUT.code()) {
                    return;
                }
                relCount.merge(edge.edgeTypeId(), 1L, Long::sum);
            });
            session.visitPrefix(Cf.VERTEX, KeyCodec.graphIdPrefix(graphId), cancel, key -> {
                if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
                    return;
                }
                vids.computeIfAbsent(KeyCodec.tagIdOf(key), id -> new HashSet<>()).add(KeyCodec.vidOf(key));
            });
        }
        List<StatsRow> rows = new ArrayList<>();
        for (NodeType type : nodeTypes(graphId)) {
            Set<Long> ids = vids.getOrDefault(type.id(), Set.of());
            rows.add(new StatsRow("NODE", type.name(), ids.size()));
        }
        for (RelType type : relTypes(graphId)) {
            rows.add(new StatsRow("REL", type.name(), relCount.getOrDefault(type.id(), 0L)));
        }
        rows.sort(Comparator.comparing(StatsRow::target).thenComparing(StatsRow::name));
        return rows;
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

    private static Map<String, Object> normalizeProperties(List<PropDef> definitions, Map<String, Object> input) {
        Map<String, PropDef> byName = new HashMap<>();
        for (PropDef definition : definitions) {
            byName.put(definition.name(), definition);
        }
        Map<String, Object> values = new LinkedHashMap<>();
        if (input == null) {
            return values;
        }
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            PropDef definition = byName.get(entry.getKey());
            if (definition == null) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "未知属性 " + entry.getKey());
            }
            Object value = entry.getValue();
            if (value instanceof Number number) {
                value = switch (definition.type()) {
                    case INT64 -> number.longValue();
                    case DOUBLE -> number.doubleValue();
                    default -> value;
                };
            } else if (value instanceof String text && definition.type() == ValueType.DATETIME) {
                try {
                    value = Instant.parse(text).toEpochMilli();
                } catch (java.time.format.DateTimeParseException ex) {
                    try {
                        value = LocalDateTime.parse(text).toInstant(ZoneOffset.UTC).toEpochMilli();
                    } catch (java.time.format.DateTimeParseException nested) {
                        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "DATETIME 格式不正确：" + text, nested);
                    }
                }
            }
            values.put(entry.getKey(), value);
        }
        return values;
    }

    private void ensureParts(int graphId, long left, long right) {
        requirePart(graphId, partOf(graphId, left));
        requirePart(graphId, partOf(graphId, right));
    }

    private byte[] edgeKey(int graphId, int edgeTypeId, Direction direction, long src, long rank, long dst) {
        return KeyCodec.edgeKey(graphId, partOf(graphId, src), src, edgeTypeId, direction.code(), rank, dst);
    }

    private void reload() {
        names.clear();
        canonicals.clear();
        nodeTypes.clear();
        relTypes.clear();
        indexes.clear();
        spaces.clear();
        moving.clear();
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
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.allIndexRecordPrefix())) {
                if (key.length != 7 || (key[0] & 0xFF) != 0x30) {
                    continue;
                }
                IndexCodec.Decoded decoded = IndexCodec.decode(session.get(Cf.CATALOG, key));
                rememberIndex(
                        KeyCodec.graphIdFromMarked(key),
                        KeyCodec.indexIdFromRecord(key),
                        decoded.name(),
                        decoded.kind() == NODE_KIND,
                        decoded.typeId(),
                        decoded.propertyIndexes(),
                        decoded.unique(),
                        decoded.ready());
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.spacePrefix())) {
                if (key.length != 3) {
                    continue;
                }
                int graphId = KeyCodec.graphIdFromMarked(key);
                byte[] value = session.get(Cf.CATALOG, key);
                spaces.put(graphId, new Space(
                        ClusterCodec.spaceMode(value),
                        ClusterCodec.spaceReplicas(value),
                        ClusterCodec.spaceParts(value)));
            }
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, new byte[] {0x53})) {
                if (key.length != 5) {
                    continue;
                }
                byte[] value = session.get(Cf.CATALOG, key);
                if (value != null && ClusterCodec.moveState(value) == ClusterCodec.MOVE_MOVING) {
                    int graphId = KeyCodec.graphIdFromMarked(key);
                    moving.computeIfAbsent(graphId, ignored -> ConcurrentHashMap.newKeySet())
                            .add(KeyCodec.movePartId(key));
                }
            }
        }
    }

    private StoredType createType(
            int graphId, int kind, String name, List<PropDef> properties, IdType idType, int idLength) {
        return createType(graphId, kind, name, properties, idType, idLength, null);
    }

    private StoredType createType(
            int graphId, int kind, String name, List<PropDef> properties, IdType idType, int idLength, String comment) {
        checkIdentifier(name);
        if (idType == null) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "ID 类型不能为空");
        }
        if (idLength <= 0 || idLength > 4096) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "ID 长度必须在 1 到 4096 之间");
        }
        if (kind == NODE_KIND && idType == IdType.INT && idLength > 19) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "INT ID 长度不能超过 19 位");
        }
        if (kind == REL_KIND) {
            idType = IdType.INT;
            idLength = 19;
        }
        final IdType storedIdType = idType;
        final int storedIdLength = idLength;
        List<PropDef> copied = copyProperties(properties);
        return store.withWriteLock(() -> {
            requireWritable(graphId);
            try (ReadSession session = store.read()) {
                requireGraphRecord(session, graphId);
                if (typeNameTaken(session, graphId, name)) {
                    throw new CengyanException(ErrorCode.NAME_CONFLICT, "类型名已存在：" + name);
                }
                byte[] counterKey = kind == NODE_KIND
                        ? KeyCodec.nextNodeTypeKey(graphId)
                        : KeyCodec.nextRelTypeKey(graphId);
                int typeId = allocate(session.get(Cf.CATALOG, counterKey), true);
                CatalogCodec.DecodedType decoded = new CatalogCodec.DecodedType(
                        1, name, copied, storedIdType, storedIdLength, comment);
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.typeKey(graphId, kind, typeId),
                        CatalogCodec.typeRecord(1, name, copied, storedIdType, storedIdLength, comment));
                batch.put(Cf.CATALOG, counterKey, counterAfter(typeId, true));
                store.mutate(batch);
                StoredType stored = remember(new KeyCodec.TypeKey(graphId, kind, typeId), decoded);
                log.info("已创建类型 {}，编号 {}", name, typeId);
                return stored;
            }
        });
    }

    private StoredType addProperty(
            int graphId, int kind, String typeName, String property, ValueType type, boolean nullable, String comment) {
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
            requireWritable(graphId);
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
                next.add(new PropDef(property, type, true, comment));
                int schemaVersion = current.schemaVersion() + 1;
                CatalogCodec.DecodedType decoded = new CatalogCodec.DecodedType(
                        schemaVersion, typeName, List.copyOf(next), current.idType(), current.idLength(), current.comment());
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.CATALOG, KeyCodec.typeKey(graphId, kind, typeKey.typeId()),
                        CatalogCodec.typeRecord(
                        schemaVersion, typeName, decoded.properties(), decoded.idType(), decoded.idLength(), current.comment()));
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
            NodeType type = new NodeType(
                    typeKey.typeId(), decoded.name(), decoded.schemaVersion(), properties,
                    decoded.idType(), decoded.idLength(), decoded.comment());
            nodeTypes.computeIfAbsent(typeKey.graphId(), id -> new ConcurrentHashMap<>()).put(type.name(), type);
            return new StoredType(type, null);
        }
        if (typeKey.kind() == REL_KIND) {
            RelType type = new RelType(typeKey.typeId(), decoded.name(), decoded.schemaVersion(), properties, decoded.comment());
            relTypes.computeIfAbsent(typeKey.graphId(), id -> new ConcurrentHashMap<>()).put(type.name(), type);
            return new StoredType(null, type);
        }
        throw new CengyanException(ErrorCode.INTERNAL, "类型种类无效");
    }

    private List<IndexDef> indexesFor(int graphId, boolean node, int typeId) {
        List<IndexDef> found = new ArrayList<>();
        for (IndexDef index : indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            if (index.node() == node && index.typeId() == typeId) {
                found.add(index);
            }
        }
        return found;
    }

    private IndexDef requireIndex(int graphId, String name) {
        requireGraph(graphId);
        IndexDef index = indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).get(name);
        if (index == null) {
            throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "索引不存在：" + name);
        }
        return index;
    }

    private void storeReady(int graphId, IndexDef index, boolean ready) {
        byte[] recordKey = KeyCodec.indexRecordKey(graphId, index.id());
        try (ReadSession session = store.read()) {
            byte[] raw = session.get(Cf.CATALOG, recordKey);
            if (raw == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "索引记录丢失：" + index.name());
            }
            IndexCodec.Decoded decoded = IndexCodec.decode(raw);
            StoreBatch batch = new StoreBatch();
            batch.put(Cf.CATALOG, recordKey, IndexCodec.record(
                    decoded.name(), decoded.kind(), decoded.typeId(), decoded.propertyIndexes(), decoded.unique(), ready));
            store.mutate(batch);
        }
        rememberIndex(
                graphId, index.id(), index.name(), index.node(), index.typeId(),
                index.propertyIndexes().stream().mapToInt(Integer::intValue).toArray(), index.unique(), ready);
    }

    private static void addRebuildKey(
            List<byte[]> fresh,
            Set<ByteBuffer> seen,
            IndexDef index,
            List<PropDef> definitions,
            Map<String, Object> props,
            byte[] identity,
            int graphId) {
        byte[] encoded = IndexMaintenance.encode(index, definitions, props);
        if (encoded == null) {
            return;
        }
        byte[] key = KeyCodec.indexKey(index.id(), encoded, graphId, identity);
        if (index.unique()) {
            ByteBuffer prefix = ByteBuffer.wrap(Arrays.copyOf(key, key.length - identity.length));
            if (!seen.add(prefix)) {
                throw new CengyanException(ErrorCode.UNIQUE_VIOLATION, "唯一索引冲突");
            }
        }
        fresh.add(key);
    }

    private void dropVertexIndex(ReadSession session, StoreBatch batch, int graphId, byte[] key) {
        if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
            return;
        }
        int tagId = KeyCodec.tagIdOf(key);
        NodeType type = nodeTypeById(graphId, tagId);
        byte[] raw = session.get(Cf.VERTEX, key);
        if (raw == null) {
            return;
        }
        IndexMaintenance.sync(
                session,
                batch,
                indexesFor(graphId, true, tagId),
                type.properties(),
                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                null,
                graphId,
                KeyCodec.vertexIdentity(tagId, KeyCodec.vidOf(key)));
    }

    private void dropEdgeIndex(ReadSession session, StoreBatch batch, int graphId, EdgeRef edge) {
        RelType type = relTypeById(graphId, edge.edgeTypeId());
        byte[] raw = session.get(Cf.PROP, KeyCodec.propKey(
                graphId, edge.edgeTypeId(), edge.rank(), edge.outSrc(), edge.outDst()));
        if (raw == null) {
            return;
        }
        IndexMaintenance.sync(
                session,
                batch,
                indexesFor(graphId, false, type.id()),
                type.properties(),
                PropCodec.decode(type.properties(), type.schemaVersion(), raw),
                null,
                graphId,
                KeyCodec.edgeIdentity(type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
    }

    private IndexDef rememberIndex(
            int graphId,
            int indexId,
            String name,
            boolean node,
            int typeId,
            int[] positions,
            boolean unique,
            boolean ready) {
        List<PropDef> definitions = node
                ? nodeTypeById(graphId, typeId).properties()
                : relTypeById(graphId, typeId).properties();
        List<String> propertyNames = new ArrayList<>();
        List<Integer> propertyIndexes = new ArrayList<>();
        for (int position : positions) {
            if (position < 0 || position >= definitions.size()) {
                throw new CengyanException(ErrorCode.INTERNAL, "索引列已经不在类型上");
            }
            propertyNames.add(definitions.get(position).name());
            propertyIndexes.add(position);
        }
        IndexDef index = new IndexDef(indexId, name, node, typeId, propertyNames, propertyIndexes, unique, ready);
        indexes.computeIfAbsent(graphId, id -> new ConcurrentHashMap<>()).put(name, index);
        return index;
    }

    private NodeType nodeTypeById(int graphId, int typeId) {
        for (NodeType type : nodeTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            if (type.id() == typeId) {
                return type;
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "节点类型丢失：" + typeId);
    }

    private RelType relTypeById(int graphId, int typeId) {
        for (RelType type : relTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            if (type.id() == typeId) {
                return type;
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失：" + typeId);
    }

    private static int propertyPosition(List<PropDef> properties, String name) {
        for (int i = 0; i < properties.size(); i++) {
            if (properties.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
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
            copied.add(new PropDef(property.name(), property.type(), property.nullable(), property.comment()));
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

    /**
     * 把已有键按新的分片数重写。只改计算函数会让旧键读不到。
     * 分片数仍是 1 时键的分片号保持 0。
     */
    public void setPartCount(int graphId, int partCount) {
        if (partCount < 1 || partCount > 65535) {
            throw new CengyanException(ErrorCode.SYNTAX, "分片数必须在 1 和 65535 之间");
        }
        store.withWriteLock(() -> {
            requireGraph(graphId);
            requireWritable(graphId);
            if (!movingParts(graphId).isEmpty()) {
                throw new CengyanException(ErrorCode.PART_READONLY, "分片正在搬迁");
            }
            Space space = spaceOf(graphId);
            if (space.parts == partCount) {
                return null;
            }
            if (store.repartitionGraph(graphId, space.parts, partCount)) {
                store.mutate(spaceBatch(graphId, space.mode, space.replicas, partCount));
                spaces.put(graphId, new Space(space.mode, space.replicas, partCount));
                log.info("图 {} 的分片数已改为 {}", graphId, partCount);
                return null;
            }
            try (ReadSession session = store.read()) {
                StoreBatch batch = new StoreBatch();
                byte[] prefix = KeyCodec.graphIdPrefix(graphId);
                for (byte[] key : session.keysWithPrefix(Cf.VERTEX, prefix)) {
                    if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
                        continue;
                    }
                    int desired = KeyCodec.partOf(KeyCodec.vidOf(key), partCount);
                    if (desired == KeyCodec.partIdOfVertex(key)) {
                        continue;
                    }
                    byte[] value = session.get(Cf.VERTEX, key);
                    batch.delete(Cf.VERTEX, key);
                    if (value != null) {
                        batch.put(Cf.VERTEX, KeyCodec.vertexKey(
                                graphId, desired, KeyCodec.vidOf(key), KeyCodec.tagIdOf(key)), value);
                    }
                }
                for (byte[] key : session.keysWithPrefix(Cf.EDGE, prefix)) {
                    if (key.length != KeyCodec.EDGE_KEY_LENGTH) {
                        continue;
                    }
                    KeyCodec.DecodedEdge decoded = KeyCodec.decodeEdge(key);
                    int desired = KeyCodec.partOf(decoded.src(), partCount);
                    if (desired == decoded.partId()) {
                        continue;
                    }
                    batch.delete(Cf.EDGE, key);
                    batch.put(Cf.EDGE, KeyCodec.edgeKey(
                            graphId, desired, decoded.src(), decoded.edgeTypeId(), decoded.direction(),
                            decoded.rank(), decoded.dst()), EMPTY);
                }
                for (byte[] key : session.keysWithPrefix(Cf.TXN, prefix)) {
                    if (key.length != 32) {
                        continue;
                    }
                    int desired = KeyCodec.partOf(KeyCodec.txnSrc(key), partCount);
                    if (desired == KeyCodec.txnPartId(key)) {
                        continue;
                    }
                    byte[] value = session.get(Cf.TXN, key);
                    batch.delete(Cf.TXN, key);
                    if (value != null) {
                        batch.put(Cf.TXN, KeyCodec.txnKey(
                                graphId, desired, KeyCodec.txnEdgeTypeId(key), KeyCodec.txnRank(key),
                                KeyCodec.txnSrc(key), KeyCodec.txnDst(key)), value);
                    }
                }
                List<Integer> holders = new ArrayList<>();
                for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.partVotePrefix(graphId, 0))) {
                    if (key.length != 7) {
                        continue;
                    }
                    byte[] value = session.get(Cf.CATALOG, key);
                    if (value != null && ClusterCodec.voteKind(value) == ClusterCodec.VOTE_VOTER) {
                        holders.add(KeyCodec.voteNodeId(key));
                    }
                }
                for (int part = 0; part < partCount; part++) {
                    if (!session.keysWithPrefix(Cf.CATALOG, KeyCodec.partVotePrefix(graphId, part)).isEmpty()) {
                        continue;
                    }
                    for (int nodeId : holders) {
                        batch.put(Cf.CATALOG, KeyCodec.partVoteKey(graphId, part, nodeId),
                                ClusterCodec.vote(ClusterCodec.VOTE_VOTER));
                    }
                }
                batch.put(Cf.CATALOG, KeyCodec.spaceKey(graphId),
                        ClusterCodec.space(space.mode, space.replicas, partCount));
                if (!batch.isEmpty()) {
                    store.mutate(batch);
                }
            }
            spaces.put(graphId, new Space(space.mode, space.replicas, partCount));
            log.info("图 {} 的分片数已改为 {}", graphId, partCount);
            return null;
        });
    }

    public void markReplicaSync(int graphId) {
        store.withWriteLock(() -> {
            requireGraph(graphId);
            Space space = spaceOf(graphId);
            if (space.mode == ClusterCodec.MODE_SYNC) {
                throw new CengyanException(ErrorCode.SPACE_READONLY, "空间正在同步副本");
            }
            store.mutate(spaceBatch(graphId, ClusterCodec.MODE_SYNC, space.replicas, space.parts));
            spaces.put(graphId, new Space(ClusterCodec.MODE_SYNC, space.replicas, space.parts));
            log.info("图 {} 进入副本同步，写入暂时拒绝", graphId);
            return null;
        });
    }

    public void finishReplica(int graphId, int replicas) {
        store.withWriteLock(() -> {
            requireGraph(graphId);
            Space space = spaceOf(graphId);
            store.mutate(spaceBatch(graphId, ClusterCodec.MODE_NORMAL, replicas, space.parts));
            spaces.put(graphId, new Space(ClusterCodec.MODE_NORMAL, replicas, space.parts));
            return null;
        });
    }

    public void markMoving(int graphId, int partId) {
        store.withWriteLock(() -> {
            requireGraph(graphId);
            requireWritable(graphId);
            if (movingParts(graphId).contains(partId)) {
                throw new CengyanException(ErrorCode.PART_READONLY, "分片 " + partId + " 正在搬迁");
            }
            StoreBatch batch = new StoreBatch();
            batch.put(Cf.CATALOG, KeyCodec.moveKey(graphId, partId), ClusterCodec.move(ClusterCodec.MOVE_MOVING));
            store.mutate(batch);
            movingParts(graphId).add(partId);
            log.info("图 {} 的分片 {} 开始搬迁", graphId, partId);
            return null;
        });
    }

    public void clearMoving(int graphId, int partId) {
        store.withWriteLock(() -> {
            StoreBatch batch = new StoreBatch();
            batch.delete(Cf.CATALOG, KeyCodec.moveKey(graphId, partId));
            store.mutate(batch);
            movingParts(graphId).remove(partId);
            return null;
        });
    }

    public void finishMove(int graphId, int partId, int sourceNode, int targetNode) {
        store.withWriteLock(() -> {
            StoreBatch batch = new StoreBatch();
            batch.put(Cf.CATALOG, KeyCodec.partVoteKey(graphId, partId, targetNode),
                    ClusterCodec.vote(ClusterCodec.VOTE_VOTER));
            if (sourceNode != targetNode) {
                batch.delete(Cf.CATALOG, KeyCodec.partVoteKey(graphId, partId, sourceNode));
            }
            batch.delete(Cf.CATALOG, KeyCodec.moveKey(graphId, partId));
            store.mutate(batch);
            movingParts(graphId).remove(partId);
            log.info("图 {} 的分片 {} 已搬到节点 {}", graphId, partId, targetNode);
            return null;
        });
    }

    /** 每 10 秒以及打开库时调用。state 1 补入边，state 2 删掉两侧。 */
    public void recoverTxn() {
        store.withWriteLock(() -> {
            List<byte[]> keys = new ArrayList<>();
            try (ReadSession session = store.read()) {
                for (int graphId : List.copyOf(canonicals.keySet())) {
                    keys.addAll(session.keysWithPrefix(Cf.TXN, KeyCodec.graphIdPrefix(graphId)));
                }
                for (byte[] key : keys) {
                    if (key.length != 32) {
                        continue;
                    }
                    byte[] value = session.get(Cf.TXN, key);
                    if (value == null || value.length < 1) {
                        continue;
                    }
                    int state = ClusterCodec.txnState(value);
                    if (state == ClusterCodec.TXN_OUT) {
                        redoInEdge(key);
                    } else if (state == ClusterCodec.TXN_ROLLBACK) {
                        eraseTxn(key, ClusterCodec.txnProperties(value));
                    }
                }
            }
            return null;
        });
    }

    private boolean insertAcross(
            ReadSession session,
            int graphId,
            RelType type,
            long rank,
            VertexWrite source,
            VertexWrite target,
            byte[] encoded,
            byte[] propKey,
            int outPart,
            int inPart,
            List<VertexPut> vertices,
            List<IndexDef> edgeIndexes,
            byte[] existingProp) {
        byte[] txnKey = KeyCodec.txnKey(graphId, outPart, type.id(), rank, source.vid(), target.vid());
        StoreBatch outBatch = new StoreBatch();
        outBatch.put(Cf.EDGE, KeyCodec.edgeKey(
                graphId, outPart, source.vid(), type.id(), Direction.OUT.code(), rank, target.vid()), EMPTY);
        outBatch.put(Cf.PROP, propKey, encoded);
        outBatch.put(Cf.TXN, txnKey, ClusterCodec.txn(ClusterCodec.TXN_OUT, encoded));
        if (!edgeIndexes.isEmpty()) {
            IndexMaintenance.sync(
                    session,
                    outBatch,
                    edgeIndexes,
                    type.properties(),
                    existingProp == null ? null : PropCodec.decode(type.properties(), type.schemaVersion(), existingProp),
                    PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                    graphId,
                    KeyCodec.edgeIdentity(type.id(), rank, source.vid(), target.vid()));
        }
        putVertices(session, outBatch, graphId, vertices, outPart);
        store.mutate(outBatch);
        try {
            StoreBatch inBatch = new StoreBatch();
            inBatch.put(Cf.EDGE, KeyCodec.edgeKey(
                    graphId, inPart, target.vid(), type.id(), Direction.IN.code(), rank, source.vid()), EMPTY);
            putVertices(session, inBatch, graphId, vertices, inPart);
            store.mutate(inBatch);
        } catch (Exception ex) {
            rollbackTxn(graphId, type, rank, source.vid(), target.vid(), outPart, encoded, propKey, txnKey, edgeIndexes);
            throw new CengyanException(ErrorCode.INTERNAL, "入边写入失败：" + ex.getMessage(), ex);
        }
        StoreBatch done = new StoreBatch();
        done.delete(Cf.TXN, txnKey);
        store.mutate(done);
        return true;
    }

    private void rollbackTxn(
            int graphId,
            RelType type,
            long rank,
            long outSrc,
            long outDst,
            int outPart,
            byte[] encoded,
            byte[] propKey,
            byte[] txnKey,
            List<IndexDef> edgeIndexes) {
        StoreBatch mark = new StoreBatch();
        mark.put(Cf.TXN, txnKey, ClusterCodec.txn(ClusterCodec.TXN_ROLLBACK, encoded));
        store.mutate(mark);
        eraseTxn(KeyCodec.txnKey(graphId, outPart, type.id(), rank, outSrc, outDst), encoded);
        if (!edgeIndexes.isEmpty()) {
            try (ReadSession session = store.read()) {
                StoreBatch indexes = new StoreBatch();
                IndexMaintenance.sync(
                        session,
                        indexes,
                        edgeIndexes,
                        type.properties(),
                        PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                        null,
                        graphId,
                        KeyCodec.edgeIdentity(type.id(), rank, outSrc, outDst));
                if (!indexes.isEmpty()) {
                    store.mutate(indexes);
                }
            }
        }
    }

    private void redoInEdge(byte[] txnKey) {
        int graphId = KeyCodec.txnGraphId(txnKey);
        int edgeTypeId = KeyCodec.txnEdgeTypeId(txnKey);
        long rank = KeyCodec.txnRank(txnKey);
        long outSrc = KeyCodec.txnSrc(txnKey);
        long outDst = KeyCodec.txnDst(txnKey);
        int inPart = partOf(graphId, outDst);
        byte[] inKey = KeyCodec.edgeKey(
                graphId, inPart, outDst, edgeTypeId, Direction.IN.code(), rank, outSrc);
        try (ReadSession session = store.read()) {
            if (session.get(Cf.EDGE, inKey) == null) {
                StoreBatch batch = new StoreBatch();
                batch.put(Cf.EDGE, inKey, EMPTY);
                store.mutate(batch);
            }
        }
        StoreBatch done = new StoreBatch();
        done.delete(Cf.TXN, txnKey);
        store.mutate(done);
    }

    private void eraseTxn(byte[] txnKey, byte[] encoded) {
        int graphId = KeyCodec.txnGraphId(txnKey);
        int edgeTypeId = KeyCodec.txnEdgeTypeId(txnKey);
        long rank = KeyCodec.txnRank(txnKey);
        long outSrc = KeyCodec.txnSrc(txnKey);
        long outDst = KeyCodec.txnDst(txnKey);
        int outPart = partOf(graphId, outSrc);
        int inPart = partOf(graphId, outDst);
        StoreBatch batch = new StoreBatch();
        batch.delete(Cf.EDGE, KeyCodec.edgeKey(
                graphId, outPart, outSrc, edgeTypeId, Direction.OUT.code(), rank, outDst));
        batch.delete(Cf.EDGE, KeyCodec.edgeKey(
                graphId, inPart, outDst, edgeTypeId, Direction.IN.code(), rank, outSrc));
        batch.delete(Cf.PROP, KeyCodec.propKey(graphId, edgeTypeId, rank, outSrc, outDst));
        batch.delete(Cf.TXN, txnKey);
        store.mutate(batch);
        if (encoded != null && encoded.length > 0) {
            RelType type = relTypeById(graphId, edgeTypeId);
            List<IndexDef> edgeIndexes = indexesFor(graphId, false, edgeTypeId);
            if (!edgeIndexes.isEmpty()) {
                    try (ReadSession session = store.read()) {
                        StoreBatch indexes = new StoreBatch();
                        IndexMaintenance.sync(
                                session,
                                indexes,
                                edgeIndexes,
                                type.properties(),
                                PropCodec.decode(type.properties(), type.schemaVersion(), encoded),
                                null,
                                graphId,
                                KeyCodec.edgeIdentity(edgeTypeId, rank, outSrc, outDst));
                        if (!indexes.isEmpty()) {
                            store.mutate(indexes);
                        }
                    }
                }
        }
    }

    private void putVertices(ReadSession session, StoreBatch batch, int graphId, List<VertexPut> vertices, int part) {
        for (VertexPut vertex : vertices) {
            if (partOf(graphId, vertex.vid()) != part) {
                continue;
            }
            byte[] vertexKey = KeyCodec.vertexKey(graphId, part, vertex.vid(), vertex.tagId());
            batch.put(Cf.VERTEX, vertexKey, vertex.value());
            List<IndexDef> nodeIndexes = indexesFor(graphId, true, vertex.tagId());
            if (nodeIndexes.isEmpty()) {
                continue;
            }
            NodeType node = nodeTypeById(graphId, vertex.tagId());
            byte[] existingVertex = session.get(Cf.VERTEX, vertexKey);
            IndexMaintenance.sync(
                    session,
                    batch,
                    nodeIndexes,
                    node.properties(),
                    existingVertex == null
                            ? null
                            : PropCodec.decode(node.properties(), node.schemaVersion(), existingVertex),
                    PropCodec.decode(node.properties(), node.schemaVersion(), vertex.value()),
                    graphId,
                    KeyCodec.vertexIdentity(vertex.tagId(), vertex.vid()));
        }
    }

    private void healIncomplete() {
        store.withWriteLock(() -> {
            for (var entry : List.copyOf(spaces.entrySet())) {
                if (entry.getValue().mode != ClusterCodec.MODE_SYNC) {
                    continue;
                }
                Space space = entry.getValue();
                store.mutate(spaceBatch(entry.getKey(), ClusterCodec.MODE_NORMAL, space.replicas, space.parts));
                spaces.put(entry.getKey(), new Space(ClusterCodec.MODE_NORMAL, space.replicas, space.parts));
                log.warn("副本同步未完成，图 {} 恢复为可写，投票成员保持不变", entry.getKey());
            }
            for (var entry : List.copyOf(moving.entrySet())) {
                for (int partId : List.copyOf(entry.getValue())) {
                    StoreBatch batch = new StoreBatch();
                    batch.delete(Cf.CATALOG, KeyCodec.moveKey(entry.getKey(), partId));
                    store.mutate(batch);
                }
            }
            moving.clear();
            return null;
        });
    }

    private Space spaceOf(int graphId) {
        return spaces.getOrDefault(graphId, new Space(ClusterCodec.MODE_NORMAL, 1, 1));
    }

    private static StoreBatch spaceBatch(int graphId, int mode, int replicas, int parts) {
        StoreBatch batch = new StoreBatch();
        batch.put(Cf.CATALOG, KeyCodec.spaceKey(graphId), ClusterCodec.space(mode, replicas, parts));
        return batch;
    }

    private Set<Integer> movingParts(int graphId) {
        return moving.computeIfAbsent(graphId, ignored -> ConcurrentHashMap.newKeySet());
    }

    private void requireWritable(int graphId) {
        Space space = spaces.get(graphId);
        if (space != null && space.mode == ClusterCodec.MODE_SYNC) {
            throw new CengyanException(ErrorCode.SPACE_READONLY, "空间正在同步副本");
        }
    }

    private void requirePart(int graphId, int partId) {
        if (movingParts(graphId).contains(partId)) {
            throw new CengyanException(ErrorCode.PART_READONLY, "分片 " + partId + " 正在搬迁");
        }
    }

    private static String nodeStripe(int graphId, String typeName) {
        return "node:" + graphId + ":" + typeName;
    }

    private static String edgeStripe(int graphId, String typeName) {
        return "edge:" + graphId + ":" + typeName;
    }

    private static String metaStripe(int graphId) {
        return "meta:" + graphId;
    }

    String graphNameOrNull(int graphId) {
        return canonicals.get(graphId);
    }

    String nodeTypeNameOrNull(int graphId, int typeId) {
        for (NodeType type : nodeTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            if (type.id() == typeId) {
                return type.name();
            }
        }
        return null;
    }

    String relTypeNameOrNull(int graphId, int typeId) {
        for (RelType type : relTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            if (type.id() == typeId) {
                return type.name();
            }
        }
        return null;
    }

    Set<Integer> nodeTypeIds(int graphId) {
        Set<Integer> ids = new HashSet<>();
        for (NodeType type : nodeTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            ids.add(type.id());
        }
        return ids;
    }

    Set<Integer> relTypeIds(int graphId) {
        Set<Integer> ids = new HashSet<>();
        for (RelType type : relTypes.getOrDefault(graphId, new ConcurrentHashMap<>()).values()) {
            ids.add(type.id());
        }
        return ids;
    }

    List<IndexDef> indexesOf(int graphId) {
        return List.copyOf(indexes.getOrDefault(graphId, new ConcurrentHashMap<>()).values());
    }

    IndexLocation locateIndex(int indexId) {
        for (var entry : indexes.entrySet()) {
            for (IndexDef index : entry.getValue().values()) {
                if (index.id() == indexId) {
                    return new IndexLocation(entry.getKey(), index);
                }
            }
        }
        return null;
    }

    record IndexLocation(int graphId, IndexDef index) {
    }

    private record Space(int mode, int replicas, int parts) {
    }

    public record BulkVertex(long vid, Map<String, Object> properties) {
        public BulkVertex {
            properties = properties == null
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        }
    }

    public record BulkVertexValue(Object id, Map<String, Object> properties) {
        public BulkVertexValue {
            properties = properties == null
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        }
    }

    public record BulkEdge(long rank, long src, long dst, Map<String, Object> properties) {
        public BulkEdge {
            properties = properties == null
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        }
    }

    private record EdgeIdentity(long rank, long src, long dst) {
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
