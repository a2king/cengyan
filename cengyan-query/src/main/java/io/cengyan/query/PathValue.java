package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.NodeType;
import io.cengyan.catalog.PropCodec;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.RelType;
import io.cengyan.catalog.ValueType;
import io.cengyan.store.Cf;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一条路径的返回对象。点和边都带上类型里定义的属性，没写入的字段是 null。 */
final class PathValue {
    private PathValue() {
    }

    static Map<String, Object> of(
            Catalog catalog,
            ReadSession session,
            int graphId,
            Map<Integer, String> typeNames,
            PathAll.Found found,
            Map<Long, Map<String, Object>> nodeCache,
            Map<EdgeRef, Map<String, Object>> edgeCache) {
        List<Map<String, Object>> nodes = new ArrayList<>(found.vids().size());
        for (long vid : found.vids()) {
            nodes.add(node(catalog, session, graphId, vid, nodeCache));
        }
        List<Map<String, Object>> edges = new ArrayList<>(found.edges().size());
        for (EdgeRef edge : found.edges()) {
            String typeName = typeNames.get(edge.edgeTypeId());
            if (typeName == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
            }
            edges.add(edge(catalog, session, graphId, typeName, edge, edgeCache));
        }
        return body(found.length(), nodes, edges);
    }

    static Map<String, Object> one(
            Catalog catalog,
            ReadSession session,
            int graphId,
            String typeName,
            long start,
            long neighbor,
            EdgeRef edge,
            Map<Long, Map<String, Object>> nodeCache,
            Map<EdgeRef, Map<String, Object>> edgeCache) {
        return body(1,
                List.of(node(catalog, session, graphId, start, nodeCache),
                        node(catalog, session, graphId, neighbor, nodeCache)),
                List.of(edge(catalog, session, graphId, typeName, edge, edgeCache)));
    }

    static Map<Integer, String> typeNames(Catalog catalog, int graphId) {
        Map<Integer, String> names = new LinkedHashMap<>();
        for (RelType type : catalog.relTypes(graphId)) {
            names.put(type.id(), type.name());
        }
        return names;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(int length, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> path = new LinkedHashMap<>();
        path.put("length", (long) length);
        path.put("nodes", nodes);
        path.put("edges", edges);
        return (Map<String, Object>) DateTimes.plain(path);
    }

    private static Map<String, Object> node(
            Catalog catalog,
            ReadSession session,
            int graphId,
            long vid,
            Map<Long, Map<String, Object>> cache) {
        Map<String, Object> cached = cache.get(vid);
        if (cached != null) {
            return cached;
        }
        List<String> types = new ArrayList<>();
        Map<String, Object> properties = new LinkedHashMap<>();
        byte[] prefix = KeyCodec.vertexPrefix(graphId, catalog.partOf(graphId, vid), vid);
        for (byte[] key : session.keysWithPrefix(Cf.VERTEX, prefix)) {
            if (key.length != KeyCodec.VERTEX_KEY_LENGTH) {
                continue;
            }
            NodeType tag = nodeType(catalog, graphId, KeyCodec.tagIdOf(key));
            if (tag == null) {
                continue;
            }
            types.add(tag.name());
            properties.putAll(decode(session, key, tag));
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", catalog.externalVertexId(graphId, null, vid));
        node.put("type", types.isEmpty() ? null : String.join(", ", types));
        node.put("properties", properties);
        cache.put(vid, node);
        return node;
    }

    private static Map<String, Object> decode(ReadSession session, byte[] key, NodeType type) {
        byte[] raw = session.get(Cf.VERTEX, key);
        if (raw == null) {
            return Map.of();
        }
        Map<String, Object> rawProps = PropCodec.decode(type.properties(), type.schemaVersion(), raw);
        Map<String, Object> values = new LinkedHashMap<>();
        for (PropDef definition : type.properties()) {
            Object value = rawProps.get(definition.name());
            if (value instanceof Long millis && definition.type() == ValueType.DATETIME) {
                values.put(definition.name(), new UtcTime(millis));
            } else {
                values.put(definition.name(), value);
            }
        }
        return values;
    }

    private static NodeType nodeType(Catalog catalog, int graphId, int typeId) {
        for (NodeType type : catalog.nodeTypes(graphId)) {
            if (type.id() == typeId) {
                return type;
            }
        }
        return null;
    }

    private static Map<String, Object> edge(
            Catalog catalog,
            ReadSession session,
            int graphId,
            String typeName,
            EdgeRef edge,
            Map<EdgeRef, Map<String, Object>> cache) {
        Map<String, Object> cached = cache.get(edge);
        if (cached != null) {
            return cached;
        }
        RelType type = catalog.relType(graphId, typeName);
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("type", typeName);
        item.put("rank", edge.rank());
        item.put("src", catalog.externalVertexId(graphId, null, edge.outSrc()));
        item.put("dst", catalog.externalVertexId(graphId, null, edge.outDst()));
        item.put("properties", properties(session, graphId, type, edge));
        cache.put(edge, item);
        return item;
    }

    private static Map<String, Object> properties(ReadSession session, int graphId, RelType type, EdgeRef edge) {
        byte[] raw = session.get(Cf.PROP, KeyCodec.propKey(graphId, type.id(), edge.rank(), edge.outSrc(), edge.outDst()));
        Map<String, Object> decoded = raw == null
                ? Map.of()
                : PropCodec.decode(type.properties(), type.schemaVersion(), raw);
        Map<String, Object> values = new LinkedHashMap<>();
        for (PropDef definition : type.properties()) {
            Object value = decoded.get(definition.name());
            if (value instanceof Long millis && definition.type() == ValueType.DATETIME) {
                values.put(definition.name(), new UtcTime(millis));
            } else {
                values.put(definition.name(), value);
            }
        }
        return values;
    }
}
