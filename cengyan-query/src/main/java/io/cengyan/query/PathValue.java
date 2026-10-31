package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.RelType;
import io.cengyan.store.EdgeRef;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一条路径的返回对象：length、nodes、edges 对齐。边上不带属性。 */
final class PathValue {
    private PathValue() {
    }

    static Map<String, Object> of(Catalog catalog, int graphId, Map<Integer, String> typeNames, PathAll.Found found) {
        List<Map<String, Object>> nodes = new ArrayList<>(found.vids().size());
        for (long vid : found.vids()) {
            nodes.add(node(catalog, graphId, vid));
        }
        List<Map<String, Object>> edges = new ArrayList<>(found.edges().size());
        for (EdgeRef edge : found.edges()) {
            String typeName = typeNames.get(edge.edgeTypeId());
            if (typeName == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
            }
            edges.add(edge(catalog, graphId, typeName, edge));
        }
        return body(found.length(), nodes, edges);
    }

    static Map<String, Object> one(
            Catalog catalog, int graphId, String typeName, long start, long neighbor, EdgeRef edge) {
        return body(1, List.of(node(catalog, graphId, start), node(catalog, graphId, neighbor)),
                List.of(edge(catalog, graphId, typeName, edge)));
    }

    static Map<Integer, String> typeNames(Catalog catalog, int graphId) {
        Map<Integer, String> names = new LinkedHashMap<>();
        for (RelType type : catalog.relTypes(graphId)) {
            names.put(type.id(), type.name());
        }
        return names;
    }

    private static Map<String, Object> body(int length, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> path = new LinkedHashMap<>();
        path.put("length", (long) length);
        path.put("nodes", nodes);
        path.put("edges", edges);
        return path;
    }

    private static Map<String, Object> node(Catalog catalog, int graphId, long vid) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", catalog.externalVertexId(graphId, null, vid));
        return node;
    }

    private static Map<String, Object> edge(Catalog catalog, int graphId, String typeName, EdgeRef edge) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("type", typeName);
        item.put("rank", edge.rank());
        item.put("src", catalog.externalVertexId(graphId, null, edge.outSrc()));
        item.put("dst", catalog.externalVertexId(graphId, null, edge.outDst()));
        return item;
    }
}
