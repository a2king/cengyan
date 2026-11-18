package com.cengyan.core;

import com.cengyan.core.exception.ResultException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 查询结果里的一条路径：服务端形如 {@code {"nodes":[...], "edges":[...], "length":n}}。不可变。 */
public final class GraphPath {
    private final List<GraphNode> nodes;
    private final List<GraphEdge> edges;

    public GraphPath(List<GraphNode> nodes, List<GraphEdge> edges) {
        this.nodes = Collections.unmodifiableList(new ArrayList<GraphNode>(nodes));
        this.edges = Collections.unmodifiableList(new ArrayList<GraphEdge>(edges));
    }

    /** 判断一个结果值是不是路径（有 nodes、edges 两个列表和 length）。 */
    public static boolean isPath(Object value) {
        if (!(value instanceof Map)) return false;
        Map<?, ?> map = (Map<?, ?>) value;
        return map.get("nodes") instanceof List && map.get("edges") instanceof List && map.containsKey("length");
    }

    /** 把结果值解析为路径；形状不对时抛 {@link ResultException}。 */
    public static GraphPath from(Object value) {
        if (!isPath(value)) throw new ResultException("值不是路径：" + Entities.describe(value), "RESULT_TYPE", null);
        Map<?, ?> map = (Map<?, ?>) value;
        List<GraphNode> nodes = new ArrayList<GraphNode>();
        for (Object item : (List<?>) map.get("nodes")) nodes.add(GraphNode.from(item));
        List<GraphEdge> edges = new ArrayList<GraphEdge>();
        for (Object item : (List<?>) map.get("edges")) edges.add(GraphEdge.from(item));
        return new GraphPath(nodes, edges);
    }

    public List<GraphNode> nodes() {
        return nodes;
    }

    public List<GraphEdge> edges() {
        return edges;
    }

    /** 边数。 */
    public int length() {
        return edges.size();
    }

    public GraphNode start() {
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    public GraphNode end() {
        return nodes.isEmpty() ? null : nodes.get(nodes.size() - 1);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof GraphPath && nodes.equals(((GraphPath) other).nodes) && edges.equals(((GraphPath) other).edges);
    }

    @Override
    public int hashCode() {
        return nodes.hashCode() * 31 + edges.hashCode();
    }

    @Override
    public String toString() {
        return "Path" + nodes + edges;
    }
}
