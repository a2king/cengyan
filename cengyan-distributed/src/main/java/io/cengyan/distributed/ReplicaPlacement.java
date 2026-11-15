package io.cengyan.distributed;

import io.cengyan.protocol.proto.ClusterProtocol.Node;
import java.util.*;

/** 优先跨 zone/rack，再按承载量选择；标签缺失时退化为普通自动放置。 */
public final class ReplicaPlacement {
    private ReplicaPlacement() { }
    public static Node choose(List<Node> candidates, List<Node> existing, Map<String, Integer> counts) {
        Set<String> ids = new HashSet<>(), zones = new HashSet<>(), racks = new HashSet<>();
        for (Node node : existing) {
            ids.add(node.getNodeId());
            if (!node.getZone().isBlank()) zones.add(node.getZone());
            if (!node.getRack().isBlank()) racks.add(node.getZone() + "/" + node.getRack());
        }
        return candidates.stream().filter(node -> !ids.contains(node.getNodeId()))
                .min(Comparator.<Node>comparingInt(node -> node.getZone().isBlank() ? 1 : zones.contains(node.getZone()) ? 2 : 0)
                        .thenComparingInt(node -> node.getRack().isBlank() ? 1 : racks.contains(node.getZone() + "/" + node.getRack()) ? 2 : 0)
                        .thenComparingInt(node -> counts.getOrDefault(node.getNodeId(), 0)).thenComparing(Node::getNodeId))
                .orElseThrow(() -> new ClusterException("INSUFFICIENT_NODES", "没有可放置副本的存储节点"));
    }
}
