package io.cengyan.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.ClusterMaintenance;
import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.protocol.proto.ClusterProtocol.Endpoint;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.Mutation;
import io.cengyan.protocol.proto.ClusterProtocol.Node;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftNode;
import java.time.Duration;
import java.util.UUID;

/**
 * Raft 数据在重启后由日志和快照恢复，不再比对或丢弃本地文件。
 * 维护模式只暂停写入，并给滚动重启提供“已封存 / 已恢复”的进度。
 */
final class RaftMaintenance {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final StorageConfig config;
    private final StorageGateway storage;
    private final RaftNode node;
    private final String boot = UUID.randomUUID().toString();

    RaftMaintenance(StorageConfig config, StorageGateway storage, RaftNode node) {
        this.config = config;
        this.storage = storage;
        this.node = node;
    }

    String bootId() { return boot; }

    void tick() {
        ByteString raw = storage.get(0, 0, ClusterMaintenance.KEY);
        ClusterMaintenance.State state = ClusterMaintenance.state(raw);
        if (state == null) return;
        var marker = new ClusterMaintenance.NodeMarker(state.id(), boot, boot, true, 0);
        storage.write(0, 0, StorageGateway.batch("maintenance.marker/" + UUID.randomUUID())
                .addConditions(StorageGateway.condition(ClusterMaintenance.KEY, raw))
                .addMutations(TransactionEngine.put(ClusterMaintenance.nodeKey(config.nodeId()), ClusterMaintenance.encode(marker))).build());
    }

    String enter() {
        for (int attempt = 0; attempt < 16; attempt++) {
            ClusterMaintenance.State existing = ClusterMaintenance.read(storage);
            if (existing != null) return existing.id();
            var state = new ClusterMaintenance.State(UUID.randomUUID().toString(), System.currentTimeMillis());
            try {
                storage.write(0, 0, StorageGateway.batch("maintenance.enter/" + state.id())
                        .addConditions(StorageGateway.condition(ClusterMaintenance.KEY, null))
                        .addMutations(TransactionEngine.put(ClusterMaintenance.KEY, ClusterMaintenance.encode(state))).build());
                return state.id();
            } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        }
        throw new ClusterException("BUSY", "维护模式状态并发更新繁忙");
    }

    void exit() {
        var batch = StorageGateway.batch("maintenance.exit/" + UUID.randomUUID())
                .addMutations(Mutation.newBuilder().setKey(ClusterMaintenance.KEY).setDelete(true));
        for (Node member : new DistributedCatalog(storage).nodes()) {
            batch.addMutations(Mutation.newBuilder().setKey(ClusterMaintenance.nodeKey(member.getNodeId())).setDelete(true));
        }
        storage.write(0, 0, batch.build());
    }

    ObjectNode status() {
        var catalog = new DistributedCatalog(storage);
        ClusterMaintenance.State state = ClusterMaintenance.read(storage);
        ObjectNode result = JSON.createObjectNode();
        result.put("active", state != null).put("id", state == null ? "" : state.id()).put("since", state == null ? 0 : state.since());
        int total = 0;
        for (GraphDefinition graph : catalog.graphs()) {
            if (graph.getState().equals("ACTIVE")) total += graph.getPartitions();
        }
        result.putObject("partitions").put("total", total).put("ready", total).put("writing", 0).put("frozen", 0).put("lost", 0);
        var nodes = result.putArray("nodes");
        String leader = "";
        try {
            var metadata = node.server().getDivision(GroupIds.metadata(config.clusterId()));
            if (metadata.getInfo().getLeaderId() != null) leader = metadata.getInfo().getLeaderId().toString();
        } catch (Exception ignored) { }
        for (Node member : catalog.nodes()) {
            String live = member.getNodeId().equals(config.nodeId()) ? boot : identity(member);
            var marker = state == null ? null : ClusterMaintenance.marker(storage.get(0, 0, ClusterMaintenance.nodeKey(member.getNodeId())));
            if (marker != null && !marker.id().equals(state.id())) marker = null;
            boolean current = marker != null && marker.bootId().equals(live);
            nodes.addObject().put("nodeId", member.getNodeId()).put("reachable", !live.isEmpty()).put("bootId", live)
                    .put("sealed", current && marker.sealedBoot().equals(live))
                    .put("resumed", current && marker.resumed())
                    .put("untrustedReplicas", 0);
        }
        result.put("metadataLeader", leader);
        return result;
    }

    private String identity(Node member) {
        try {
            Endpoint rpc = member.getRpc();
            var response = storage.direct(new java.net.InetSocketAddress(rpc.getHost(), rpc.getPort()), "maintenance.boot", ByteString.EMPTY, Duration.ofSeconds(2));
            return response.getCode().equals("OK") ? response.getBody().toStringUtf8() : "";
        } catch (Exception ex) { return ""; }
    }
}
