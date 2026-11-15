package io.cengyan.compute;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import io.cengyan.raft.*;
import org.apache.ratis.protocol.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Compute 控制面；运行负载不写入该 Group。 */
public final class ComputeMembership {
    private final ComputeConfig config;
    private final RaftNode raft;
    private final ClientId clientId = ClientId.randomId();
    private final AtomicLong sequence = new AtomicLong();
    private final RaftGroupId groupId;
    public ComputeMembership(ComputeConfig config, RaftNode raft) { this.config = config; this.raft = raft; groupId = GroupIds.compute(config.clusterId()); }

    public List<ComputeConfig.Peer> members() {
        try {
            var result = new ArrayList<ComputeConfig.Peer>();
            for (RaftPeer peer : raft.server().getDivision(groupId).getRaftConf().getCurrentPeers()) {
                if (peer.getClientAddress() != null && peer.getAdminAddress() != null) {
                    result.add(new ComputeConfig.Peer(peer.getId().toString(), ComputeConfig.address(peer.getClientAddress()),
                            ComputeConfig.address(peer.getAddress()), ComputeConfig.address(peer.getAdminAddress())));
                }
            }
            return List.copyOf(result);
        } catch (Exception ex) { return config.members(); }
    }
    public void join(Node node) {
        try {
            var division = raft.server().getDivision(groupId);
            if (!division.getInfo().isLeaderReady()) throw new ClusterException("NOT_LEADER", "Compute 控制面 Leader 尚未就绪");
            var voters = new ArrayList<>(division.getRaftConf().getCurrentPeers());
            var peer = RaftPeer.newBuilder().setId(node.getNodeId())
                    .setAddress(new java.net.InetSocketAddress(node.getRaft().getHost(), node.getRaft().getPort()))
                    .setClientAddress(new java.net.InetSocketAddress(node.getRpc().getHost(), node.getRpc().getPort()))
                    .setAdminAddress(new java.net.InetSocketAddress(node.getClient().getHost(), node.getClient().getPort())).build();
            var existing = voters.stream().filter(p -> p.getId().equals(peer.getId())).findFirst();
            if (existing.isPresent()) {
                if (!existing.get().getAddress().equals(peer.getAddress()) || !Objects.equals(existing.get().getClientAddress(), peer.getClientAddress())) {
                    throw new ClusterException("NODE_CONFLICT", "已注册节点不能使用不同地址重新加入");
                }
                return;
            }
            try (var client = raft.client(RaftGroup.valueOf(groupId, division.getRaftConf().getAllPeers()))) {
                var learner = RaftPeer.newBuilder().setId(peer.getId()).setAddress(peer.getAddress()).setClientAddress(peer.getClientAddress())
                        .setAdminAddress(peer.getAdminAddress()).setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
                if (!client.admin().setConfiguration(voters, List.of(learner)).isSuccess()) throw new ClusterException("FAILED", "Compute learner 加入失败");
                voters.add(peer);
                if (!client.admin().setConfiguration(voters, List.of()).isSuccess()) throw new ClusterException("FAILED", "Compute 成员提升失败");
            }
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("FAILED", "Compute 成员变更失败", ex); }
    }
    public void setDraining(String nodeId, boolean draining) {
        try {
            var division = raft.server().getDivision(groupId);
            var group = RaftGroup.valueOf(groupId, division.getRaftConf().getAllPeers());
            try (var client = raft.client(group)) {
                var batch = StorageGateway.batch("compute.drain/" + UUID.randomUUID()).addMutations(Mutation.newBuilder()
                        .setKey(ByteString.copyFromUtf8("compute/drain/" + nodeId)).setValue(ByteString.copyFromUtf8(Boolean.toString(draining)))).build();
                var response = client.io().send(RocksStateMachine.message(batch.toByteArray()));
                if (!response.isSuccess()) throw new ClusterException("FAILED", "Compute 排空状态未提交");
            }
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("FAILED", "Compute 排空状态更新失败", ex); }
    }
    public boolean draining(String nodeId) {
        try {
            // 控制状态本地已提交视图；不用在每次负载心跳写 Raft。
            var division = raft.server().getDivision(groupId);
            var reply = division.getStateMachine().query(RocksStateMachine.message(ReadKey.newBuilder()
                    .setKey(ByteString.copyFromUtf8("compute/drain/" + nodeId)).build().toByteArray())).join();
            var value = StoredValue.parseFrom(reply.getContent().toByteArray());
            return value.getFound() && value.getValue().toStringUtf8().equals("true");
        } catch (Exception ex) { return true; }
    }
    public void remove(String nodeId) {
        if (nodeId.equals(config.nodeId())) throw new ClusterException("NODE_NOT_DRAINED", "不能通过本节点移除本节点，请在另一节点执行");
        if (!draining(nodeId)) throw new ClusterException("NODE_NOT_DRAINED", "先排空计算节点");
        try {
            var division = raft.server().getDivision(groupId);
            var voters = division.getRaftConf().getCurrentPeers().stream().filter(peer -> !peer.getId().toString().equals(nodeId)).toList();
            if (voters.isEmpty()) throw new ClusterException("NODE_NOT_DRAINED", "不能移除最后一个计算节点");
            try (var client = raft.client(RaftGroup.valueOf(groupId, division.getRaftConf().getAllPeers()))) {
                if (!client.admin().setConfiguration(voters, List.of()).isSuccess()) throw new ClusterException("FAILED", "Compute 移除未成功确认");
            }
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("FAILED", "Compute 节点移除失败", ex); }
    }
}
