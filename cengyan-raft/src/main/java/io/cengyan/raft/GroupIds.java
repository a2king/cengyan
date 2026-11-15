package io.cengyan.raft;

import org.apache.ratis.protocol.RaftGroupId;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** 各类 Group 使用独立命名域，相同 clusterId 和分片身份在所有机器上得到相同 UUID。 */
public final class GroupIds {
    private GroupIds() { }
    public static RaftGroupId metadata(String clusterId) { return id(clusterId, "metadata"); }
    public static RaftGroupId compute(String clusterId) { return id(clusterId, "compute"); }
    public static RaftGroupId data(String clusterId, long graphId, int partId) {
        if (graphId <= 0 || partId < 0 || partId > 65534) throw new IllegalArgumentException("图分片身份无效");
        return id(clusterId, "data/" + graphId + "/" + partId);
    }
    private static RaftGroupId id(String clusterId, String suffix) {
        if (clusterId == null || clusterId.isBlank()) throw new IllegalArgumentException("集群 ID 不能为空");
        // 长度编码避免 clusterId 本身包含分隔符产生命名冲突。
        String name = "cengyan/v1/" + clusterId.length() + ":" + clusterId + "/" + suffix;
        return RaftGroupId.valueOf(UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)));
    }
}
