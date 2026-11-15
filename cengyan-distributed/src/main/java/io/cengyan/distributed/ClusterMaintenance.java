package io.cengyan.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * 集群维护模式在 Metadata 中的键和编码。维护期间禁止数据写入，Storage 封存本地分片，
 * 计划内重启后比对本地文件，一致则直接恢复可信，不做全量复制。
 */
public final class ClusterMaintenance {
    private static final ObjectMapper JSON = new ObjectMapper();
    /** 存在即表示集群处于维护模式。数据写入的 CAS 以"该键不存在"为条件。 */
    public static final ByteString KEY = TransactionEngine.text("cluster/maintenance");
    private static final String NODE_PREFIX = "cluster/maintenanceNode/";

    /** 维护状态：id 每次进入维护模式重新生成，封存文件只对同一个 id 有效。 */
    public record State(String id, long since) { }

    /**
     * 单个 Storage 节点在本次维护中的进度。
     * @param bootId     写入该记录的进程 bootId
     * @param sealedBoot 封存本地分片的进程 bootId，空表示尚未封存
     * @param resumed    本进程启动后已处理完上次封存（或无需处理）
     * @param untrusted  重启比对未通过、等待全量同步的分片数
     */
    public record NodeMarker(String id, String bootId, String sealedBoot, boolean resumed, int untrusted) { }

    private ClusterMaintenance() { }

    public static ByteString nodeKey(String nodeId) { return TransactionEngine.text(NODE_PREFIX + nodeId); }

    public static ByteString encode(Object value) {
        try { return ByteString.copyFrom(JSON.writeValueAsBytes(value)); }
        catch (IOException ex) { throw new IllegalStateException(ex); }
    }

    public static State state(ByteString bytes) {
        if (bytes == null) return null;
        try { return JSON.readValue(bytes.toByteArray(), State.class); }
        catch (IOException ex) { throw new ClusterException("CORRUPT_DATA", "维护状态损坏", ex); }
    }

    public static NodeMarker marker(ByteString bytes) {
        if (bytes == null) return null;
        try { return JSON.readValue(bytes.toByteArray(), NodeMarker.class); }
        catch (IOException ex) { throw new ClusterException("CORRUPT_DATA", "节点维护记录损坏", ex); }
    }

    public static State read(StorageGateway gateway) { return state(gateway.get(0, 0, KEY)); }

    public static ClusterException rejected() {
        return new ClusterException("CLUSTER_MAINTENANCE", "集群维护中，暂停写入");
    }
}
