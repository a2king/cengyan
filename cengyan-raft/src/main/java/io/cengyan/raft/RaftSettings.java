package io.cengyan.raft;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** 共识参数显式启用落盘、线性一致读取和安全快照清理。 */
public record RaftSettings(long segmentBytes, long snapshotEntries, long retainedEntries, int retainedSnapshots,
                           long rpcTimeoutMinMs, long rpcTimeoutMaxMs,
                           long firstElectionTimeoutMinMs, long firstElectionTimeoutMaxMs) {
    /**
     * 首次选主超时保持 Ratis 的默认：不设置时它从 {@code raft.server.rpc.timeout.min} 继承。
     * 显式调大只适合跨机房部署，单机会让进程启动后的第一个选主慢到秒级。
     */
    public RaftSettings(long segmentBytes, long snapshotEntries, long retainedEntries, int retainedSnapshots) {
        this(segmentBytes, snapshotEntries, retainedEntries, retainedSnapshots, 600, 1200, 0, 0);
    }

    public RaftSettings {
        if (segmentBytes < 1024 * 1024 || snapshotEntries < 1 || retainedEntries < 0 || retainedSnapshots < 1
                || rpcTimeoutMinMs < 1 || rpcTimeoutMaxMs < rpcTimeoutMinMs
                || (firstElectionTimeoutMinMs != 0 && firstElectionTimeoutMinMs < rpcTimeoutMaxMs)
                || (firstElectionTimeoutMaxMs != 0 && firstElectionTimeoutMaxMs < firstElectionTimeoutMinMs)) {
            throw new IllegalArgumentException("Raft 日志和快照配置无效");
        }
    }

    /** 首次选主超时是否由本配置显式指定；为 0 时沿用 Ratis 从 rpc 超时继承的值。 */
    public boolean customFirstElectionTimeout() { return firstElectionTimeoutMinMs > 0 && firstElectionTimeoutMaxMs > 0; }

    public static RaftSettings defaults() { return new RaftSettings(16L * 1024 * 1024, 2_000, 1_000, 1); }

    public RaftProperties properties(Path directory, String host, int port) {
        RaftProperties properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
        NettyConfigKeys.Server.setHost(properties, host);
        NettyConfigKeys.Server.setPort(properties, port);
        NettyConfigKeys.Server.setUseEpoll(properties, System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("linux"));
        NettyConfigKeys.Client.setUseEpoll(properties, System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("linux"));
        RaftServerConfigKeys.setStorageDir(properties, List.of(directory.toFile()));
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setForceSyncNum(properties, 1);
        RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(segmentBytes));
        RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf(Math.min(segmentBytes, 4L * 1024 * 1024)));
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
        RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, retainedEntries);
        RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(16L * 1024 * 1024));
        RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(16L * 1024 * 1024 + 8));
        RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf(32L * 1024 * 1024));
        RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(properties, SizeInBytes.valueOf(256 * 1024));
        RaftServerConfigKeys.setStagingCatchupGap(properties, 1);
        // 两节点删掉其中一个、或删掉后用同一节点 ID 再加入，都会改变多数派，必须允许。
        RaftServerConfigKeys.LeaderElection.setMemberMajorityAdd(properties, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotEntries);
        RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, retainedSnapshots);
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        // 租约有效时读不再等一轮心跳。多机必须同步时钟，这是部署前提。
        RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(rpcTimeoutMinMs, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(rpcTimeoutMaxMs, TimeUnit.MILLISECONDS));
        if (customFirstElectionTimeout()) {
            RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties,
                    TimeDuration.valueOf(firstElectionTimeoutMinMs, TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties,
                    TimeDuration.valueOf(firstElectionTimeoutMaxMs, TimeUnit.MILLISECONDS));
        }
        return properties;
    }
}
