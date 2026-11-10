package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** rebuild 分片不走 Raft 应用路径，幂等记录必须由分片自己清理，否则随写入无限增长。 */
class ReplicaIdempotencyPurgeTest {
    private static WriteBatch batch(String id, long expiresAtMs) {
        return WriteBatch.newBuilder().setRequestId(id).setExpiresAtMs(expiresAtMs)
                .addMutations(io.cengyan.protocol.proto.ClusterProtocol.Mutation.newBuilder()
                        .setKey(ByteString.copyFromUtf8("k-" + id)).setValue(ByteString.copyFromUtf8("v"))).build();
    }

    @Test void purgeRemovesOnlyExpiredNonTransactionalRecords(@TempDir Path dir) throws Exception {
        try (RocksStateMachine replica = RocksStateMachine.freshReplica(dir.resolve("0-0"))) {
            long now = System.currentTimeMillis();
            assertEquals("OK", replica.applyReplica(batch("old", now - 1000), 1).getCode());
            assertEquals("OK", replica.applyReplica(batch("fresh", now + 3_600_000), 2).getCode());
            assertEquals("OK", replica.applyReplica(batch("tx.123", now - 1000), 3).getCode());

            // 未到期前一条都不能删。
            assertEquals(0, replica.purgeExpiredIdempotency(now - 2000));
            // 清理到当前时间：只删过期的普通记录，事务记录保留。
            assertEquals(1, replica.purgeExpiredIdempotency(now));
            assertEquals("OK", replica.applyReplica(batch("fresh", now + 3_600_000), 4).getCode(), "未过期记录仍在，重放返回原结果");
            assertEquals("OK", replica.applyReplica(batch("tx.123", now - 1000), 5).getCode(), "事务记录仍受幂等保护");
            // 过期记录已删，同一 requestId 可以重新执行。
            assertEquals("OK", replica.applyReplica(batch("old", now - 1000), 6).getCode());
        }
    }
}
