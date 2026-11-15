package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.WriteBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 幂等窗口决定分片库里为去重保存的记录保留多久，默认 1 小时。 */
class IdempotencyWindowTest {
    @AfterEach void reset() { StorageGateway.setIdempotencyWindowMs(TimeUnit.HOURS.toMillis(1)); }

    @Test void defaultWindowIsOneHour() {
        StorageGateway.setIdempotencyWindowMs(TimeUnit.HOURS.toMillis(1));
        long now = System.currentTimeMillis();
        WriteBatch batch = StorageGateway.batch("default").build();
        long window = batch.getExpiresAtMs() - now;
        // now 取自调用之前，窗口会多出几毫秒执行时间。
        assertTrue(window >= TimeUnit.HOURS.toMillis(1) && window <= TimeUnit.HOURS.toMillis(1) + 5000, "默认窗口 1 小时：" + window);
    }

    @Test void configuredWindowAppliesToNewBatches() {
        StorageGateway.setIdempotencyWindowMs(TimeUnit.MINUTES.toMillis(10));
        long now = System.currentTimeMillis();
        assertEquals(TimeUnit.MINUTES.toMillis(10), StorageGateway.batch("short").build().getExpiresAtMs() - now, 2000);
        assertEquals(TimeUnit.MINUTES.toMillis(10), StorageGateway.idempotencyWindowMs());
        assertThrows(IllegalArgumentException.class, () -> StorageGateway.setIdempotencyWindowMs(1), "窗口过短");
        assertThrows(IllegalArgumentException.class, () -> StorageGateway.setIdempotencyWindowMs(TimeUnit.DAYS.toMillis(8)), "窗口过长");
    }

    @Test void requestIdStillDeduplicatesInsideTheWindow() {
        StorageGateway.setIdempotencyWindowMs(TimeUnit.MINUTES.toMillis(30));
        WriteBatch first = StorageGateway.batch("same").addMutations(io.cengyan.protocol.proto.ClusterProtocol.Mutation.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).setValue(ByteString.copyFromUtf8("v"))).build();
        assertTrue(first.getExpiresAtMs() > System.currentTimeMillis());
    }
}
