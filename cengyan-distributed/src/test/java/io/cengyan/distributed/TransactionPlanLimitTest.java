package io.cengyan.distributed;

import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 事务计划上限来自 transaction_plan_max_bytes，默认 16 MiB。 */
class TransactionPlanLimitTest {
    @AfterEach void reset() { TransactionEngine.setPlanMaxBytes(TransactionEngine.DEFAULT_PLAN_MAX_BYTES); }

    @Test void defaultLimitIs16MiB() {
        assertEquals(16 * 1024 * 1024, TransactionEngine.planMaxBytes());
    }

    @Test void configuredLimitRejectsOversizedPlan() {
        TransactionEngine.setPlanMaxBytes(1024 * 1024);
        TransactionPlan plan = TransactionPlan.newBuilder().setTransactionId("oversize").setGraphId(1).setCoordinatorPart(0)
                .setDeadlineMs(System.currentTimeMillis() + 60_000)
                .addParticipants(ParticipantPlan.newBuilder().setPartId(0).addChanges(ConditionalMutation.newBuilder()
                        .setKey(TransactionEngine.text("k")).setValue(TransactionEngine.text("v".repeat(1024 * 1024))))).build();
        ClusterException error = assertThrows(ClusterException.class, () -> TransactionEngine.normalize(plan));
        assertEquals("BATCH_TOO_LARGE", error.code());
        TransactionEngine.setPlanMaxBytes(2 * 1024 * 1024);
        assertDoesNotThrow(() -> TransactionEngine.normalize(plan));
    }

    @Test void limitMustStayWithinOneToSixtyFourMiB() {
        assertThrows(IllegalArgumentException.class, () -> TransactionEngine.setPlanMaxBytes(1024));
        assertThrows(IllegalArgumentException.class, () -> TransactionEngine.setPlanMaxBytes(128 * 1024 * 1024));
    }
}
