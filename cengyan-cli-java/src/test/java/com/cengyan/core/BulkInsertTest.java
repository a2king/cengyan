package com.cengyan.core;

import com.cengyan.core.exception.BulkInsertException;
import com.cengyan.core.exception.CengyanException;
import com.cengyan.core.exception.CommitUncertainException;
import com.cengyan.core.exception.GqlSyntaxException;
import com.cengyan.core.internal.Json;
import com.cengyan.core.internal.PartitionRouter;
import com.cengyan.core.internal.Transport;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkInsertTest {
    private static final int PARTITIONS = 4;

    /**
     * 假集群：拓扑固定。第 failAt 个 bulkInsert 默认按真实 ComputeTransport 的行为返回 code=FAILED
     * （写请求重试用尽、提交状态未知）；设置 failure 时改为直接抛出该异常。
     */
    static final class FakeCluster implements Transport {
        final List<Map<String, Object>> bulks = new ArrayList<Map<String, Object>>();
        final List<Address> targets = new ArrayList<Address>();
        int failAt = -1;
        RuntimeException failure;
        String code = "OK";
        List<Object> leaders = Collections.emptyList();
        List<Object> computes = Collections.emptyList();
        String idType = "INT64";
        int topologyCalls;

        @Override
        public Map<String, Object> request(Map<String, Object> payload, Long timeout, boolean write, Address address) {
            Object op = payload.get("op");
            if ("login".equals(op)) return map("code", "OK", "token", "t");
            if ("bulkTopology".equals(op)) {
                topologyCalls++;
                return map("code", "OK", "partitions", (long) PARTITIONS, "vertexIdType", idType, "edgeIdType", idType,
                        "leaders", leaders, "computes", computes);
            }
            bulks.add(new LinkedHashMap<String, Object>(payload));
            targets.add(address);
            if (bulks.size() == failAt) {
                if (failure != null) throw failure;
                return map("code", "FAILED", "requestId", payload.get("requestId"), "message", "写请求重试 3 次后仍未获得成功确认");
            }
            if (!"OK".equals(code)) return map("code", code, "message", "服务端拒绝");
            return map("code", "OK", "rowsAffected", (long) ((List<?>) payload.get("rows")).size(), "skipped", 0L, "duplicates", 0L);
        }

        @Override
        public void updateAddresses(List<Address> addresses) {
        }

        @Override
        public void close() {
        }

        ClusterGraph graph() {
            return new ClusterClient(this, "root", "p").graph("g");
        }

        @SuppressWarnings("unchecked")
        List<List<Object>> plan() {
            List<List<Object>> plan = new ArrayList<List<Object>>();
            for (Map<String, Object> bulk : bulks) {
                List<Object> entry = new ArrayList<Object>();
                entry.add(bulk.get("requestId"));
                for (Map<String, Object> row : (List<Map<String, Object>>) bulk.get("rows")) entry.add(row.get("id"));
                plan.add(entry);
            }
            return plan;
        }
    }

    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i + 1 < pairs.length; i += 2) map.put((String) pairs[i], pairs[i + 1]);
        return map;
    }

    static List<Map<String, Object>> rows(int count) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (long i = 0; i < count; i++) rows.add(map("id", i));
        return rows;
    }

    static final RequestIdProvider IDENTITY = (index, batch) -> "job-" + index;

    static BulkOptions.Builder options() {
        return BulkOptions.builder();
    }

    static BulkOptions sequential(int batchSize) {
        return options().batchSize(batchSize).alignPartitions(false).build();
    }

    static BulkProgress progress(CengyanException error) {
        assertTrue(error.bulkProgress() != null, "异常上必须带批量进度");
        return error.bulkProgress();
    }

    // ---------------------------------------------------------------- 顺序切批（与 pycengyan test_cluster_bulk 对应）

    @Test
    void lazyIterableIsSplitAndEachBatchHasOwnId() {
        FakeCluster cluster = new FakeCluster();
        Iterable<Map<String, Object>> lazy = () -> rows(5).iterator();
        BulkProgress result = cluster.graph().insertNodes("N", lazy, sequential(2));
        assertEquals(5, result.confirmed());
        assertEquals(3, result.completedBatches());
        assertNull(result.failedBatch());
        Set<Object> ids = new HashSet<Object>();
        for (Map<String, Object> bulk : cluster.bulks) ids.add(bulk.get("requestId"));
        assertEquals(3, ids.size());
        assertEquals(cluster.bulks.get(2).get("requestId"), result.requestId());
        assertEquals("node", cluster.bulks.get(0).get("kind"));
        assertEquals(Boolean.FALSE, cluster.bulks.get(0).get("skipMissing"));
    }

    @Test
    void failedBatchStopsAndReportsOnlyConfirmedProgress() {
        FakeCluster cluster = new FakeCluster();
        cluster.failAt = 2;
        CommitUncertainException error = assertThrows(CommitUncertainException.class,
                () -> cluster.graph().insertNodes("N", rows(10), sequential(2)));
        BulkProgress progress = progress(error);
        assertEquals(2, progress.confirmed());
        assertEquals(1, (int) progress.failedBatch());
        assertEquals(2, cluster.bulks.size());
        assertEquals(cluster.bulks.get(1).get("requestId"), progress.requestId());
        assertEquals(progress.requestId(), error.requestId());
    }

    @Test
    void callerOwnedRequestIdIsReproducibleForReplay() {
        FakeCluster cluster = new FakeCluster();
        BulkOptions opts = options().batchSize(2).requestId(IDENTITY).alignPartitions(false).build();
        cluster.graph().insertNodes("N", rows(5), opts);
        cluster.graph().insertNodes("N", rows(5), opts);
        List<Object> ids = new ArrayList<Object>();
        for (Map<String, Object> bulk : cluster.bulks) ids.add(bulk.get("requestId"));
        assertEquals(Arrays.asList("job-0", "job-1", "job-2", "job-0", "job-1", "job-2"), ids);
    }

    @Test
    void invalidRequestIdIsRejectedBeforeAnyWrite() {
        assertThrows(IllegalArgumentException.class, () -> options().requestId("has space"));
        FakeCluster cluster = new FakeCluster();
        BulkOptions bad = options().requestId((i, rows) -> "has space").alignPartitions(false).build();
        assertThrows(IllegalArgumentException.class, () -> cluster.graph().insertNodes("N", rows(1), bad));
        assertTrue(cluster.bulks.isEmpty());
    }

    @Test
    void serverBusinessErrorKeepsItsOwnType() {
        FakeCluster cluster = new FakeCluster();
        cluster.code = "SYNTAX";
        GqlSyntaxException error = assertThrows(GqlSyntaxException.class, () -> cluster.graph().insertNodes("N", rows(1), sequential(1)));
        assertEquals(0, progress(error).confirmed());
        assertEquals(0, (int) progress(error).failedBatch());
    }

    @Test
    void replaySkipsConfirmedBatchesAndReusesFailedRequestId() {
        BulkOptions first = options().batchSize(2).requestId(IDENTITY).alignPartitions(false).build();
        FakeCluster broken = new FakeCluster();
        broken.failAt = 2;
        CommitUncertainException error = assertThrows(CommitUncertainException.class, () -> broken.graph().insertNodes("N", rows(6), first));
        int failed = progress(error).failedBatch();
        assertEquals(1, failed);
        assertEquals("job-1", error.requestId());

        FakeCluster resumed = new FakeCluster();
        BulkProgress result = resumed.graph().insertNodes("N", rows(6), first.toBuilder().startBatch(failed).build());
        assertEquals(Arrays.asList(Arrays.<Object>asList("job-1", 2L, 3L), Arrays.<Object>asList("job-2", 4L, 5L)), resumed.plan());
        assertEquals(4, result.confirmed());
        assertEquals(3, result.completedBatches());
    }

    @Test
    void invalidOptionsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> options().startBatch(-1));
        assertThrows(IllegalArgumentException.class, () -> options().batchSize(0));
        assertThrows(IllegalArgumentException.class, () -> options().batchSize(BulkOptions.MAX_BATCH_ROWS + 1));
        assertThrows(IllegalArgumentException.class, () -> options().maxBatchBytes(100));
        assertThrows(IllegalArgumentException.class, () -> options().maxBatchBytes(BulkOptions.MAX_BATCH_BYTES + 1));
    }

    @Test
    void edgeWithoutIdIsRejectedBeforeAnyWrite() {
        for (Object row : Arrays.asList(map("src", "a", "dst", "b"), map("id", null, "src", "a", "dst", "b"), "e1")) {
            FakeCluster cluster = new FakeCluster();
            BulkInsertException error = assertThrows(BulkInsertException.class,
                    () -> cluster.graph().insertEdges("E", Collections.singletonList(row)));
            assertEquals("INVALID_ARGUMENT", error.code());
            assertEquals(0, progress(error).confirmed());
            assertTrue(cluster.bulks.isEmpty());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void edgeMissingIdStopsAfterConfirmedBatches() {
        FakeCluster cluster = new FakeCluster();
        List<Map<String, Object>> rows = Arrays.asList(map("id", "e1", "src", "a", "dst", "b"), map("id", "e2", "src", "a", "dst", "c"),
                map("src", "a", "dst", "d"));
        BulkInsertException error = assertThrows(BulkInsertException.class,
                () -> cluster.graph().insertEdges("E", rows, options().batchSize(2).build()));
        assertEquals(2, progress(error).confirmed());
        assertEquals(1, (int) progress(error).failedBatch());
        assertEquals(1, cluster.bulks.size());
        List<Map<String, Object>> sent = (List<Map<String, Object>>) cluster.bulks.get(0).get("rows");
        assertEquals("e2", sent.get(1).get("id"));
    }

    @Test
    void skipMissingFlagIsEdgeOnlyAndEdgesNeverAlign() {
        FakeCluster cluster = new FakeCluster();
        BulkOptions opts = options().skipMissingEndpoints(true).build();
        cluster.graph().insertEdges("E", Collections.singletonList(map("id", "e", "src", "a", "dst", "b")), opts);
        assertEquals(Boolean.TRUE, cluster.bulks.get(0).get("skipMissing"));
        assertEquals("edge", cluster.bulks.get(0).get("kind"));
        assertEquals(0, cluster.topologyCalls);
        cluster.graph().insertNodes("N", rows(1), opts.toBuilder().alignPartitions(false).build());
        assertEquals(Boolean.FALSE, cluster.bulks.get(1).get("skipMissing"));
    }

    @Test
    void wireEncodableValuesAreAccepted() {
        FakeCluster cluster = new FakeCluster();
        Map<String, Object> row = map("id", "a", "seen", LocalDate.of(2026, 11, 10), "amount", new BigDecimal("1.25"), "ref", UUID.randomUUID());
        assertEquals(1, cluster.graph().insertNodes("N", Collections.singletonList(row), sequential(10)).confirmed());
    }

    @Test
    void rowLargerThanBatchLimitIsRecordTooLarge() {
        FakeCluster cluster = new FakeCluster();
        char[] text = new char[4000];
        Arrays.fill(text, 'x');
        BulkInsertException error = assertThrows(BulkInsertException.class, () -> cluster.graph().insertNodes("N",
                Collections.singletonList(map("id", new String(text))), options().maxBatchBytes(1024).alignPartitions(false).build()));
        assertEquals("RECORD_TOO_LARGE", error.code());
        assertTrue(cluster.bulks.isEmpty());
    }

    @Test
    void nanIsRejectedBeforeAnyWrite() {
        FakeCluster cluster = new FakeCluster();
        List<Map<String, Object>> rows = Arrays.asList(map("id", 1L, "score", 1.0), map("id", 2L, "score", Double.NaN));
        BulkInsertException error = assertThrows(BulkInsertException.class, () -> cluster.graph().insertNodes("N", rows, sequential(10)));
        assertEquals("INVALID_ARGUMENT", error.code());
        assertTrue(cluster.bulks.isEmpty());
    }

    @Test
    void transportErrorsKeepTheirTypeAndCarryProgress() {
        FakeCluster cluster = new FakeCluster();
        cluster.failAt = 2;
        cluster.failure = new com.cengyan.core.exception.ConnectionException("传输已关闭", "CONNECTION_ERROR", null);
        com.cengyan.core.exception.ConnectionException error = assertThrows(com.cengyan.core.exception.ConnectionException.class,
                () -> cluster.graph().insertNodes("N", rows(4), sequential(1)));
        assertEquals(1, (int) progress(error).failedBatch());
        assertEquals(cluster.bulks.get(1).get("requestId"), error.requestId());
    }

    @Test
    void nonProtocolFailureIsCommitUncertain() {
        FakeCluster cluster = new FakeCluster();
        cluster.failAt = 1;
        cluster.failure = new IllegalStateException("意外");
        CommitUncertainException error = assertThrows(CommitUncertainException.class, () -> cluster.graph().insertNodes("N", rows(1), sequential(1)));
        assertEquals("FAILED", error.code());
        assertEquals(cluster.bulks.get(0).get("requestId"), error.requestId());
    }

    // ---------------------------------------------------------------- 分片对齐（与 pycengyan test_bulk_aligned 对应）

    @Test
    @SuppressWarnings("unchecked")
    void everyAlignedBatchStaysInOnePartitionWithGlobalNumbering() {
        FakeCluster cluster = new FakeCluster();
        BulkProgress result = cluster.graph().insertNodes("P", rows(40), options().batchSize(5).requestId(IDENTITY).build());
        assertEquals(40, result.confirmed());
        for (Map<String, Object> bulk : cluster.bulks) {
            Set<Integer> parts = new HashSet<Integer>();
            for (Map<String, Object> row : (List<Map<String, Object>>) bulk.get("rows")) parts.add(PartitionRouter.partOf(row.get("id"), "INT64", PARTITIONS));
            assertEquals(1, parts.size());
        }
        for (int i = 0; i < cluster.bulks.size(); i++) assertEquals("job-" + i, cluster.bulks.get(i).get("requestId"));
        assertEquals(cluster.bulks.size(), result.completedBatches());
    }

    @Test
    void alignedFailureReportsGlobalPositionAndReplayResendsOnlyTheRest() {
        BulkOptions opts = options().batchSize(5).requestId(IDENTITY).build();
        FakeCluster full = new FakeCluster();
        full.graph().insertNodes("P", rows(40), opts);
        List<List<Object>> expected = full.plan();

        FakeCluster broken = new FakeCluster();
        broken.failAt = 3;
        CommitUncertainException error = assertThrows(CommitUncertainException.class, () -> broken.graph().insertNodes("P", rows(40), opts));
        BulkProgress progress = progress(error);
        assertEquals(2, (int) progress.failedBatch());
        assertEquals(2, progress.completedBatches());
        assertEquals(expected.get(0).size() - 1 + expected.get(1).size() - 1, progress.confirmed());
        assertEquals("job-2", progress.requestId());

        FakeCluster resumed = new FakeCluster();
        BulkProgress result = resumed.graph().insertNodes("P", rows(40), opts.toBuilder().startBatch(progress.failedBatch()).build());
        assertEquals(expected.subList(2, expected.size()), resumed.plan());
        assertEquals(expected.size(), result.completedBatches());
    }

    @Test
    void alignedBatchGoesToComputeNextToPartitionLeader() {
        int leaderPart = PartitionRouter.partOf(0L, "INT64", PARTITIONS);
        FakeCluster cluster = new FakeCluster();
        cluster.leaders = Arrays.<Object>asList(map("part", (long) leaderPart, "host", "10.0.0.7", "port", 9000L));
        cluster.computes = Arrays.<Object>asList(map("host", "10.0.0.7", "port", 19669L), map("host", "10.0.0.8", "port", 19669L));
        cluster.graph().insertNodes("P", rows(40), options().batchSize(5).build());
        for (int i = 0; i < cluster.bulks.size(); i++) {
            @SuppressWarnings("unchecked")
            Object id = ((List<Map<String, Object>>) cluster.bulks.get(i).get("rows")).get(0).get("id");
            int part = PartitionRouter.partOf(id, "INT64", PARTITIONS);
            assertEquals(part == leaderPart ? new Address("10.0.0.7", 19669) : null, cluster.targets.get(i));
        }
    }

    @Test
    void alignedInvalidIdsAreReportedWithProgress() {
        for (Object bad : Arrays.asList(null, BigInteger.ONE.shiftLeft(63), "abc", true, 1.5)) {
            FakeCluster cluster = new FakeCluster();
            List<Map<String, Object>> rows = rows(20);
            rows.add(map("id", bad));
            BulkInsertException error = assertThrows(BulkInsertException.class,
                    () -> cluster.graph().insertNodes("P", rows, options().batchSize(5).build()), String.valueOf(bad));
            assertEquals("INVALID_ARGUMENT", error.code());
            assertEquals(cluster.bulks.size(), (int) progress(error).failedBatch());
            long sent = 0;
            for (Map<String, Object> bulk : cluster.bulks) sent += ((List<?>) bulk.get("rows")).size();
            assertEquals(sent, progress(error).confirmed());
        }
    }

    @Test
    void alignedStringIdsUseStringHash() {
        FakeCluster cluster = new FakeCluster();
        cluster.idType = "STRING";
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 30; i++) rows.add(map("id", "org-" + i));
        cluster.graph().insertNodes("P", rows, options().batchSize(4).build());
        for (Map<String, Object> bulk : cluster.bulks) {
            Set<Integer> parts = new HashSet<Integer>();
            for (Object row : (List<?>) bulk.get("rows")) parts.add(PartitionRouter.partOf(((Map<?, ?>) row).get("id"), "STRING", PARTITIONS));
            assertEquals(1, parts.size());
        }
        FakeCluster wrong = new FakeCluster();
        wrong.idType = "STRING";
        assertThrows(BulkInsertException.class, () -> wrong.graph().insertNodes("P", rows(1)));
    }

    // ---------------------------------------------------------------- 字节上限

    @Test
    @SuppressWarnings("unchecked")
    void fullBatchesLeaveRoomForServerAddedFields() {
        FakeCluster cluster = new FakeCluster();
        char[] payload = new char[900];
        Arrays.fill(payload, 'x');
        String text = new String(payload);
        Iterable<Map<String, Object>> rows = () -> new Iterator<Map<String, Object>>() {
            long next;

            @Override
            public boolean hasNext() {
                return next < 12_000;
            }

            @Override
            public Map<String, Object> next() {
                return map("id", next++, "payload", text);
            }
        };
        cluster.graph().insertNodes("P", rows, options().batchSize(BulkOptions.MAX_BATCH_ROWS).alignPartitions(false).build());
        assertTrue(cluster.bulks.size() > 1);
        long total = 0;
        for (Map<String, Object> bulk : cluster.bulks) {
            assertTrue(Json.encodedSize(bulk) <= BulkOptions.MAX_BATCH_BYTES - 64 * 1024, "批次超过了 4 MiB − 64 KiB");
            total += ((List<Object>) bulk.get("rows")).size();
        }
        assertEquals(12_000, total);
    }

    @Test
    void callerRowsAreNotModified() {
        FakeCluster cluster = new FakeCluster();
        Map<String, Object> row = map("id", 1L, "name", "a");
        cluster.graph().insertNodes("N", Collections.singletonList(row), sequential(1));
        assertEquals(map("id", 1L, "name", "a"), row);
        assertFalse(row.containsKey("requestId"));
    }
}
