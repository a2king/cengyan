package io.cengyan.storage;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.LeaseHorizon;
import io.cengyan.protocol.proto.ClusterProtocol.ScanPage;
import io.cengyan.raft.GroupIds;
import io.cengyan.raft.RaftNode;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 图标记为 DROPPED 之后，在本机后台卸下 Data Group 并清空文件。
 * 卸下时只把目录改名为 {@code raft/dropped-<uuid>}，Ratis 重启不会把它当成活 Group。
 * 文件按 {@code background_bytes_per_second} 慢慢删除，避免一次清空把删除请求拖到超时。
 */
final class DroppedGraphCleanup {
    private static final Logger log = LoggerFactory.getLogger(DroppedGraphCleanup.class);
    private final StorageConfig config;
    private final RaftNode node;
    private final StorageGateway gateway;
    private final AtomicBoolean closed;
    private final AtomicBoolean cleaning = new AtomicBoolean();
    private final ExecutorService deleter;
    private ScheduledExecutorService scheduler;
    private volatile boolean paused;

    DroppedGraphCleanup(StorageConfig config, RaftNode node, StorageGateway gateway, AtomicBoolean closed) {
        this.config = config;
        this.node = node;
        this.gateway = gateway;
        this.closed = closed;
        deleter = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "storage-drop-cleanup");
            thread.setDaemon(true);
            return thread;
        });
    }

    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "storage-drop-scan");
            thread.setDaemon(true);
            return thread;
        });
        long interval = Math.max(1000, config.backgroundIntervalMs());
        scheduler.scheduleWithFixedDelay(this::tick, 2000, interval, TimeUnit.MILLISECONDS);
    }

    void pause() { paused = true; }

    void tick() { run(false); }

    /** 立刻跑一轮。测试用来证明删除发生在状态提交之后。 */
    void runOnce() { run(true); }

    void close() {
        paused = true;
        if (scheduler != null) scheduler.shutdownNow();
        deleter.shutdownNow();
    }

    boolean graphDropped(long graphId) {
        for (GraphDefinition graph : droppedGraphs()) if (graph.getId() == graphId) return true;
        return false;
    }

    boolean leaseRetained(long graphId) {
        long now = System.currentTimeMillis();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        ByteString prefix = TransactionEngine.text("schema/lease/" + graphId + "/");
        do {
            ScanPage page = gateway.scan(0, 0, prefix, after, 100, cursor);
            for (var item : page.getEntriesList()) {
                try {
                    if (LeaseHorizon.parseFrom(item.getValue()).getRetainUntilMs() > now) return true;
                } catch (InvalidProtocolBufferException ex) {
                    throw new ClusterException("CORRUPT_DATA", "租约授权窗口损坏", ex);
                }
            }
            if (!page.getMore()) return false;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (!closed.get());
        return true;
    }

    private void run(boolean force) {
        if (closed.get()) return;
        if (paused && !force) return;
        try { quarantineDropped(); }
        catch (Exception ex) {
            if (!closed.get()) log.warn("扫描已删除图失败，稍后重试：{}", ex.toString());
        }
        startDeleter();
    }

    private void quarantineDropped() {
        for (GraphDefinition graph : droppedGraphs()) {
            if (closed.get()) return;
            if (graph.getId() <= 0) continue;
            boolean leased;
            try { leased = leaseRetained(graph.getId()); }
            catch (RuntimeException ex) {
                log.warn("读取图 {} 的 Schema 租约失败，本轮不删除它的数据目录：{}", graph.getId(), ex.toString());
                continue;
            }
            int parts = graph.getPartitions();
            if (parts < 1 || parts > 65535) {
                log.warn("已删除图 {} 的分片数无效：{}", graph.getId(), parts);
                continue;
            }
            for (int part = 0; part < parts; part++) {
                if (closed.get()) return;
                int partId = part;
                try {
                    RaftGroupId id = GroupIds.data(config.clusterId(), graph.getId(), partId);
                    if (!node.hosts(id) && !node.dataDirectoryPresent(id)) continue;
                    if (node.hosts(id)) armWriteGate(graph.getId(), partId);
                    if (leased) continue;
                    Optional<Path> quarantined = node.detachDataGroup(id);
                    quarantined.ifPresent(path -> log.info("已隔离已删除图的数据目录：graphId={} partId={} groupId={} directory={}",
                            graph.getId(), partId, id.getUuid(), path.getFileName()));
                } catch (RuntimeException | IOException ex) {
                    log.warn("隔离已删除图 {} 分片 {} 失败，稍后重试：{}", graph.getId(), partId, ex.toString());
                }
            }
        }
    }

    private void armWriteGate(long graphId, int part) {
        try {
            var result = gateway.write(graphId, part, StorageGateway.batch("graph.dropgate/" + graphId + "/" + part + "/" + UUID.randomUUID())
                    .addMutations(TransactionEngine.put(TransactionEngine.text("__schema/writeGate"), TransactionEngine.text("DROPPED"))).build());
            if (!"OK".equals(result.getCode())) log.warn("图 {} 分片 {} 写栅栏未成功：{}", graphId, part, result.getCode());
        } catch (RuntimeException ex) {
            log.warn("图 {} 分片 {} 写栅栏失败，稍后重试：{}", graphId, part, ex.toString());
        }
    }

    private List<GraphDefinition> droppedGraphs() {
        List<GraphDefinition> result = new ArrayList<>();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            ScanPage page = gateway.scan(0, 0, TransactionEngine.text("catalog/graph/"), after, 100, cursor);
            for (var item : page.getEntriesList()) {
                try {
                    GraphDefinition graph = GraphDefinition.parseFrom(item.getValue());
                    if ("DROPPED".equals(graph.getState())) result.add(graph);
                } catch (InvalidProtocolBufferException ex) {
                    throw new ClusterException("CORRUPT_DATA", "图目录损坏", ex);
                }
            }
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (!closed.get());
        return result;
    }

    private void startDeleter() {
        if (!cleaning.compareAndSet(false, true)) return;
        try { deleter.execute(this::drain); }
        catch (RejectedExecutionException ex) { cleaning.set(false); }
    }

    private void drain() {
        try {
            long rate = Math.max(1, config.backgroundBytesPerSecond());
            while (!closed.get()) {
                List<Path> dirs = node.quarantinedDirectories();
                if (dirs.isEmpty()) return;
                for (Path dir : dirs) {
                    if (closed.get()) return;
                    long started = System.nanoTime();
                    node.deleteQuarantined(dir, rate, closed::get);
                    if (!Files.exists(dir)) {
                        log.info("已清空隔离目录 {}，耗时 {} ms", dir.getFileName(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
                    }
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            if (!closed.get()) log.warn("后台清空已删除图目录失败，稍后重试：{}", ex.toString());
        } finally { cleaning.set(false); }
    }
}
