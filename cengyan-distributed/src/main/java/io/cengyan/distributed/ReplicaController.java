package io.cengyan.distributed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 副本数调整和副本修复。只在 Metadata Leader 的后台循环里推进（{@link #tick()}），每个分片一个持久化 {@link ReplicaTask}。
 *
 * <p>数据复制交给 Ratis：新节点先以 listener 身份加入分片，由 Leader 在后台发快照和日志；追平后再提升为投票成员，
 * 最后改路由。每轮对每个任务只前进一步：要做成员变更时只发起（存储节点在后台执行），之后每轮读成员状态看是否完成，
 * 不在一次 RPC 里等数据复制，所以大分片同步多久都不会超时。取消时把还没切换路由的分片回滚到原成员。
 *
 * <p>副本数调整是一个作业（{@code tasks/replicaJob/<图 ID>}，JSON），作业按分片陆续创建任务，同时最多 {@link #MAX_ACTIVE} 个分片在同步。
 */
public final class ReplicaController {
    /** 同时在同步的分片数上限（含副本修复和遗留任务），避免多个大快照抢带宽。 */
    public static final int MAX_ACTIVE = 2;
    static final ObjectMapper JSON = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    /** 成员变更发出后存储节点没有在执行、目标也没达到时，隔多久重新发起。 */
    private static final long REISSUE_NANOS = Duration.ofSeconds(30).toNanos();
    private static final Set<String> TERMINAL = Set.of("FINISHED", "CANCELED");
    private static final Set<String> ACTIVE_JOB = Set.of("RUNNING", "CANCELING");

    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    /** 本进程上次为某任务某阶段发起成员变更的时间。换 Leader 后为空，会立刻重新发起一次，存储端按任务去重。 */
    private final Map<String, Long> issued = new ConcurrentHashMap<>();
    private final Set<String> released = ConcurrentHashMap.newKeySet();
    private final Set<String> abandoned = ConcurrentHashMap.newKeySet();

    public ReplicaController(StorageGateway storage) { this.storage = storage; catalog = new DistributedCatalog(storage); }

    /** 副本数调整作业。state：RUNNING、CANCELING、DONE、CANCELED、FAILED。 */
    public record Job(String id, String graph, long graphId, int replicas, int previous, int parts, String state,
                      long createdAt, long updatedAt, String message) {
        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean active() { return ACTIVE_JOB.contains(state); }
        Job with(String nextState, String nextMessage) {
            return new Job(id, graph, graphId, replicas, previous, parts, nextState, createdAt, System.currentTimeMillis(), nextMessage);
        }
    }

    /** 分片 Leader 看到的成员状态，来自存储节点的 data.membership。 */
    record Membership(Node leader, Set<String> voters, Set<String> listeners, boolean transitional, boolean configuring,
                      String configureTask, String error, long commitIndex, Map<String, Long> match, long snapshotBytes) {
        static Membership parse(Node leader, ByteString body) {
            try {
                JsonNode node = JSON.readTree(body.toByteArray());
                Set<String> voters = new HashSet<>(), listeners = new HashSet<>();
                node.path("voters").forEach(v -> voters.add(v.asText()));
                node.path("listeners").forEach(v -> listeners.add(v.asText()));
                Map<String, Long> match = new HashMap<>();
                node.path("match").fields().forEachRemaining(e -> match.put(e.getKey(), e.getValue().asLong()));
                return new Membership(leader, voters, listeners, node.path("transitional").asBoolean(), node.path("configuring").asBoolean(),
                        node.path("configureTask").asText(""), node.path("error").asText(""), node.path("commitIndex").asLong(), match,
                        node.path("snapshotBytes").asLong(-1));
            } catch (java.io.IOException ex) { throw new ClusterException("PROTOCOL_ERROR", "分片成员状态无效", ex); }
        }
        boolean stable() { return !transitional && !configuring; }
    }

    // ---- 作业 ----

    /** 创建副本数调整作业后立即返回，不等数据复制。同一个图同时只能有一个作业；同一请求重试返回同一个作业。 */
    public Job resize(String requestId, String graphName, int replicas) {
        GraphDefinition graph = catalog.graph(graphName);
        DistributedCatalog.requireWritable(graph);
        List<Node> nodes = catalog.placementNodes();
        if (replicas < 1 || replicas > nodes.size()) throw new ClusterException("INSUFFICIENT_NODES",
                "副本数超出可用存储节点数量：要保留 " + replicas + " 份数据，可用存储节点 " + nodes.size() + " 个");
        ByteString key = jobKey(graph.getId());
        ByteString existing = storage.get(0, 0, key);
        Job current = existing == null ? null : parseJob(existing);
        if (current != null && current.id().equals(requestId)) return current;
        if (current != null && current.active()) {
            if (current.state().equals("RUNNING") && current.replicas() == replicas) return current;
            throw new ClusterException("BUSY", "图 " + graphName + " 正在调整副本，先等它完成或取消");
        }
        long now = System.currentTimeMillis();
        Job job = new Job(requestId, graphName, graph.getId(), replicas, Math.max(1, graph.getReplicas()), graph.getPartitions(), "RUNNING", now, now, "");
        storage.write(0, 0, StorageGateway.batch("replica.job/" + requestId).addConditions(StorageGateway.condition(key, existing))
                .addMutations(TransactionEngine.put(key, jobBytes(job))).build());
        return job;
    }

    /** 请求取消正在进行的作业。还没切换路由的分片会回滚，已完成的分片保留新副本。 */
    public Job cancel(String graphName) {
        GraphDefinition graph = catalog.graph(graphName);
        ByteString key = jobKey(graph.getId());
        ByteString existing = storage.get(0, 0, key);
        if (existing == null) throw new ClusterException("NOTHING_TO_DO", "这个图没有正在调整的副本任务");
        Job job = parseJob(existing);
        if (!job.state().equals("RUNNING")) {
            if (job.state().equals("CANCELING")) return job;
            throw new ClusterException("NOTHING_TO_DO", "这个图没有正在调整的副本任务");
        }
        Job next = job.with("CANCELING", "正在取消");
        storage.write(0, 0, StorageGateway.batch("replica.cancel/" + job.id() + "/" + UUID.randomUUID())
                .addConditions(StorageGateway.condition(key, existing)).addMutations(TransactionEngine.put(key, jobBytes(next))).build());
        return next;
    }

    /** 作业和各分片的进度，给页面轮询。只读 Metadata；正在同步的分片再问一次分片 Leader 拿日志进度。 */
    public ObjectNode status(String graphName) {
        GraphDefinition graph = catalog.graph(graphName);
        ObjectNode result = JSON.createObjectNode().put("graph", graphName).put("parts", graph.getPartitions());
        ByteString bytes = storage.get(0, 0, jobKey(graph.getId()));
        Job job = bytes == null ? null : parseJob(bytes);
        if (job != null) result.set("job", JSON.valueToTree(job));
        Map<Integer, ReplicaTask> tasks = new HashMap<>();
        if (job != null) for (ReplicaTask task : tasks("tasks/replica/" + job.id() + "/")) tasks.put(part(task), task);
        Map<Integer, ReplicaTask> others = new HashMap<>();
        for (ReplicaTask task : tasks("tasks/replica/")) {
            if (task.getBefore().getGraphId() != graph.getId() || TERMINAL.contains(task.getStage())) continue;
            if (job == null || !task.getTaskId().startsWith(job.id() + "/")) others.put(task.getBefore().getPartId(), task);
        }
        ArrayNode parts = result.putArray("partitions");
        int done = 0, running = 0, pending = 0;
        for (int part = 0; part < graph.getPartitions(); part++) {
            DataGroupSpec spec = spec(storage.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
            ReplicaTask task = tasks.getOrDefault(part, others.get(part));
            ObjectNode row = parts.addObject().put("part", part);
            ArrayNode members = row.putArray("members");
            spec.getMembersList().forEach(m -> members.add(m.getNodeId()));
            String state;
            if (task != null && !TERMINAL.contains(task.getStage())) {
                state = task.getStage();
                running++;
                ArrayNode targets = row.putArray("targets");
                added(task).forEach(n -> targets.add(n.getNodeId()));
                if (!task.getError().isBlank()) row.put("error", task.getError());
                progress(task, row);
            } else if (task != null && task.getStage().equals("CANCELED")) {
                state = "CANCELED";
            } else if (job == null || spec.getMembersCount() == job.replicas() || (task != null && task.getStage().equals("FINISHED"))) {
                state = "DONE"; done++;
            } else {
                state = job.active() ? "PENDING" : "NOT_CHANGED"; pending++;
            }
            row.put("state", state);
        }
        result.put("done", done).put("running", running).put("pending", pending);
        return result;
    }

    private void progress(ReplicaTask task, ObjectNode row) {
        try {
            Membership m = membership(task);
            row.put("commitIndex", m.commitIndex());
            ObjectNode match = row.putObject("match");
            for (Node target : added(task)) if (m.match().containsKey(target.getNodeId())) match.put(target.getNodeId(), m.match().get(target.getNodeId()));
            if (m.snapshotBytes() >= 0) row.put("snapshotBytes", m.snapshotBytes());
            // 快照传输期间 match 没有意义（-1），改看新副本已经收到多少字节。
            ObjectNode received = row.putObject("receivedBytes");
            ByteString body = task.getBefore().toBuilder().clearControlTaskId().build().toByteString();
            for (Node target : added(task)) {
                if (m.match().getOrDefault(target.getNodeId(), -1L) > 0) continue;
                try {
                    var reply = storage.direct(DistributedCatalog.address(target), "data.receiving", body, Duration.ofSeconds(3));
                    if (!reply.getCode().equals("OK")) continue;
                    long bytes = Long.parseLong(reply.getBody().toStringUtf8().trim());
                    if (bytes >= 0) received.put(target.getNodeId(), bytes);
                } catch (RuntimeException ignored) { /* 旧版 Storage 不支持或节点暂时不可达，只是少显示进度 */ }
            }
        } catch (RuntimeException ex) { row.put("progressError", ex.getMessage() == null ? ex.toString() : ex.getMessage()); }
    }

    // ---- 后台推进 ----

    /** 兼容旧调用：推进一轮。 */
    public void recover() { tick(); }

    /** 还有没结束的副本任务或作业。后台据此决定是否快速推进。 */
    public boolean busy() {
        return tasks("tasks/replica/").stream().anyMatch(t -> !TERMINAL.contains(t.getStage())) || jobs().stream().anyMatch(Job::active);
    }

    /** Metadata Leader 每轮调用一次：推进所有任务一步、处理取消、按并发上限给作业派新分片、收尾完成的作业。 */
    public void tick() {
        Map<Long, Job> jobs = new HashMap<>();
        for (Job job : jobs()) jobs.put(job.graphId(), job);
        int active = 0;
        for (ReplicaTask task : tasks("tasks/replica/")) {
            Job job = jobOf(task, jobs);
            ReplicaTask next = task;
            boolean orphan = job == null && !task.getTaskId().startsWith("auto_");
            // 取消的作业，以及旧版本同步调用失败后留下、不属于任何作业的任务：还没切路由的都回滚，交给新作业统一处理。
            if ((orphan || job != null && job.state().equals("CANCELING")) && Set.of("CREATED", "CATCHING_UP").contains(task.getStage()))
                next = stage(task, "ROLLING_BACK");
            try { next = advance(next); }
            catch (ClusterException ex) {
                if (ex.code().equals("STALE_CONTROLLER")) throw ex;
                next = error(next, ex);
            }
            if (!TERMINAL.contains(next.getStage())) active++;
        }
        for (Job job : jobs.values()) {
            if (!job.active()) continue;
            try { active += drive(job, MAX_ACTIVE - active); }
            catch (ClusterException ex) {
                if (ex.code().equals("STALE_CONTROLLER")) throw ex;
                if (Set.of("GRAPH_NOT_FOUND", "GRAPH_STATE", "INSUFFICIENT_NODES").contains(ex.code())) save(job, job.with("FAILED", ex.getMessage()));
            }
        }
    }

    /** 给作业派新分片并在全部分片结束后收尾。返回新开始的分片数。 */
    private int drive(Job job, int slots) {
        List<ReplicaTask> mine = tasks("tasks/replica/" + job.id() + "/");
        Map<Integer, ReplicaTask> byPart = new HashMap<>();
        for (ReplicaTask task : mine) byPart.put(part(task), task);
        boolean busy = mine.stream().anyMatch(t -> !TERMINAL.contains(t.getStage()));
        GraphDefinition graph = catalog.graph(job.graph());
        if (graph.getId() != job.graphId()) throw new ClusterException("GRAPH_NOT_FOUND", "图已被删除重建，副本任务终止");
        List<DataGroupSpec> specs = new ArrayList<>();
        Map<String, Integer> counts = new HashMap<>();
        for (int part = 0; part < job.parts(); part++) {
            DataGroupSpec spec = spec(storage.get(0, 0, StorageGateway.groupKey(job.graphId(), part)));
            specs.add(spec);
            for (Node member : spec.getMembersList()) counts.merge(member.getNodeId(), 1, Integer::sum);
        }
        Set<Integer> busyParts = new HashSet<>();
        for (ReplicaTask task : tasks("tasks/replica/"))
            if (task.getBefore().getGraphId() == job.graphId() && !TERMINAL.contains(task.getStage())) busyParts.add(task.getBefore().getPartId());
        List<Integer> pending = new ArrayList<>();
        for (int part = 0; part < job.parts(); part++) {
            if (byPart.containsKey(part) || busyParts.contains(part)) continue;
            if (specs.get(part).getMembersCount() != job.replicas()) pending.add(part);
        }
        if (job.state().equals("CANCELING")) {
            if (busy) return 0;
            long finished = mine.stream().filter(t -> t.getStage().equals("FINISHED")).count();
            if (pending.isEmpty() && busyParts.isEmpty()) return finish(job, graph);
            save(job, job.with("CANCELED", finished == 0 ? "已取消，没有分片改变"
                    : "已取消。已完成的 " + finished + " 个分片保留新副本，其余分片回滚到原来的成员"));
            return 0;
        }
        int started = 0;
        if (!pending.isEmpty() && slots > 0) {
            List<Node> nodes = catalog.placementNodes();
            if (job.replicas() > nodes.size()) throw new ClusterException("INSUFFICIENT_NODES", "可用存储节点不足 " + job.replicas() + " 个");
            for (int part : pending) {
                if (started >= slots) break;
                DataGroupSpec old = specs.get(part);
                List<Node> next = new ArrayList<>(old.getMembersList());
                while (next.size() < job.replicas()) {
                    Node chosen = ReplicaPlacement.choose(nodes, next, counts);
                    counts.merge(chosen.getNodeId(), 1, Integer::sum);
                    next.add(chosen);
                }
                while (next.size() > job.replicas()) next.removeLast();
                String taskId = job.id() + "/" + part;
                ReplicaTask task = ReplicaTask.newBuilder().setTaskId(taskId).setBefore(old)
                        .setAfter(old.toBuilder().clearMembers().addAllMembers(next)).setStage("CREATED").build();
                try {
                    storage.write(0, 0, StorageGateway.batch("replica.task/" + taskId).addConditions(StorageGateway.condition(key(taskId), null))
                            .addMutations(TransactionEngine.put(key(taskId), task.toByteString())).build());
                } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
                started++;
            }
            return started;
        }
        if (busy || !pending.isEmpty() || !busyParts.isEmpty()) return 0;
        return finish(job, graph);
    }

    private int finish(Job job, GraphDefinition graph) {
        if (graph.getReplicas() != job.replicas()) {
            try { catalog.update(job.id() + "/factor", graph, graph.toBuilder().setReplicas(job.replicas()).build()); }
            catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; return 0; }
        }
        save(job, job.with("DONE", "已完成：每个分片保留 " + job.replicas() + " 份数据"));
        return 0;
    }

    /** 让任务前进一步，不等数据复制。返回最新的任务。 */
    ReplicaTask advance(ReplicaTask task) {
        String stage = task.getStage();
        if (TERMINAL.contains(stage)) {
            if (released.add(task.getTaskId())) release(task);
            return task;
        }
        DataGroupSpec before = task.getBefore(), after = task.getAfter();
        if (stage.equals("CREATED")) {
            claim(task);
            List<Node> added = added(task);
            if (added.isEmpty()) return stage(task, "CATCHING_UP");
            Membership m = membership(task);
            task = configureError(task, m);
            boolean joined = added.stream().allMatch(n -> m.listeners().contains(n.getNodeId()) || m.voters().contains(n.getNodeId()));
            if (joined && m.stable()) return stage(task, "CATCHING_UP");
            if (!m.configuring() && due(task)) {
                // 新节点先建一个空的分片，再作为 listener 加入，由 Leader 在后台发快照。
                var staging = before.toBuilder().clearLearners().addAllLearners(added).setControlTaskId(task.getTaskId()).build();
                for (Node target : added) {
                    var response = storage.direct(DistributedCatalog.address(target), "data.addGroup", staging.toByteString(), Duration.ofSeconds(30));
                    if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
                }
                configure(task, m, staging);
            }
            return task;
        }
        if (stage.equals("CATCHING_UP")) {
            claim(task);
            Membership m = membership(task);
            if (m.voters().equals(ids(after.getMembersList())) && m.stable()) return stage(task, "UPDATING_TOPOLOGY");
            task = configureError(task, m);
            if (!m.configuring() && due(task)) configure(task, m, after.toBuilder().setControlTaskId(task.getTaskId()).build());
            return task;
        }
        if (stage.equals("UPDATING_TOPOLOGY")) {
            ByteString route = StorageGateway.groupKey(before.getGraphId(), before.getPartId());
            ByteString current = storage.get(0, 0, route);
            if (!after.toByteString().equals(current)) {
                if (!before.toByteString().equals(current)) throw new ClusterException("TOPOLOGY_CONFLICT", "副本任务的原始路由已经改变，拒绝覆盖后续拓扑");
                storage.write(0, 0, StorageGateway.batch("replica.route/" + task.getTaskId())
                        .addConditions(StorageGateway.condition(route, before.toByteString())).addMutations(TransactionEngine.put(route, after.toByteString())).build());
            }
            storage.route(before.getGraphId(), before.getPartId(), after.getMembersList().stream().map(DistributedCatalog::address).toList());
            task = stage(task, "FINISHED");
            if (released.add(task.getTaskId())) release(task);
            return task;
        }
        if (stage.equals("ROLLING_BACK")) {
            Membership m = membership(task);
            task = configureError(task, m);
            List<Node> added = added(task);
            // 还不是投票成员的新节点直接卸下：正在发快照的话 Ratis 会因为它不再响应而放弃这次加入。
            // 已经是投票成员的先移出配置再卸下，否则少一票可能让分片失去多数派。
            for (Node target : added)
                if (!m.voters().contains(target.getNodeId()) && abandoned.add(task.getTaskId() + "/" + target.getNodeId())
                        && abandon(task, target) != null) abandoned.remove(task.getTaskId() + "/" + target.getNodeId());
            boolean restored = m.voters().equals(ids(before.getMembersList())) && added.stream().noneMatch(n -> m.listeners().contains(n.getNodeId()));
            if (restored && m.stable()) {
                for (Node target : added) {
                    String failure = abandon(task, target);
                    // 节点不可达时先结束回滚，留下的空副本由下次 data.addGroup 清掉；节点回答了失败就下一轮重试。
                    if (failure != null && !failure.startsWith("UNREACHABLE")) return error(task, "卸下 " + target.getNodeId() + " 上的新副本失败：" + failure);
                }
                task = stage(task, "CANCELED");
                if (released.add(task.getTaskId())) release(task);
                return task;
            }
            // 不等 Leader 上的旧变更结束：加入新副本那次 setConfiguration 要等新副本追平才确认，新副本卸下后永远等不到，
            // 客户端会一直重试，Leader 一直报 configuring。Leader 收到不同的成员目标会先停掉旧变更再执行回滚；
            // 目标相同则直接返回，所以每 30 秒重发一次没有副作用。
            if (due(task)) configure(task, m, before.toBuilder().clearLearners().setControlTaskId(task.getTaskId()).build());
            return task;
        }
        throw new ClusterException("CORRUPT_DATA", "未知的副本任务阶段：" + stage);
    }

    /** 存储节点上这个任务最近一次成员变更失败的原因，记进任务给页面看；下一轮会重新发起。 */
    private ReplicaTask configureError(ReplicaTask task, Membership m) {
        if (m.configuring() || m.error().isBlank() || !m.configureTask().equals(task.getTaskId())) return task;
        return error(task, m.error());
    }

    /** 让分片 Leader 在后台执行成员变更。存储节点立即返回；同一任务同一目标重复发起不会重复执行。 */
    private void configure(ReplicaTask task, Membership leader, DataGroupSpec spec) {
        issued.put(task.getTaskId() + "/" + task.getStage(), System.nanoTime());
        var response = storage.direct(DistributedCatalog.address(leader.leader()), "data.configure", spec.toByteString(), Duration.ofSeconds(30));
        if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
    }

    private boolean due(ReplicaTask task) {
        Long last = issued.get(task.getTaskId() + "/" + task.getStage());
        return last == null || System.nanoTime() - last > REISSUE_NANOS;
    }

    /** 让新节点卸下这次加的副本。成功返回 null，失败返回 "错误码 说明"。 */
    private String abandon(ReplicaTask task, Node target) {
        try {
            var response = storage.direct(DistributedCatalog.address(target), "data.abandonReplica",
                    task.getBefore().toBuilder().setControlTaskId(task.getTaskId()).build().toByteString(), Duration.ofSeconds(30));
            return response.getCode().equals("OK") ? null : response.getCode() + " " + response.getMessage();
        } catch (ClusterException ex) {
            // direct 连不上或超时都抛 FAILED；节点自己回答的失败走上面的返回值。
            if (ex.code().equals("STALE_CONTROLLER")) throw ex;
            return "UNREACHABLE " + ex.getMessage();
        }
    }

    /**
     * 问这次变更涉及的每个节点（新成员在前），谁是分片 Leader 就用谁的回答。不按路由找：单副本搬迁时 Leader 会转到新节点，
     * 而被移出的老节点不知道新 Leader，路由在任务完成前又只指向老节点。
     */
    Membership membership(ReplicaTask task) {
        List<Node> candidates = new ArrayList<>(task.getAfter().getMembersList());
        for (Node node : task.getBefore().getMembersList())
            if (candidates.stream().noneMatch(n -> n.getNodeId().equals(node.getNodeId()))) candidates.add(node);
        ByteString body = task.getBefore().toBuilder().clearControlTaskId().build().toByteString();
        String last = "没有可询问的节点";
        for (Node node : candidates) {
            try {
                var response = storage.direct(DistributedCatalog.address(node), "data.membership", body, Duration.ofSeconds(5));
                if (response.getCode().equals("OK")) return Membership.parse(node, response.getBody());
                last = node.getNodeId() + "：" + response.getCode() + " " + response.getMessage();
            } catch (ClusterException ex) {
                if (ex.code().equals("STALE_CONTROLLER")) throw ex;
                last = node.getNodeId() + "：" + ex.getMessage();
            }
        }
        throw new ClusterException("GROUP_UNAVAILABLE", "分片 " + task.getBefore().getPartId() + " 暂时没有 Leader（" + last + "）");
    }

    private void claim(ReplicaTask task) {
        DataGroupSpec before = task.getBefore();
        ByteString activeKey = activeKey(before);
        ByteString owner = storage.get(0, 0, activeKey);
        if (owner == null) {
            storage.write(0, 0, StorageGateway.batch("replica.claim/" + task.getTaskId())
                    .addConditions(StorageGateway.condition(activeKey, null))
                    .addConditions(StorageGateway.condition(StorageGateway.groupKey(before.getGraphId(), before.getPartId()), before.toByteString()))
                    .addMutations(TransactionEngine.put(activeKey, TransactionEngine.text(task.getTaskId()))).build());
        } else if (!owner.toStringUtf8().equals(task.getTaskId())) {
            throw new ClusterException("TOPOLOGY_CONFLICT", "分片已由另一副本任务占用：" + owner.toStringUtf8());
        }
    }

    private void release(ReplicaTask task) {
        ByteString activeKey = activeKey(task.getBefore());
        ByteString owner = storage.get(0, 0, activeKey);
        if (owner == null || !owner.toStringUtf8().equals(task.getTaskId())) return;
        try {
            storage.write(0, 0, StorageGateway.batch("replica.release/" + task.getTaskId())
                    .addConditions(StorageGateway.condition(activeKey, owner))
                    .addMutations(Mutation.newBuilder().setKey(activeKey).setDelete(true)).build());
        } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
    }

    public void drain(String requestId, String nodeId) {
        catalog.nodes().stream().filter(n -> n.getNodeId().equals(nodeId)).findFirst()
                .orElseThrow(() -> new ClusterException("NODE_NOT_FOUND", "存储节点不存在"));
        ByteString key = TransactionEngine.text("nodes/draining/" + nodeId);
        storage.write(0, 0, StorageGateway.batch("node.drain/" + requestId).addMutations(TransactionEngine.put(key, TransactionEngine.text("true"))).build());
    }

    /** Leader 周期调用。还有副本任务没结束时不派新的修复，每次最多一个分片，修复优先，避免新增节点触发无界复制。 */
    public void reconcile(long recoveryTimeoutMs) {
        if (tasks("tasks/replica/").stream().anyMatch(t -> !TERMINAL.contains(t.getStage()))) return;
        List<Node> nodes = catalog.nodes();
        Map<String, Boolean> healthy = new HashMap<>();
        Set<String> draining = new HashSet<>();
        long now = System.currentTimeMillis();
        for (Node node : nodes) {
            if (storage.get(0, 0, TransactionEngine.text("nodes/draining/" + node.getNodeId())) != null) draining.add(node.getNodeId());
            boolean alive;
            try { alive = storage.direct(DistributedCatalog.address(node), "ping", ByteString.EMPTY, Duration.ofMillis(500)).getCode().equals("OK"); }
            catch (Exception ex) { alive = false; }
            healthy.put(node.getNodeId(), alive);
            ByteString failureKey = TransactionEngine.text("health/offline/" + node.getNodeId());
            ByteString previous = storage.get(0, 0, failureKey);
            if (alive && previous != null) storage.write(0, 0, StorageGateway.batch("health.online/" + node.getNodeId() + "/" + previous.toStringUtf8())
                    .addConditions(StorageGateway.condition(failureKey, previous)).addMutations(Mutation.newBuilder().setKey(failureKey).setDelete(true)).build());
            else if (!alive && previous == null) {
                try { storage.write(0, 0, StorageGateway.batch("health.offline/" + node.getNodeId() + "/" + now)
                        .addConditions(StorageGateway.condition(failureKey, null)).addMutations(TransactionEngine.put(failureKey, TransactionEngine.text(Long.toString(now)))).build()); }
                catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
            }
        }
        List<DataGroupSpec> groups = new ArrayList<>();
        Map<String, Integer> counts = new HashMap<>();
        for (Node node : nodes) counts.put(node.getNodeId(), 0);
        for (GraphDefinition graph : catalog.graphs()) {
            if (!graph.getState().equals("ACTIVE")) continue;
            for (int part = 0; part < graph.getPartitions(); part++) {
                DataGroupSpec spec = spec(storage.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
                groups.add(spec);
                for (Node member : spec.getMembersList()) counts.merge(member.getNodeId(), 1, Integer::sum);
            }
        }
        for (DataGroupSpec group : groups) {
            for (Node source : group.getMembersList()) {
                boolean failed = !healthy.getOrDefault(source.getNodeId(), false);
                ByteString since = failed ? storage.get(0, 0, TransactionEngine.text("health/offline/" + source.getNodeId())) : null;
                if (failed && (group.getMembersCount() == 1 || since == null || now - Long.parseLong(since.toStringUtf8()) < recoveryTimeoutMs)) continue;
                List<Node> targets = nodes.stream().filter(n -> healthy.getOrDefault(n.getNodeId(), false) && !draining.contains(n.getNodeId())
                                && group.getMembersList().stream().noneMatch(member -> member.getNodeId().equals(n.getNodeId())))
                        .sorted(Comparator.comparingInt(n -> counts.getOrDefault(n.getNodeId(), 0))).toList();
                if (targets.isEmpty()) continue;
                Node target = ReplicaPlacement.choose(targets, group.getMembersList().stream().filter(n -> !n.getNodeId().equals(source.getNodeId())).toList(), counts);
                if (!failed && !draining.contains(source.getNodeId()) && counts.getOrDefault(source.getNodeId(), 0) - counts.getOrDefault(target.getNodeId(), 0) < 2) continue;
                List<Node> desired = new ArrayList<>(group.getMembersList());
                desired.set(desired.indexOf(source), target);
                String taskId = "auto_" + UUID.randomUUID();
                ReplicaTask task = ReplicaTask.newBuilder().setTaskId(taskId).setBefore(group)
                        .setAfter(group.toBuilder().clearMembers().addAllMembers(desired)).setStage("CREATED").build();
                storage.write(0, 0, StorageGateway.batch("replica.task/" + taskId).addConditions(StorageGateway.condition(key(taskId), null))
                        .addMutations(TransactionEngine.put(key(taskId), task.toByteString())).build());
                // 只登记，由 tick 一步步推进。
                return;
            }
        }
    }

    /** 测试和单机工具用：一直推进直到没有未结束的任务和作业。 */
    public void runUntilIdle(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            tick();
            if (!busy()) return;
            if (System.nanoTime() > deadline) {
                StringBuilder detail = new StringBuilder();
                for (ReplicaTask task : tasks("tasks/replica/"))
                    if (!TERMINAL.contains(task.getStage())) detail.append(" [").append(task.getTaskId()).append(' ').append(task.getStage())
                            .append(task.getError().isBlank() ? "" : " " + task.getError()).append(']');
                for (Job job : jobs()) if (job.active()) detail.append(" [作业 ").append(job.id()).append(' ').append(job.state()).append(']');
                throw new ClusterException("TIMEOUT", "副本任务没有在限定时间内完成：" + detail);
            }
            Thread.sleep(200);
        }
    }

    // ---- 存取 ----

    private ReplicaTask stage(ReplicaTask previous, String stage) {
        ReplicaTask next = previous.toBuilder().setStage(stage).setError("").build();
        try { storage.write(0, 0, StorageGateway.batch("replica.stage/" + previous.getTaskId() + "/" + stage + "/" + UUID.randomUUID())
                .addConditions(StorageGateway.condition(key(previous.getTaskId()), previous.toByteString())).addMutations(TransactionEngine.put(key(previous.getTaskId()), next.toByteString())).build()); }
        catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        return parse(storage.get(0, 0, key(previous.getTaskId())));
    }

    /** 记下最近一次失败，给页面显示。任务不会因此终止，下一轮继续重试。 */
    private ReplicaTask error(ReplicaTask task, Exception ex) {
        return error(task, ex.getMessage() == null ? ex.toString() : ex.getMessage());
    }
    private ReplicaTask error(ReplicaTask task, String message) {
        if (message.length() > 500) message = message.substring(0, 500);
        if (message.equals(task.getError())) return task;
        ReplicaTask next = task.toBuilder().setError(message).build();
        try {
            storage.write(0, 0, StorageGateway.batch("replica.error/" + task.getTaskId() + "/" + UUID.randomUUID())
                    .addConditions(StorageGateway.condition(key(task.getTaskId()), task.toByteString()))
                    .addMutations(TransactionEngine.put(key(task.getTaskId()), next.toByteString())).build());
            return next;
        } catch (RuntimeException ignored) { return task; }
    }

    private void save(Job old, Job next) {
        ByteString key = jobKey(old.graphId());
        ByteString current = storage.get(0, 0, key);
        if (current == null || !parseJob(current).equals(old)) return;
        try {
            storage.write(0, 0, StorageGateway.batch("replica.job/" + old.id() + "/" + next.state() + "/" + UUID.randomUUID())
                    .addConditions(StorageGateway.condition(key, current)).addMutations(TransactionEngine.put(key, jobBytes(next))).build());
        } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
    }

    private List<ReplicaTask> tasks(String prefix) {
        List<ReplicaTask> result = new ArrayList<>();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text(prefix), after, 100, cursor);
            for (var item : page.getEntriesList()) result.add(parse(item.getValue()));
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (true);
        return result;
    }

    private List<Job> jobs() {
        List<Job> result = new ArrayList<>();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("tasks/replicaJob/"), after, 100, cursor);
            for (var item : page.getEntriesList()) result.add(parseJob(item.getValue()));
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (true);
        return result;
    }

    private static Job jobOf(ReplicaTask task, Map<Long, Job> jobs) {
        Job job = jobs.get(task.getBefore().getGraphId());
        return job != null && task.getTaskId().startsWith(job.id() + "/") ? job : null;
    }

    static List<Node> added(ReplicaTask task) {
        Set<String> before = ids(task.getBefore().getMembersList());
        return task.getAfter().getMembersList().stream().filter(n -> !before.contains(n.getNodeId())).toList();
    }
    private static Set<String> ids(List<Node> nodes) {
        Set<String> ids = new HashSet<>();
        for (Node node : nodes) ids.add(node.getNodeId());
        return ids;
    }
    private static int part(ReplicaTask task) { return task.getBefore().getPartId(); }
    private static ByteString activeKey(DataGroupSpec group) {
        return TransactionEngine.text("tasks/activeReplica/" + group.getGraphId() + "/" + group.getPartId());
    }
    private static ByteString key(String task) { return TransactionEngine.text("tasks/replica/" + task); }
    static ByteString jobKey(long graphId) { return TransactionEngine.text("tasks/replicaJob/" + graphId); }
    private static ByteString jobBytes(Job job) {
        try { return ByteString.copyFrom(JSON.writeValueAsBytes(job)); }
        catch (java.io.IOException ex) { throw new IllegalStateException(ex); }
    }
    static Job parseJob(ByteString bytes) {
        try { return JSON.readValue(bytes.toByteArray(), Job.class); }
        catch (java.io.IOException ex) { throw new ClusterException("CORRUPT_DATA", "副本作业损坏", ex); }
    }
    private static DataGroupSpec spec(ByteString bytes) {
        try { return DataGroupSpec.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "分片定义损坏", ex); }
    }
    private static ReplicaTask parse(ByteString bytes) {
        try { return ReplicaTask.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "副本任务损坏", ex); }
    }
}
