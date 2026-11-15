package io.cengyan.distributed;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import java.time.Duration;
import java.util.*;

/** 仅 Metadata Leader 调用的持久化副本变更控制器，复制本身完全交给 Ratis。 */
public final class ReplicaController {
    private final StorageGateway storage;
    private final DistributedCatalog catalog;
    public ReplicaController(StorageGateway storage) { this.storage = storage; catalog = new DistributedCatalog(storage); }

    public void resize(String requestId, String graphName, int replicas) {
        GraphDefinition graph = catalog.graph(graphName);
        DistributedCatalog.requireWritable(graph);
        List<Node> nodes = catalog.placementNodes();
        if (replicas < 1 || replicas > nodes.size()) throw new ClusterException("INSUFFICIENT_NODES", "副本数超出存储节点数量");
        for (int part = 0; part < graph.getPartitions(); part++) {
            DataGroupSpec old = spec(storage.get(0, 0, StorageGateway.groupKey(graph.getId(), part)));
            List<Node> next = new ArrayList<>(old.getMembersList());
            while (next.size() < replicas) next.add(ReplicaPlacement.choose(nodes, next, Map.of()));
            while (next.size() > replicas) next.removeLast();
            if (!next.equals(old.getMembersList())) {
                String taskId = requestId + "/" + part;
                ReplicaTask task = ReplicaTask.newBuilder().setTaskId(taskId).setBefore(old)
                        .setAfter(old.toBuilder().clearMembers().addAllMembers(next)).setStage("CREATED").build();
                ByteString key = key(taskId), existing = storage.get(0, 0, key);
                if (existing == null) storage.write(0, 0, StorageGateway.batch("replica.task/" + taskId).addConditions(StorageGateway.condition(key, null))
                        .addMutations(TransactionEngine.put(key, task.toByteString())).build());
                else task = parse(existing);
                resume(task);
            }
        }
        GraphDefinition latest = catalog.graph(graphName);
        catalog.update(requestId + "/factor", latest, latest.toBuilder().setReplicas(replicas).build());
    }

    public void resume(ReplicaTask initial) {
        ReplicaTask task = initial;
        if (task.getStage().equals("FINISHED")) { release(task); return; }
        var before = task.getBefore(); var after = task.getAfter();
        ByteString activeKey = TransactionEngine.text("tasks/activeReplica/" + before.getGraphId() + "/" + before.getPartId());
        ByteString owner = storage.get(0, 0, activeKey);
        if (owner == null) {
            storage.write(0, 0, StorageGateway.batch("replica.claim/" + task.getTaskId())
                    .addConditions(StorageGateway.condition(activeKey, null))
                    .addConditions(StorageGateway.condition(StorageGateway.groupKey(before.getGraphId(), before.getPartId()), before.toByteString()))
                    .addMutations(TransactionEngine.put(activeKey, TransactionEngine.text(task.getTaskId()))).build());
        } else if (!owner.toStringUtf8().equals(task.getTaskId())) {
            throw new ClusterException("TOPOLOGY_CONFLICT", "分片已由另一副本任务占用");
        }
        List<Node> added = after.getMembersList().stream().filter(node -> before.getMembersList().stream().noneMatch(old -> old.getNodeId().equals(node.getNodeId()))).toList();
        if (task.getStage().equals("CREATED")) {
            var staging = before.toBuilder().clearLearners().addAllLearners(added).setControlTaskId(task.getTaskId()).build();
            for (Node target : added) {
                var response = storage.direct(DistributedCatalog.address(target), "data.addGroup", staging.toByteString(), Duration.ofSeconds(30));
                if (!response.getCode().equals("OK")) throw new ClusterException(response.getCode(), response.getMessage());
            }
            if (!added.isEmpty()) storage.call(before.getGraphId(), before.getPartId(), "data.configure", task.getTaskId(), staging.toByteString(), Duration.ofSeconds(60));
            task = stage(task, "CATCHING_UP");
        }
        if (task.getStage().equals("CATCHING_UP")) {
            // Ratis 的 reconfiguration 在提升成员前负责 staging catch-up，不能仅看本地已应用数就自行切成员。
            storage.call(before.getGraphId(), before.getPartId(), "data.configure", task.getTaskId(), after.toBuilder().setControlTaskId(task.getTaskId()).build().toByteString(), Duration.ofSeconds(60));
            task = stage(task, "UPDATING_TOPOLOGY");
        }
        if (task.getStage().equals("UPDATING_TOPOLOGY")) {
            ByteString route = StorageGateway.groupKey(before.getGraphId(), before.getPartId());
            ByteString current = storage.get(0, 0, route);
            if (!after.toByteString().equals(current)) {
                if (!before.toByteString().equals(current)) throw new ClusterException("TOPOLOGY_CONFLICT", "副本任务的原始路由已经改变，拒绝覆盖后续拓扑");
                storage.write(0, 0, StorageGateway.batch("replica.route/" + task.getTaskId())
                        .addConditions(StorageGateway.condition(route, before.toByteString())).addMutations(TransactionEngine.put(route, after.toByteString())).build());
            }
            storage.route(before.getGraphId(), before.getPartId(), after.getMembersList().stream().map(DistributedCatalog::address).toList());
            task = stage(task, "FINISHED");
            release(task);
        }
    }
    private void release(ReplicaTask task) {
        ByteString activeKey = TransactionEngine.text("tasks/activeReplica/" + task.getBefore().getGraphId() + "/" + task.getBefore().getPartId());
        ByteString owner = storage.get(0, 0, activeKey);
        if (owner == null || !owner.toStringUtf8().equals(task.getTaskId())) return;
        try {
            storage.write(0, 0, StorageGateway.batch("replica.release/" + task.getTaskId())
                    .addConditions(StorageGateway.condition(activeKey, owner))
                    .addMutations(Mutation.newBuilder().setKey(activeKey).setDelete(true)).build());
        } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
    }
    public void drain(String requestId, String nodeId) {
        Node node = catalog.nodes().stream().filter(n -> n.getNodeId().equals(nodeId)).findFirst()
                .orElseThrow(() -> new ClusterException("NODE_NOT_FOUND", "存储节点不存在"));
        ByteString key = TransactionEngine.text("nodes/draining/" + nodeId);
        storage.write(0, 0, StorageGateway.batch("node.drain/" + requestId).addMutations(TransactionEngine.put(key, TransactionEngine.text("true"))).build());
    }
    public void recover() {
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("tasks/replica/"), after, 100, cursor);
            for (var item : page.getEntriesList()) resume(parse(item.getValue()));
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (true);
    }
    /** Leader 周期调用。每轮最多迁移一个分片，修复优先，避免新增节点触发无界复制。 */
    public void reconcile(long recoveryTimeoutMs) {
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
                resume(task);
                return;
            }
        }
    }
    private ReplicaTask stage(ReplicaTask previous, String stage) {
        ReplicaTask next = previous.toBuilder().setStage(stage).build();
        try { storage.write(0, 0, StorageGateway.batch("replica.stage/" + previous.getTaskId() + "/" + stage)
                .addConditions(StorageGateway.condition(key(previous.getTaskId()), previous.toByteString())).addMutations(TransactionEngine.put(key(previous.getTaskId()), next.toByteString())).build()); }
        catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
        return parse(storage.get(0, 0, key(previous.getTaskId())));
    }
    private static ByteString key(String task) { return TransactionEngine.text("tasks/replica/" + task); }
    private static DataGroupSpec spec(ByteString bytes) {
        try { return DataGroupSpec.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "分片定义损坏", ex); }
    }
    private static ReplicaTask parse(ByteString bytes) {
        try { return ReplicaTask.parseFrom(bytes); } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "副本任务损坏", ex); }
    }
}
