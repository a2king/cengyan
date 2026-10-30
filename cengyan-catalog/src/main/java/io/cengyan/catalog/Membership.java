package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;
import io.cengyan.store.StoreOptions;
import io.cengyan.store.WriteGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 存储节点、副本和计算节点。目录记在本机库的 catalog 列族。
 * 查询仍读本机库。副本是检查点拷贝，搬迁目标另开一份库，两边都能按前缀读到边。
 * 有 3 个投票成员以后，写入先进入提交日志，多数派确认后再写 RocksDB。
 */
public final class Membership implements WriteGate {
    private static final Logger log = LoggerFactory.getLogger(Membership.class);
    private static final int LOCAL_NODE = 1;

    private final Catalog catalog;
    private final GraphStore local;
    private final Path home;
    private final StoreOptions options;
    private final ConcurrentHashMap<Integer, GraphStore> stores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Boolean> voters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Compute> computes = new ConcurrentHashMap<>();
    private final Object schedule = new Object();
    private volatile Runnable afterReadonly;
    private volatile Runnable duringMove;
    private volatile boolean crashBeforePromote;

    private Membership(Catalog catalog, GraphStore local, Path home, StoreOptions options) {
        this.catalog = catalog;
        this.local = local;
        this.home = home;
        this.options = options;
    }

    public static Membership open(Catalog catalog, GraphStore local, String address, StoreOptions options) {
        if (catalog == null || local == null) {
            throw new IllegalArgumentException("目录和存储不能为空");
        }
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("本机地址不能为空");
        }
        if (options == null) {
            throw new IllegalArgumentException("打开参数不能为空");
        }
        Membership membership = new Membership(catalog, local, local.directory(), options);
        membership.stores.put(LOCAL_NODE, local);
        membership.ensureLocal(address);
        membership.reopenVoters();
        local.setWriteGate(membership);
        return membership;
    }

    /** 页面上的副本数只计投票成员，学习者不算。 */
    public int voterCount(String graph) {
        return votersOf(catalog.graph(graph).graphId()).size();
    }

    public GraphStore store(int nodeId) {
        GraphStore found = stores.get(nodeId);
        if (found == null) {
            throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "节点没有打开：" + nodeId);
        }
        return found;
    }

    /** 测试用：拷贝完成后、提升之前让这次加副本失败。 */
    public void crashBeforePromote() {
        this.crashBeforePromote = true;
    }

    /** 空间进入只读之后调用。用来观察同步期间的写入被拒绝、读取仍可用。 */
    public void afterReadonly(Runnable hook) {
        this.afterReadonly = hook;
    }

    /** 某个分片标成搬迁中之后调用。此时只有这个分片拒绝写入。 */
    public void duringMove(Runnable hook) {
        this.duringMove = hook;
    }

    public void ensureVotes(int graphId) {
        local.withWriteLock(() -> {
            int parts = catalog.partCount(graphId);
            try (ReadSession session = local.read()) {
                List<Integer> holders = new ArrayList<>();
                for (int part = 0; part < parts; part++) {
                    List<Integer> partVoters = voterIds(session, graphId, part);
                    if (partVoters.isEmpty()) {
                        if (holders.isEmpty()) {
                            holders.add(LOCAL_NODE);
                        }
                        StoreBatch batch = new StoreBatch();
                        for (int nodeId : holders) {
                            batch.put(Cf.CATALOG, KeyCodec.partVoteKey(graphId, part, nodeId),
                                    ClusterCodec.vote(ClusterCodec.VOTE_VOTER));
                        }
                        local.mutate(batch);
                        voters.put(LOCAL_NODE, true);
                    } else if (holders.isEmpty()) {
                        holders.addAll(partVoters);
                    }
                }
            }
            return null;
        });
    }

    public List<Integer> addReplica(String graph, int replicas) {
        int graphId = catalog.graph(graph).graphId();
        ensureVotes(graphId);
        int current = votersOf(graphId).size();
        if (replicas <= current) {
            throw new CengyanException(ErrorCode.SYNTAX, "副本数必须大于当前投票成员数");
        }
        catalog.markReplicaSync(graphId);
        Runnable hook = afterReadonly;
        afterReadonly = null;
        if (hook != null) {
            hook.run();
        }
        List<Integer> created = new ArrayList<>();
        boolean promoted = false;
        try {
            local.flush();
            int parts = catalog.partCount(graphId);
            for (int ordinal = current; ordinal < replicas; ordinal++) {
                int nodeId = nextNodeId();
                Path dir = nodeDir(nodeId);
                try {
                    Files.createDirectories(dir.getParent());
                } catch (IOException ex) {
                    throw new CengyanException(ErrorCode.INTERNAL, "无法创建节点目录：" + ex.getMessage(), ex);
                }
                local.checkpoint(dir);
                GraphStore opened = local.openReplica(dir);
                stores.put(nodeId, opened);
                created.add(nodeId);
                putNode(nodeId, ClusterCodec.ROLE_STORAGE, ClusterCodec.STATE_LEARNER, "checkpoint");
                StoreBatch votes = new StoreBatch();
                for (int part = 0; part < parts; part++) {
                    votes.put(Cf.CATALOG, KeyCodec.partVoteKey(graphId, part, nodeId),
                            ClusterCodec.vote(ClusterCodec.VOTE_LEARNER));
                }
                local.mutate(votes);
            }
            verifyEdges(graphId, created);
            if (crashBeforePromote) {
                crashBeforePromote = false;
                throw new CengyanException(ErrorCode.INTERNAL, "提升前源进程已退出");
            }
            promote(graphId, created, replicas);
            promoted = true;
            log.info("图 {} 的投票副本数已是 {}", graph, replicas);
            return List.copyOf(votersOf(graphId));
        } finally {
            if (!promoted) {
                for (int nodeId : created) {
                    GraphStore opened = stores.remove(nodeId);
                    if (opened != null && opened != local) {
                        opened.close();
                    }
                    deleteQuietly(nodeDir(nodeId));
                }
            }
        }
    }

    public int addStorage(String address) {
        String location = requireAddress(address);
        int nodeId = nextNodeId();
        GraphStore opened = local.openReplica(nodeDir(nodeId));
        stores.put(nodeId, opened);
        putNode(nodeId, ClusterCodec.ROLE_STORAGE, ClusterCodec.STATE_ACTIVE, location);
        for (String graph : catalog.graphs()) {
            int graphId = catalog.graph(graph).graphId();
            ensureVotes(graphId);
            int parts = Math.max(1, catalog.partCount(graphId));
            movePart(graphId, Math.floorMod(nodeId - 2, parts), nodeId);
        }
        log.info("已加入存储节点 {}，地址 {}", nodeId, location);
        return nodeId;
    }

    public void drainStorage(int nodeId) {
        if (nodeId == LOCAL_NODE && soleVoter(nodeId)) {
            throw new CengyanException(ErrorCode.NODE_NOT_DRAINED, "该存储节点是某个分片的唯一投票成员");
        }
        if (soleVoter(nodeId)) {
            throw new CengyanException(ErrorCode.NODE_NOT_DRAINED, "该存储节点是某个分片的唯一投票成员");
        }
        byte[] raw = nodeRecord(nodeId);
        if (ClusterCodec.nodeRole(raw) != ClusterCodec.ROLE_STORAGE) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这不是存储节点");
        }
        putNode(nodeId, ClusterCodec.ROLE_STORAGE, ClusterCodec.STATE_DRAINING, ClusterCodec.nodeAddress(raw));
        log.info("存储节点 {} 开始下线", nodeId);
    }

    public void removeStorage(int nodeId) {
        if (nodeId == LOCAL_NODE) {
            throw new CengyanException(ErrorCode.NODE_NOT_DRAINED, "不能移除本机存储");
        }
        byte[] raw = nodeRecord(nodeId);
        if (ClusterCodec.nodeState(raw) != ClusterCodec.STATE_DRAINING || hasVote(nodeId)) {
            throw new CengyanException(ErrorCode.NODE_NOT_DRAINED, "节点还没有排空");
        }
        StoreBatch batch = new StoreBatch();
        batch.delete(Cf.CATALOG, KeyCodec.nodeKey(nodeId));
        local.mutate(batch);
        voters.remove(nodeId);
        GraphStore opened = stores.remove(nodeId);
        if (opened != null && opened != local) {
            opened.close();
        }
        log.info("已移除存储节点 {}", nodeId);
    }

    public int addCompute(String address) {
        String location = requireAddress(address);
        checkHealth(location);
        int nodeId = nextNodeId();
        putNode(nodeId, ClusterCodec.ROLE_COMPUTE, ClusterCodec.STATE_ACTIVE, location);
        computes.put(nodeId, new Compute(nodeId));
        log.info("已加入计算节点 {}，地址 {}", nodeId, location);
        return nodeId;
    }

    public void drainCompute(int nodeId) {
        Compute compute = requireCompute(nodeId);
        byte[] raw = nodeRecord(nodeId);
        putNode(nodeId, ClusterCodec.ROLE_COMPUTE, ClusterCodec.STATE_DRAINING, ClusterCodec.nodeAddress(raw));
        synchronized (schedule) {
            compute.state = ClusterCodec.STATE_DRAINING;
        }
        log.info("计算节点 {} 不再接收新会话", nodeId);
    }

    public void removeCompute(int nodeId) {
        Compute compute = requireCompute(nodeId);
        synchronized (schedule) {
            if (compute.pins > 0 || compute.active > 0) {
                throw new CengyanException(ErrorCode.NODE_NOT_DRAINED, "计算节点仍有绑定的会话或正在执行的查询");
            }
        }
        StoreBatch batch = new StoreBatch();
        batch.delete(Cf.CATALOG, KeyCodec.nodeKey(nodeId));
        local.mutate(batch);
        computes.remove(nodeId);
        log.info("已移除计算节点 {}", nodeId);
    }

    /** 已经绑定的会话返回原来的节点。没有计算节点时返回 0，查询留在本进程。 */
    public int choose(int current) {
        synchronized (schedule) {
            if (current != 0) {
                return current;
            }
            Compute best = null;
            for (Compute compute : computes.values()) {
                if (compute.state != ClusterCodec.STATE_ACTIVE) {
                    continue;
                }
                if (best == null || lighter(compute, best)) {
                    best = compute;
                }
            }
            return best == null ? 0 : best.id;
        }
    }

    public void notePin(int nodeId) {
        synchronized (schedule) {
            Compute compute = computes.get(nodeId);
            if (compute != null) {
                compute.pins++;
            }
        }
    }

    public void enter(int nodeId) {
        synchronized (schedule) {
            Compute compute = computes.get(nodeId);
            if (compute != null) {
                compute.active++;
            }
        }
    }

    public void leave(int nodeId) {
        synchronized (schedule) {
            Compute compute = computes.get(nodeId);
            if (compute != null && compute.active > 0) {
                compute.active--;
            }
        }
    }

    /** 测试上报负载。选择顺序是正在执行的查询数、已计入内存、CPU、节点号。 */
    public void report(int nodeId, int running, long memory, double cpu) {
        synchronized (schedule) {
            Compute compute = requireCompute(nodeId);
            compute.baseRunning = running;
            compute.baseMemory = memory;
            compute.cpu = cpu;
        }
    }

    @Override
    public void replicate(GraphStore primary, StoreBatch batch) {
        List<GraphStore> voting = votingStores();
        if (voting.size() < 3) {
            primary.applyLocal(batch);
            for (GraphStore voter : voting) {
                if (voter == primary) {
                    continue;
                }
                try {
                    voter.applyLocal(batch);
                } catch (RuntimeException ex) {
                    log.warn("副本写入失败：{}", ex.toString());
                }
            }
            return;
        }
        int majority = voting.size() / 2 + 1;
        int logged = 0;
        for (GraphStore voter : voting) {
            try {
                appendLog(voter, batch);
                logged++;
            } catch (RuntimeException ex) {
                log.warn("提交日志写入失败：{}", ex.toString());
            }
        }
        if (logged < majority) {
            throw new CengyanException(ErrorCode.INTERNAL, "提交日志没有达到多数派");
        }
        int applied = 0;
        RuntimeException last = null;
        for (GraphStore voter : voting) {
            try {
                voter.applyLocal(batch);
                applied++;
            } catch (RuntimeException ex) {
                last = ex;
                log.warn("投票成员写入失败：{}", ex.toString());
            }
        }
        if (applied < majority) {
            if (last instanceof CengyanException cengyan) {
                throw cengyan;
            }
            throw new CengyanException(ErrorCode.INTERNAL, "多数派没有写入成功", last);
        }
    }

    public void close() {
        local.setWriteGate(null);
        for (GraphStore opened : stores.values()) {
            if (opened != local) {
                opened.close();
            }
        }
        stores.clear();
    }

    private void promote(int graphId, List<Integer> created, int replicas) {
        int parts = catalog.partCount(graphId);
        StoreBatch batch = new StoreBatch();
        for (int nodeId : created) {
            byte[] raw = nodeRecord(nodeId);
            batch.put(Cf.CATALOG, KeyCodec.nodeKey(nodeId),
                    ClusterCodec.node(ClusterCodec.ROLE_STORAGE, ClusterCodec.STATE_ACTIVE,
                            ClusterCodec.nodeAddress(raw)));
            for (int part = 0; part < parts; part++) {
                batch.put(Cf.CATALOG, KeyCodec.partVoteKey(graphId, part, nodeId),
                        ClusterCodec.vote(ClusterCodec.VOTE_VOTER));
            }
        }
        local.mutate(batch);
        catalog.finishReplica(graphId, replicas);
        for (int nodeId : created) {
            voters.put(nodeId, true);
        }
    }

    private void movePart(int graphId, int partId, int target) {
        int source = soleSource(graphId, partId);
        if (source == target) {
            return;
        }
        catalog.markMoving(graphId, partId);
        try {
            Runnable hook = duringMove;
            duringMove = null;
            if (hook != null) {
                hook.run();
            }
            GraphStore targetStore = stores.get(target);
            if (targetStore == null) {
                throw new CengyanException(ErrorCode.INTERNAL, "搬迁目标没有打开");
            }
            copyPart(graphId, partId, targetStore);
            catalog.finishMove(graphId, partId, source, target);
            voters.put(target, true);
            if (!hasVote(source)) {
                voters.remove(source);
                GraphStore sourceStore = stores.get(source);
                if (sourceStore != null && sourceStore != targetStore) {
                    sourceStore.deletePartition(graphId, partId);
                }
                if (source == LOCAL_NODE) {
                    voters.put(LOCAL_NODE, true);
                }
            }
        } catch (RuntimeException ex) {
            catalog.clearMoving(graphId, partId);
            throw ex;
        }
    }

    private void copyPart(int graphId, int partId, GraphStore target) {
        byte[] prefix = Arrays.copyOf(KeyCodec.vertexPrefix(graphId, partId, 0L), 4);
        StoreBatch batch = new StoreBatch();
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.VERTEX, prefix)) {
                byte[] value = session.get(Cf.VERTEX, key);
                if (value != null) {
                    batch.put(Cf.VERTEX, key, value);
                }
            }
            for (byte[] key : session.keysWithPrefix(Cf.EDGE, prefix)) {
                batch.put(Cf.EDGE, key, new byte[0]);
                if (key.length != KeyCodec.EDGE_KEY_LENGTH) {
                    continue;
                }
                KeyCodec.DecodedEdge decoded = KeyCodec.decodeEdge(key);
                if (decoded.direction() != Direction.OUT.code()) {
                    continue;
                }
                byte[] propKey = KeyCodec.propKey(
                        graphId, decoded.edgeTypeId(), decoded.rank(), decoded.src(), decoded.dst());
                byte[] prop = session.get(Cf.PROP, propKey);
                if (prop != null) {
                    batch.put(Cf.PROP, propKey, prop);
                }
            }
        }
        if (!batch.isEmpty()) {
            target.applyLocal(batch);
        }
    }

    private void verifyEdges(int graphId, List<Integer> created) {
        byte[] prefix = KeyCodec.graphIdPrefix(graphId);
        try (ReadSession source = local.read()) {
            List<byte[]> expected = sorted(source.keysWithPrefix(Cf.EDGE, prefix));
            for (int nodeId : created) {
                try (ReadSession copy = stores.get(nodeId).read()) {
                    List<byte[]> actual = sorted(copy.keysWithPrefix(Cf.EDGE, prefix));
                    if (expected.size() != actual.size()) {
                        throw new CengyanException(ErrorCode.INTERNAL, "副本键范围不一致");
                    }
                    for (int i = 0; i < expected.size(); i++) {
                        if (!Arrays.equals(expected.get(i), actual.get(i))) {
                            throw new CengyanException(ErrorCode.INTERNAL, "副本键范围不一致");
                        }
                    }
                }
            }
        }
    }

    private static List<byte[]> sorted(List<byte[]> keys) {
        List<byte[]> copy = new ArrayList<>(keys);
        copy.sort(KeyCodec::compare);
        return copy;
    }

    private List<GraphStore> votingStores() {
        List<GraphStore> found = new ArrayList<>();
        for (Integer nodeId : voters.keySet()) {
            GraphStore opened = stores.get(nodeId);
            if (opened != null) {
                found.add(opened);
            }
        }
        if (found.stream().noneMatch(opened -> opened == local)) {
            found.add(local);
        }
        return found;
    }

    private Set<Integer> votersOf(int graphId) {
        Set<Integer> ids = new HashSet<>();
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.partVotePrefix(graphId))) {
                if (key.length != 7) {
                    continue;
                }
                byte[] value = session.get(Cf.CATALOG, key);
                if (value != null && ClusterCodec.voteKind(value) == ClusterCodec.VOTE_VOTER) {
                    ids.add(KeyCodec.voteNodeId(key));
                }
            }
        }
        return ids;
    }

    private static List<Integer> voterIds(ReadSession session, int graphId, int partId) {
        List<Integer> ids = new ArrayList<>();
        for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.partVotePrefix(graphId, partId))) {
            if (key.length != 7) {
                continue;
            }
            byte[] value = session.get(Cf.CATALOG, key);
            if (value != null && ClusterCodec.voteKind(value) == ClusterCodec.VOTE_VOTER) {
                ids.add(KeyCodec.voteNodeId(key));
            }
        }
        return ids;
    }

    private int soleSource(int graphId, int partId) {
        try (ReadSession session = local.read()) {
            List<Integer> ids = voterIds(session, graphId, partId);
            if (ids.isEmpty()) {
                return LOCAL_NODE;
            }
            ids.sort(Comparator.naturalOrder());
            return ids.get(0);
        }
    }

    private boolean soleVoter(int nodeId) {
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, new byte[] {0x51})) {
                if (key.length != 7 || KeyCodec.voteNodeId(key) != nodeId) {
                    continue;
                }
                byte[] value = session.get(Cf.CATALOG, key);
                if (value == null || ClusterCodec.voteKind(value) != ClusterCodec.VOTE_VOTER) {
                    continue;
                }
                int graphId = KeyCodec.graphIdFromMarked(key);
                int partId = KeyCodec.votePartId(key);
                List<Integer> ids = voterIds(session, graphId, partId);
                if (ids.size() == 1 && ids.get(0) == nodeId) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasVote(int nodeId) {
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, new byte[] {0x51})) {
                if (key.length == 7 && KeyCodec.voteNodeId(key) == nodeId) {
                    return true;
                }
            }
        }
        return false;
    }

    private void ensureLocal(String address) {
        try (ReadSession session = local.read()) {
            if (session.get(Cf.CATALOG, KeyCodec.nodeKey(LOCAL_NODE)) != null) {
                return;
            }
        }
        putNode(LOCAL_NODE, ClusterCodec.ROLE_STORAGE, ClusterCodec.STATE_ACTIVE, address);
        voters.put(LOCAL_NODE, true);
    }

    private void reopenVoters() {
        Set<Integer> ids = new HashSet<>();
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, new byte[] {0x51})) {
                if (key.length != 7) {
                    continue;
                }
                byte[] value = session.get(Cf.CATALOG, key);
                if (value != null && ClusterCodec.voteKind(value) == ClusterCodec.VOTE_VOTER) {
                    ids.add(KeyCodec.voteNodeId(key));
                }
            }
        }
        for (int nodeId : ids) {
            voters.put(nodeId, true);
            if (nodeId == LOCAL_NODE || stores.containsKey(nodeId)) {
                continue;
            }
            Path dir = nodeDir(nodeId);
            if (Files.isDirectory(dir)) {
                stores.put(nodeId, local.openReplica(dir));
            }
        }
        if (voters.isEmpty()) {
            voters.put(LOCAL_NODE, true);
        }
    }

    private void putNode(int nodeId, int role, int state, String address) {
        StoreBatch batch = new StoreBatch();
        batch.put(Cf.CATALOG, KeyCodec.nodeKey(nodeId), ClusterCodec.node(role, state, address));
        local.mutate(batch);
    }

    private byte[] nodeRecord(int nodeId) {
        try (ReadSession session = local.read()) {
            byte[] raw = session.get(Cf.CATALOG, KeyCodec.nodeKey(nodeId));
            if (raw == null) {
                throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "节点不存在：" + nodeId);
            }
            return raw;
        }
    }

    private int nextNodeId() {
        int max = LOCAL_NODE;
        try (ReadSession session = local.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.nodePrefix())) {
                if (key.length == 3) {
                    max = Math.max(max, KeyCodec.nodeIdOf(key));
                }
            }
        }
        if (max >= 65535) {
            throw new CengyanException(ErrorCode.INTERNAL, "节点编号已经用完");
        }
        return max + 1;
    }

    private Path nodeDir(int nodeId) {
        return home.resolve("nodes").resolve(Integer.toString(nodeId));
    }

    private Compute requireCompute(int nodeId) {
        Compute compute = computes.get(nodeId);
        if (compute == null) {
            byte[] raw = nodeRecord(nodeId);
            if (ClusterCodec.nodeRole(raw) != ClusterCodec.ROLE_COMPUTE) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这不是计算节点");
            }
            compute = new Compute(nodeId);
            compute.state = ClusterCodec.nodeState(raw);
            computes.put(nodeId, compute);
        }
        return compute;
    }

    private static boolean lighter(Compute left, Compute right) {
        int leftRunning = left.baseRunning + left.active;
        int rightRunning = right.baseRunning + right.active;
        if (leftRunning != rightRunning) {
            return leftRunning < rightRunning;
        }
        if (left.baseMemory != right.baseMemory) {
            return left.baseMemory < right.baseMemory;
        }
        if (Double.compare(left.cpu, right.cpu) != 0) {
            return left.cpu < right.cpu;
        }
        return left.id < right.id;
    }

    private static String requireAddress(String address) {
        if (address == null || address.isBlank() || address.lastIndexOf(':') <= 0) {
            throw new CengyanException(ErrorCode.SYNTAX, "地址要写成主机:端口");
        }
        return address.trim();
    }

    private static void checkHealth(String address) {
        int colon = address.lastIndexOf(':');
        String host = address.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException ex) {
            throw new CengyanException(ErrorCode.SYNTAX, "地址要写成主机:端口");
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 500);
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "计算节点健康检查失败：" + address);
        }
    }

    private static void appendLog(GraphStore node, StoreBatch batch) {
        try {
            Path dir = node.directory().resolve("commit-log");
            Files.createDirectories(dir);
            long seq;
            try (var listed = Files.list(dir)) {
                seq = listed.count() + 1;
            }
            Files.write(dir.resolve(seq + ".bin"), encode(batch));
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "提交日志写入失败：" + ex.getMessage(), ex);
        }
    }

    private static byte[] encode(StoreBatch batch) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            for (StoreBatch.Op op : batch.operations()) {
                out.writeByte(op.family().ordinal());
                out.writeByte(op.delete() ? 1 : 0);
                out.writeInt(op.key().length);
                out.write(op.key());
                out.writeInt(op.value().length);
                out.write(op.value());
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "提交日志编码失败", ex);
        }
    }

    private static void deleteQuietly(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException ex) {
            log.warn("删除未提升的副本目录失败：{}", ex.toString());
        }
    }

    private static final class Compute {
        private final int id;
        private int state = ClusterCodec.STATE_ACTIVE;
        private int baseRunning;
        private long baseMemory;
        private double cpu;
        private int active;
        private int pins;

        private Compute(int id) {
            this.id = id;
        }
    }
}
