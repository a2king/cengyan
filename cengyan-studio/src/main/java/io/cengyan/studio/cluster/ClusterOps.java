package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.DistributedCatalog;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.studio.cluster.ClusterState.NodeSpec;
import io.cengyan.studio.cluster.ClusterState.Role;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * 集群内部管理调用。Storage 侧（维护模式、成员、Leader 转移、排空）直接走 Storage RPC；
 * Compute 侧（排空、移除、健康检查）走 Compute 客户端 JSON 协议，以数据库 root 登录。
 */
class ClusterOps {
    private static final Duration RPC = Duration.ofSeconds(30);

    // ---- Storage ----
    private StorageGateway gateway(ClusterState state) {
        List<InetSocketAddress> seeds = state.nodes(Role.STORAGE).stream().filter(NodeSpec::joined)
                .map(n -> new InetSocketAddress(ConfigRenderer.host(state, n), n.rpcPort())).toList();
        if (seeds.isEmpty()) throw new ClusterError("NO_STORAGE", "集群还没有已加入的存储节点");
        return new StorageGateway(state.clusterId(), seeds);
    }

    private JsonNode storageCall(ClusterState state, String method, ObjectNode body) {
        try (StorageGateway gateway = gateway(state)) {
            var response = gateway.call(0, 0, method, "studio-" + UUID.randomUUID(),
                    ByteString.copyFrom(ClusterStore.JSON.writeValueAsBytes(body)), RPC);
            return response.getBody().isEmpty() ? ClusterStore.JSON.createObjectNode() : ClusterStore.JSON.readTree(response.getBody().toByteArray());
        } catch (ClusterException ex) {
            throw new ClusterError(ex.code(), ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new ClusterError("PROTOCOL_ERROR", "Storage 响应无法解析", ex);
        }
    }

    String enterMaintenance(ClusterState state) {
        return storageCall(state, "metadata.maintenance.enter", ClusterStore.JSON.createObjectNode()).path("id").asText();
    }

    void exitMaintenance(ClusterState state) {
        storageCall(state, "metadata.maintenance.exit", ClusterStore.JSON.createObjectNode());
    }

    JsonNode maintenanceStatus(ClusterState state) {
        return storageCall(state, "metadata.maintenance.status", ClusterStore.JSON.createObjectNode());
    }

    boolean transferLeader(ClusterState state, String nodeId) {
        return storageCall(state, "metadata.transferLeader", ClusterStore.JSON.createObjectNode().put("nodeId", nodeId)).path("transferred").asBoolean();
    }

    /** 已加入 Metadata 的存储节点 ID。 */
    Set<String> storageMembers(ClusterState state) {
        try (StorageGateway gateway = gateway(state)) {
            Set<String> ids = new HashSet<>();
            new DistributedCatalog(gateway).nodes().forEach(n -> ids.add(n.getNodeId()));
            return ids;
        } catch (ClusterException ex) { throw new ClusterError(ex.code(), ex.getMessage(), ex); }
    }

    boolean storageReachable(ClusterState state, NodeSpec node) {
        try (StorageGateway gateway = gateway(state)) {
            return gateway.direct(new InetSocketAddress(ConfigRenderer.host(state, node), node.rpcPort()), "ping", ByteString.EMPTY, Duration.ofSeconds(3))
                    .getCode().equals("OK");
        } catch (RuntimeException ex) { return false; }
    }

    /** 所有活跃图空间中最大的副本数。 */
    int maxReplicas(ClusterState state) {
        try (StorageGateway gateway = gateway(state)) {
            return new DistributedCatalog(gateway).graphs().stream().filter(g -> g.getState().equals("ACTIVE"))
                    .mapToInt(g -> g.getReplicas()).max().orElse(0);
        } catch (ClusterException ex) { throw new ClusterError(ex.code(), ex.getMessage(), ex); }
    }

    void drainStorage(ClusterState state, String nodeId) {
        storageCall(state, "metadata.drain", ClusterStore.JSON.createObjectNode().put("nodeId", nodeId));
    }

    /** @return true 已移除；false 仍有分片在迁移 */
    boolean removeStorage(ClusterState state, String nodeId) {
        try {
            storageCall(state, "metadata.remove", ClusterStore.JSON.createObjectNode().put("nodeId", nodeId));
            return true;
        } catch (ClusterError ex) {
            if (ex.code().equals("NODE_NOT_DRAINED")) return false;
            throw ex;
        }
    }

    // ---- Compute ----
    /** 以 root 登录指定 Compute 的客户端端口并执行 admin 命令。 */
    JsonNode computeAdmin(ClusterState state, NodeSpec via, ObjectNode request, int timeoutMs) throws IOException {
        String host = ConfigRenderer.host(state, via);
        JsonNode login = computeCall(host, via.clientPort(), ClusterStore.JSON.createObjectNode().put("op", "login")
                .put("user", "root").put("password", state.rootPassword()), 10_000);
        if (!"OK".equals(login.path("code").asText())) throw new ClusterError(login.path("code").asText("FAILED"), "Compute 登录失败：" + login.path("message").asText());
        request.put("token", login.path("token").asText());
        JsonNode result = computeCall(host, via.clientPort(), request, timeoutMs);
        if (!"OK".equals(result.path("code").asText())) throw new ClusterError(result.path("code").asText("FAILED"), result.path("message").asText("Compute 命令失败"));
        return result;
    }

    private static ObjectNode admin(String command) {
        return ClusterStore.JSON.createObjectNode().put("op", "admin").put("command", command);
    }

    /** showNodes 中计算节点的状态：ACTIVE / DRAINING / OFFLINE；不在成员中返回 null。 */
    String computeState(ClusterState state, NodeSpec via, String nodeId) throws IOException {
        for (JsonNode row : computeAdmin(state, via, admin("showNodes"), 15_000).path("rows"))
            if (row.path(0).asText().equals(nodeId) && row.path(1).asText().equals("COMPUTE")) return row.path(2).asText();
        return null;
    }

    void drainCompute(ClusterState state, NodeSpec via, String nodeId) throws IOException {
        computeAdmin(state, via, admin("drainCompute").put("nodeId", nodeId), 15_000);
    }

    void resumeCompute(ClusterState state, NodeSpec via, String nodeId) throws IOException {
        computeAdmin(state, via, admin("resumeCompute").put("nodeId", nodeId), 15_000);
    }

    /** 节点上执行、排队、代理中的请求总数；节点不可达时抛出异常。 */
    int computeBusy(ClusterState state, NodeSpec via, String nodeId) throws IOException {
        JsonNode load = computeAdmin(state, via, admin("computeLoad").put("nodeId", nodeId), 15_000);
        return load.path("active").asInt() + load.path("reserved").asInt() + load.path("queued").asInt() + load.path("proxied").asInt();
    }

    /** force 为真时节点不可达也移除。 */
    void removeCompute(ClusterState state, NodeSpec via, String nodeId, boolean force) throws IOException {
        computeAdmin(state, via, admin("removeCompute").put("nodeId", nodeId).put("force", force), 60_000);
    }

    /** 直接连该节点的客户端端口登录并执行一条只读命令。 */
    boolean computeHealthy(ClusterState state, NodeSpec node) {
        try { checkCompute(state, node); return true; }
        catch (RuntimeException ex) { return false; }
    }

    /** 同 computeHealthy，但失败时抛出说明原因的异常，供等待步骤写进任务日志。 */
    void checkCompute(ClusterState state, NodeSpec node) {
        String address = ConfigRenderer.host(state, node) + ":" + node.clientPort();
        try { computeAdmin(state, node, admin("showNodes"), 10_000); }
        catch (java.net.ConnectException ex) { throw new ClusterError("PROGRESS", "客户端端口 " + address + " 拒绝连接（进程未启动或尚未监听）"); }
        catch (java.net.SocketTimeoutException ex) { throw new ClusterError("PROGRESS", "连接 " + address + " 超时，请检查防火墙是否放行该端口"); }
        catch (IOException ex) { throw new ClusterError("PROGRESS", "连接 " + address + " 失败：" + ex.getMessage()); }
        catch (ClusterError ex) { throw new ClusterError("PROGRESS", "计算节点已响应但未就绪：" + ex.code() + " " + ex.getMessage()); }
    }

    /** 存储节点 RPC 可连且已加入 Metadata 成员；否则抛出说明原因的异常。 */
    void checkStorage(ClusterState state, NodeSpec node) {
        String address = ConfigRenderer.host(state, node) + ":" + node.rpcPort();
        if (!storageReachable(state, node)) throw new ClusterError("PROGRESS", "RPC 端口 " + address + " 无法连接（进程未启动、尚未监听或被防火墙拦截）");
        Set<String> members;
        try { members = storageMembers(state); }
        catch (ClusterError ex) { throw new ClusterError("PROGRESS", "无法读取 Metadata 成员：" + ex.getMessage()); }
        if (!members.contains(node.id())) throw new ClusterError("PROGRESS", "节点已启动，尚未加入 Metadata 成员（当前成员：" + String.join(", ", members) + "）");
    }

    void setRootPassword(ClusterState state, NodeSpec via, String password) throws IOException {
        computeAdmin(state, via, admin("setPassword").put("user", "root").put("password", password), 15_000);
    }

    JsonNode computeCall(String host, int port, ObjectNode request, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            socket.setSoTimeout(timeoutMs + 3000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.write(ClusterStore.LINE.writeValueAsString(request.put("timeoutMs", timeoutMs)));
            writer.write('\n');
            writer.flush();
            String line = reader.readLine();
            if (line == null) throw new IOException("Compute 关闭了连接");
            return ClusterStore.LINE.readTree(line);
        }
    }
}
