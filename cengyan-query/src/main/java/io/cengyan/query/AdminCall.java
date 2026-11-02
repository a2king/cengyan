package io.cengyan.query;

/** 一条管理命令。集群相关的字段只在对应命令里使用。refresh 为真时重新统计并覆盖缓存。 */
public record AdminCall(
        String command,
        String index,
        String user,
        String password,
        String role,
        String address,
        int nodeId,
        String graph,
        int replicas,
        int parts,
        boolean refresh) {
    public static AdminCall simple(
            String command, String index, String user, String password, String role) {
        return new AdminCall(command, index, user, password, role, "", 0, "", 0, 0, false);
    }

    public AdminCall refresh(boolean refresh) {
        return new AdminCall(
                command, index, user, password, role, address, nodeId, graph, replicas, parts, refresh);
    }
}
