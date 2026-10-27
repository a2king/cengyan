package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Auth;
import io.cengyan.catalog.GraphName;

/** 一个连接上的当前图和登录用户。进程退出后消失。 */
public final class SessionState {
    private GraphName current;
    private String user;
    private int role;

    public void use(GraphName graph) {
        if (graph == null) {
            throw new IllegalArgumentException("图不能为空");
        }
        this.current = graph;
    }

    public GraphName current() {
        if (current == null) {
            throw new CengyanException(ErrorCode.GRAPH_NOT_FOUND, "还没有选择图");
        }
        return current;
    }

    public void bind(String name, int role) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("用户不能为空");
        }
        if (role != Auth.ROLE_READ && role != Auth.ROLE_WRITE) {
            throw new IllegalArgumentException("角色无效");
        }
        this.user = name;
        this.role = role;
    }

    /** 进程内测试没有登录用户，服务协议必须先登录。 */
    public boolean authenticated() {
        return user != null;
    }

    public boolean readOnly() {
        return authenticated() && role == Auth.ROLE_READ;
    }

    public String user() {
        return user;
    }

    public int role() {
        return role;
    }
}
