package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.GraphName;

/** 一个连接上的当前图。进程退出后消失。 */
public final class SessionState {
    private GraphName current;

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
}
