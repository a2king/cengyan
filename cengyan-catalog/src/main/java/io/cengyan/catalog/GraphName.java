package io.cengyan.catalog;

/**
 * 一次名字解析的结果。kind 为 1 是图，2 是别名。之后的读写只用 graphId。
 */
public record GraphName(int graphId, String canonical, String input, int kind) {
    public static final int GRAPH = 1;
    public static final int ALIAS = 2;

    public boolean alias() {
        return kind == ALIAS;
    }
}
