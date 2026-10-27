package io.cengyan.query;

/** 一次执行使用的安全上限。 */
public record QueryLimits(int maxEdgesPerVertex) {
    public QueryLimits {
        if (maxEdgesPerVertex <= 0) {
            throw new IllegalArgumentException("每点边数上限必须为正");
        }
    }
}
