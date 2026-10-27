package io.cengyan.query;

/** 一次执行使用的安全上限。用户写的 LIMIT 不算在这里。 */
public record QueryLimits(int maxEdgesPerVertex, int maxFrontierPerHop, int maxHops, int maxPaths) {
    public QueryLimits {
        if (maxEdgesPerVertex <= 0 || maxFrontierPerHop <= 0 || maxHops <= 0 || maxPaths <= 0) {
            throw new IllegalArgumentException("查询上限必须为正");
        }
    }

    /** 测试里只改每点边数时，其余上限用计划书里的默认值。 */
    public QueryLimits(int maxEdgesPerVertex) {
        this(maxEdgesPerVertex, 64, 10, 100);
    }
}
