package io.cengyan.query;

/**
 * 一次执行使用的资源预算。用户写的 LIMIT 不算在这里。
 *
 * <p>值为 {@code 0} 表示不限。服务端默认使用不限预算；部署者可以通过配置重新打开预算，
 * 以防止不受信任的查询拖垮实例。这里的预算是执行策略，不是 GQL 语义。</p>
 */
public record QueryLimits(int maxEdgesPerVertex, int maxFrontierPerHop, int maxHops, int maxPaths) {
    public QueryLimits {
        if (maxEdgesPerVertex < 0 || maxFrontierPerHop < 0 || maxHops < 0 || maxPaths < 0) {
            throw new IllegalArgumentException("查询预算不能为负，0 表示不限");
        }
        maxEdgesPerVertex = unlimited(maxEdgesPerVertex);
        maxFrontierPerHop = unlimited(maxFrontierPerHop);
        maxHops = unlimited(maxHops);
        maxPaths = unlimited(maxPaths);
    }

    /** 测试里只改每点边数时，其余上限用计划书里的默认值。 */
    public QueryLimits(int maxEdgesPerVertex) {
        this(maxEdgesPerVertex, 0, 0, 0);
    }

    private static int unlimited(int value) {
        return value == 0 ? Integer.MAX_VALUE : value;
    }
}
