package io.cengyan.query;

/**
 * 一次执行使用的资源预算。用户写的 LIMIT 不算在这里。
 *
 * <p>整数预算值为 {@code 0} 表示不限。内存预算也是执行策略，不是 GQL 语义：
 * Java 中间结果受预算保护，排序、去重和聚合交给 DuckDB 时使用该预算作为内存上限，
 * DuckDB 可以在临时目录中落盘。</p>
 */
public record QueryLimits(
        int maxEdgesPerVertex,
        int maxFrontierPerHop,
        int maxHops,
        int maxPaths,
        long queryMemoryBytes,
        boolean supernodeSpillEnabled,
        int supernodeEdgeThreshold,
        boolean supernodeFrontierSpillEnabled,
        long supernodeSpillMaxBytes,
        int supernodeSpillBatchEdges) {
    public QueryLimits(int maxEdgesPerVertex, int maxFrontierPerHop, int maxHops, int maxPaths) {
        this(maxEdgesPerVertex, maxFrontierPerHop, maxHops, maxPaths, 0L, false, 0, false, 0L, 4096);
    }

    public QueryLimits(int maxEdgesPerVertex, long queryMemoryBytes) {
        this(maxEdgesPerVertex, 0, 0, 0, queryMemoryBytes, false, 0, false, 0L, 4096);
    }

    public QueryLimits {
        if (maxEdgesPerVertex < 0 || maxFrontierPerHop < 0 || maxHops < 0 || maxPaths < 0) {
            throw new IllegalArgumentException("查询预算不能为负，0 表示不限");
        }
        if (queryMemoryBytes < 0 || supernodeEdgeThreshold < 0
                || supernodeSpillMaxBytes < 0 || supernodeSpillBatchEdges <= 0) {
            throw new IllegalArgumentException("查询内存预算不能为负，0 表示不限");
        }
        maxEdgesPerVertex = unlimited(maxEdgesPerVertex);
        maxFrontierPerHop = unlimited(maxFrontierPerHop);
        maxHops = unlimited(maxHops);
        maxPaths = unlimited(maxPaths);
    }

    /** 测试里只改每点边数时，其余上限用计划书里的默认值。 */
    public QueryLimits(int maxEdgesPerVertex) {
        this(maxEdgesPerVertex, 0, 0, 0, 0L, false, 0, false, 0L, 4096);
    }

    private static int unlimited(int value) {
        return value == 0 ? Integer.MAX_VALUE : value;
    }
}
