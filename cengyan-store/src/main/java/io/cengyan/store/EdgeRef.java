package io.cengyan.store;

/** 一条逻辑边。端点不随扫描方向对调。 */
public record EdgeRef(int edgeTypeId, long rank, long outSrc, long outDst) {
}
