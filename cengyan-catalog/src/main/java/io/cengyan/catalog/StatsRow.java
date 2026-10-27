package io.cengyan.catalog;

/** 一次统计里的一行。最大度数只作观察，不代替每跳的边数上限。 */
public record StatsRow(String target, String name, long count, long maxDegree) {
}
