package io.cengyan.catalog;

/** 一次统计里的一行。 */
public record StatsRow(String target, String name, long count) {
}
