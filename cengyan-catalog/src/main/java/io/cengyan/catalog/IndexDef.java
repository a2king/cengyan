package io.cengyan.catalog;

import java.util.List;

/** 一个属性索引。列下标指向类型属性列表，末尾加可空列不会把它们挤开。 */
public record IndexDef(
        int id,
        String name,
        boolean node,
        int typeId,
        List<String> properties,
        List<Integer> propertyIndexes,
        boolean unique,
        boolean ready) {
    public IndexDef {
        properties = List.copyOf(properties);
        propertyIndexes = List.copyOf(propertyIndexes);
    }

    public IndexDef withReady(boolean ready) {
        return new IndexDef(id, name, node, typeId, properties, propertyIndexes, unique, ready);
    }
}
