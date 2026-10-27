package io.cengyan.catalog;

import java.util.List;

/** 节点类型。一个点的每种节点类型对应一条点键。 */
public record NodeType(int id, String name, int schemaVersion, List<PropDef> properties) {
}
