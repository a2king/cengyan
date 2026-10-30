package io.cengyan.catalog;

import java.util.List;

/** 边类型。属性存在 prop 列族，不跟着出边和入边各存一份。 */
public record RelType(int id, String name, int schemaVersion, List<PropDef> properties, String comment) {
    public RelType(int id, String name, int schemaVersion, List<PropDef> properties) {
        this(id, name, schemaVersion, properties, null);
    }
}
