package io.cengyan.catalog;

/** 一个属性定义。下标就是写入时的顺序。 */
public record PropDef(String name, ValueType type, boolean nullable, String comment) {
    public PropDef(String name, ValueType type, boolean nullable) {
        this(name, type, nullable, null);
    }
}
