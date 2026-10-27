package io.cengyan.catalog;

/** 第一版列类型。列表和映射先不做。 */
public enum ValueType {
    BOOL(1),
    INT64(2),
    DOUBLE(3),
    STRING(4),
    DATETIME(5);

    private final int code;

    ValueType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ValueType fromCode(int code) {
        for (ValueType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("未知列类型 " + code);
    }
}
