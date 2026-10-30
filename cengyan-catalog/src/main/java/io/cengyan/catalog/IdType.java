package io.cengyan.catalog;

/** 节点对外暴露的 ID 类型。存储层仍使用 long 作为内部点键。 */
public enum IdType {
    INT(1),
    STRING(2);

    private final int code;

    IdType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static IdType fromCode(int code) {
        for (IdType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("未知 ID 类型 " + code);
    }

    public static IdType parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("ID 类型不能为空");
        }
        return switch (text.toUpperCase(java.util.Locale.ROOT)) {
            case "INT", "INTEGER", "INT64" -> INT;
            case "STRING", "STR" -> STRING;
            default -> throw new IllegalArgumentException("未知 ID 类型 " + text);
        };
    }
}
