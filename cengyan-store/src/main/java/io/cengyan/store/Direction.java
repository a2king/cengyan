package io.cengyan.store;

/** 边键里的方向。0 是出边，1 是入边。 */
public enum Direction {
    OUT(0),
    IN(1);

    private final int code;

    Direction(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Direction of(int code) {
        return switch (code) {
            case 0 -> OUT;
            case 1 -> IN;
            default -> throw new IllegalArgumentException("direction 只能是 0 或 1");
        };
    }
}
