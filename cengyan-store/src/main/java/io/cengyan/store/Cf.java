package io.cengyan.store;

import java.nio.charset.StandardCharsets;

/** 建库时一次建好的列族。阶段 1 用不到的列族也先建出来。 */
public enum Cf {
    DEFAULT("default"),
    VERTEX("vertex"),
    EDGE("edge"),
    PROP("prop"),
    CATALOG("catalog"),
    INDEX("index"),
    TXN("txn");

    private final byte[] nameBytes;

    Cf(String name) {
        this.nameBytes = name.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] nameBytes() {
        return nameBytes;
    }

    public String familyName() {
        return new String(nameBytes, StandardCharsets.UTF_8);
    }
}
