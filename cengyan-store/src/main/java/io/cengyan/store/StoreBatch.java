package io.cengyan.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 一次原子提交里的写入和删除。键和值在放入时复制。 */
public final class StoreBatch {
    private final List<Op> operations = new ArrayList<>();

    public void put(Cf family, byte[] key, byte[] value) {
        if (family == null || key == null || value == null) {
            throw new IllegalArgumentException("列族、键、值都不能为空");
        }
        operations.add(new Op(family, key.clone(), value.clone(), false));
    }

    public void delete(Cf family, byte[] key) {
        if (family == null || key == null) {
            throw new IllegalArgumentException("列族和键都不能为空");
        }
        operations.add(new Op(family, key.clone(), new byte[0], true));
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    List<Op> operations() {
        return Collections.unmodifiableList(operations);
    }

    record Op(Cf family, byte[] key, byte[] value, boolean delete) {
    }
}
