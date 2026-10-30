package io.cengyan.store;

import java.util.ArrayList;
import java.util.Arrays;
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

    /** Adds an operation while preserving the caller's delete flag. */
    public void put(Cf family, byte[] key, byte[] value, boolean delete) {
        if (delete) {
            delete(family, key);
        } else {
            put(family, key, value);
        }
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

    public List<Op> operations() {
        return Collections.unmodifiableList(operations);
    }

    public boolean deletes(Cf family, byte[] key) {
        for (Op op : operations) {
            if (op.delete && op.family == family && Arrays.equals(op.key, key)) {
                return true;
            }
        }
        return false;
    }

    /** 同一前缀上是否已经放入了另一条索引键。同一身份的覆盖不算冲突。 */
    public boolean putsOther(Cf family, byte[] prefix, byte[] exceptKey) {
        for (Op op : operations) {
            if (op.delete || op.family != family || Arrays.equals(op.key, exceptKey)) {
                continue;
            }
            if (KeyCodec.startsWith(op.key, prefix)) {
                return true;
            }
        }
        return false;
    }

    public record Op(Cf family, byte[] key, byte[] value, boolean delete) {
    }
}
