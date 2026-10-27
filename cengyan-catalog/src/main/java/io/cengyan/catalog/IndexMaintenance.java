package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** 把点和边的属性变化写进同一批次的索引键。空值不占键，因此多个空值不冲突。 */
final class IndexMaintenance {
    private static final byte[] EMPTY = new byte[0];

    private IndexMaintenance() {
    }

    static void sync(
            ReadSession session,
            StoreBatch batch,
            List<IndexDef> indexes,
            List<PropDef> definitions,
            Map<String, Object> previous,
            Map<String, Object> next,
            int graphId,
            byte[] identity) {
        if (indexes == null || indexes.isEmpty()) {
            return;
        }
        for (IndexDef index : indexes) {
            byte[] oldKey = key(index, definitions, previous, graphId, identity);
            byte[] newKey = key(index, definitions, next, graphId, identity);
            if (Arrays.equals(oldKey, newKey)) {
                continue;
            }
            if (oldKey != null) {
                batch.delete(Cf.INDEX, oldKey);
            }
            if (newKey != null) {
                ensureUnique(session, batch, index, newKey, identity);
                batch.put(Cf.INDEX, newKey, EMPTY);
            }
        }
    }

    static byte[] encode(IndexDef index, List<PropDef> definitions, Map<String, Object> props) {
        if (props == null) {
            return null;
        }
        byte[][] parts = new byte[index.propertyIndexes().size()][];
        int size = 0;
        for (int i = 0; i < index.propertyIndexes().size(); i++) {
            int position = index.propertyIndexes().get(i);
            if (position < 0 || position >= definitions.size()) {
                throw new CengyanException(ErrorCode.INTERNAL, "索引列已经不在类型上");
            }
            Object value = props.get(definitions.get(position).name());
            if (value == null) {
                return null;
            }
            parts[i] = IndexCodec.encode(definitions.get(position).type(), value);
            size += parts[i].length;
        }
        byte[] encoded = new byte[size];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, encoded, offset, part.length);
            offset += part.length;
        }
        return encoded;
    }

    private static byte[] key(
            IndexDef index, List<PropDef> definitions, Map<String, Object> props, int graphId, byte[] identity) {
        byte[] encoded = encode(index, definitions, props);
        if (encoded == null) {
            return null;
        }
        return KeyCodec.indexKey(index.id(), encoded, graphId, identity);
    }

    private static void ensureUnique(
            ReadSession session, StoreBatch batch, IndexDef index, byte[] key, byte[] identity) {
        if (!index.unique()) {
            return;
        }
        int prefixLength = key.length - identity.length;
        byte[] prefix = Arrays.copyOf(key, prefixLength);
        for (byte[] existing : session.keysWithPrefix(Cf.INDEX, prefix)) {
            if (sameIdentity(existing, prefixLength, identity) || batch.deletes(Cf.INDEX, existing)) {
                continue;
            }
            throw conflict();
        }
        if (batch.putsOther(Cf.INDEX, prefix, key)) {
            throw conflict();
        }
    }

    private static boolean sameIdentity(byte[] key, int prefixLength, byte[] identity) {
        if (key.length != prefixLength + identity.length) {
            return false;
        }
        for (int i = 0; i < identity.length; i++) {
            if (key[prefixLength + i] != identity[i]) {
                return false;
            }
        }
        return true;
    }

    private static CengyanException conflict() {
        return new CengyanException(ErrorCode.UNIQUE_VIOLATION, "唯一索引冲突");
    }
}
