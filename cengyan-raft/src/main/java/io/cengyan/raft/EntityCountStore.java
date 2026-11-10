package io.cengyan.raft;

import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.EntityCounts;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * 每个 Data Group 的计数元数据。namespace=3，不属于业务实体或邻接键。
 * 只统计 baseline / 已完成提交落地的主记录版本；Prepare、Abort 和历史压缩不会增减计数。
 * 修改必须在状态机串行应用期间，与实体、幂等结果及 lastApplied 使用同一个 WriteBatch。
 */
final class EntityCountStore {
    private static final byte[] READY = {0, 2};
    private static final byte[] VERSION = {1};

    private EntityCountStore() { }

    /** 升级旧快照时重建一次；没有快照则从空计数开始，随后日志重放自动维护。 */
    static void initialize(RocksDB db, WriteOptions writes) throws RocksDBException, IOException {
        byte[] ready = db.get(READY);
        if (Arrays.equals(ready, VERSION)) return;
        if (ready != null) throw new IOException("不支持的实体计数元数据版本");
        Map<Key, Long> counts = new TreeMap<>();
        try (RocksIterator iterator = db.newIterator(); WriteBatch batch = new WriteBatch()) {
            // 不接受残留的半成品；正常发布的初始化 marker 和计数是原子写入的。
            iterator.seek(new byte[] {3});
            while (iterator.isValid() && iterator.key()[0] == 3) {
                batch.delete(iterator.key());
                iterator.next();
            }
            iterator.status();
            for (byte kind : new byte[] {16, 17}) {
                iterator.seek(new byte[] {2, kind});
                while (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (key.length < 2 || key[0] != 2 || key[1] != kind) break;
                    Key counted = entity(key, iterator.value());
                    if (counted != null) add(counts, counted, 1);
                    iterator.next();
                }
                iterator.status();
            }
            for (var entry : counts.entrySet()) batch.put(entry.getKey().bytes(), number(entry.getValue()));
            batch.put(READY, VERSION);
            db.write(writes, batch);
        }
    }

    /** finalValues 按物理主键去重，处理同一 WriteBatch 内多次修改同一键。 */
    static void update(RocksDB db, WriteBatch batch, Map<com.google.protobuf.ByteString, byte[]> finalValues)
            throws RocksDBException, IOException {
        Map<Key, Long> deltas = new TreeMap<>();
        for (var entry : finalValues.entrySet()) {
            byte[] key = entry.getKey().toByteArray();
            if (!primary(key)) continue;
            Key before = entity(key, db.get(key));
            Key after = entity(key, entry.getValue());
            if (java.util.Objects.equals(before, after)) continue;
            if (before != null) add(deltas, before, -1);
            if (after != null) add(deltas, after, 1);
        }
        for (var entry : deltas.entrySet()) {
            if (entry.getValue() == 0) continue;
            byte[] key = entry.getKey().bytes();
            byte[] old = db.get(key);
            long next = Math.addExact(old == null ? 0 : number(old), entry.getValue());
            if (next < 0) throw new IOException("实体计数不能为负，拒绝应用损坏的统计");
            batch.put(key, number(next));
        }
    }

    static EntityCounts read(RocksDB db, long generation) throws RocksDBException, IOException {
        var result = EntityCounts.newBuilder().setFormatVersion(1).setGeneration(generation);
        for (byte kind : new byte[] {16, 17}) {
            byte[] prefix = new Key(kind, generation, "").bytes();
            try (RocksIterator iterator = db.newIterator()) {
                iterator.seek(prefix);
                while (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (key.length < prefix.length || !Arrays.equals(prefix, Arrays.copyOf(key, prefix.length))) break;
                    long count = number(iterator.value());
                    String type = new String(key, prefix.length, key.length - prefix.length, StandardCharsets.UTF_8);
                    if (type.isEmpty()) {
                        if (kind == 16) result.setTotalNodes(count); else result.setTotalEdges(count);
                    } else {
                        result.addTypesBuilder().setNode(kind == 16).setType(type).setCount(count);
                    }
                    iterator.next();
                }
                iterator.status();
            }
        }
        return result.build();
    }

    private static boolean primary(byte[] key) {
        return key.length > 10 && key[0] == 2 && (key[1] == 16 || key[1] == 17);
    }

    private static Key entity(byte[] key, byte[] encoded) throws IOException {
        if (!primary(key) || encoded == null) return null;
        VersionedCell cell = VersionedCell.parseFrom(encoded);
        for (int i = cell.getVersionsCount() - 1; i >= 0; i--) {
            var version = cell.getVersions(i);
            if (!version.getBaseline() && version.getCommittedAtMs() == 0) continue;
            if (version.getDeleted()) return null;
            Entity entity = Entity.parseFrom(version.getValue());
            if (entity.getType().isBlank() || entity.getNode() != (key[1] == 16)) {
                throw new IOException("实体主键与计数对象类型不一致");
            }
            return new Key(key[1], ByteBuffer.wrap(key, 2, 8).getLong(), entity.getType());
        }
        return null;
    }

    private static void add(Map<Key, Long> counts, Key key, long delta) {
        counts.merge(key, delta, Math::addExact);
        counts.merge(new Key(key.kind, key.generation, ""), delta, Math::addExact);
    }

    private static byte[] number(long value) { return ByteBuffer.allocate(8).putLong(value).array(); }
    private static long number(byte[] bytes) throws IOException {
        if (bytes.length != 8) throw new IOException("实体计数元数据损坏");
        long value = ByteBuffer.wrap(bytes).getLong();
        if (value < 0) throw new IOException("实体计数元数据为负");
        return value;
    }

    private record Key(byte kind, long generation, String type) implements Comparable<Key> {
        byte[] bytes() {
            byte[] name = type.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(10 + name.length).put((byte) 3).put(kind).putLong(generation).put(name).array();
        }
        @Override public int compareTo(Key other) { return Arrays.compareUnsigned(bytes(), other.bytes()); }
    }
}
