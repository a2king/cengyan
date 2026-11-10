package io.cengyan.raft;

import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EntityCountStoreTest {
    static { RocksDB.loadLibrary(); }
    @TempDir Path directory;

    @Test
    void 准备回滚覆盖删除历史压缩及重复落地不重复计数() throws Exception {
        try (Options options = new Options().setCreateIfMissing(true);
             WriteOptions writes = new WriteOptions().setSync(true);
             RocksDB db = RocksDB.open(options, directory.resolve("db").toString())) {
            EntityCountStore.initialize(db, writes);
            byte[] key = key(true, 0, 1);
            var preparing = version("create", true, "Company", false, 0);
            apply(db, writes, key, cell(preparing));
            assertEquals(0, EntityCountStore.read(db, 0).getTotalNodes());
            var committed = preparing.toBuilder().setCommittedAtMs(100).build();
            apply(db, writes, key, cell(committed));
            apply(db, writes, key, cell(committed));
            assertEquals(1, EntityCountStore.read(db, 0).getTotalNodes());
            var overwrite = version("update", true, "Company", false, 0);
            apply(db, writes, key, cell(committed, overwrite));
            assertEquals(1, EntityCountStore.read(db, 0).getTotalNodes());
            // Abort removes only the prepared version, without touching the count.
            apply(db, writes, key, cell(committed));
            apply(db, writes, key, cell(committed.toBuilder().setBaseline(true).clearTransactionId().build()));
            assertEquals(1, EntityCountStore.read(db, 0).getTotalNodes());
            apply(db, writes, key, cell(committed, version("delete", true, "Company", true, 200)));
            assertEquals(0, EntityCountStore.read(db, 0).getTotalNodes());
            apply(db, writes, key, null);
            assertEquals(0, EntityCountStore.read(db, 0).getTotalNodes());
            apply(db, writes, key, cell(version("recreate", true, "Company", false, 300)));
            assertEquals(1, EntityCountStore.read(db, 0).getTotalNodes());
        }
    }

    @Test
    void 旧数据初始化保留代次及类型边主记录只计一次并支持重启() throws Exception {
        Path path = directory.resolve("legacy");
        try (Options options = new Options().setCreateIfMissing(true);
             WriteOptions writes = new WriteOptions().setSync(true);
             RocksDB db = RocksDB.open(options, path.toString())) {
            db.put(key(true, 0, 1), cell(version("n1", true, "Company", false, 100)));
            db.put(key(true, 0, 2), cell(version("n2", true, "Person", false, 100)));
            db.put(key(false, 0, 3), cell(version("e1", false, "owns", false, 100)));
            db.put(key(true, 1, 1), cell(version("migration", true, "Company", false, 100)));
            // Adjacency entry has the same entity value but must never be counted.
            byte[] adjacency = key(false, 0, 4); adjacency[1] = 18;
            db.put(adjacency, cell(version("edge-adj", false, "owns", false, 100)));
            db.put(key(true, 0, 5), cell(version("pending", true, "Company", false, 0)));
            EntityCountStore.initialize(db, writes);
            EntityCounts counts = EntityCountStore.read(db, 0);
            assertEquals(2, counts.getTotalNodes());
            assertEquals(1, counts.getTotalEdges());
            assertEquals(3, counts.getTypesCount());
            assertEquals(1, EntityCountStore.read(db, 1).getTotalNodes());
            EntityCountStore.initialize(db, writes);
            assertEquals(counts, EntityCountStore.read(db, 0));
        }
        try (Options options = new Options(); WriteOptions writes = new WriteOptions();
             RocksDB db = RocksDB.open(options, path.toString())) {
            EntityCountStore.initialize(db, writes);
            assertEquals(2, EntityCountStore.read(db, 0).getTotalNodes());
            apply(db, writes, key(true, 0, 6), cell(version("next", true, "Company", false, 200)));
            assertEquals(3, EntityCountStore.read(db, 0).getTotalNodes());
        }
    }

    @Test
    void 同批多类型及同键多次修改使用最终值并汇总总量() throws Exception {
        try (Options options = new Options().setCreateIfMissing(true); WriteOptions writes = new WriteOptions();
             RocksDB db = RocksDB.open(options, directory.resolve("batch").toString())) {
            EntityCountStore.initialize(db, writes);
            Map<ByteString, byte[]> values = new LinkedHashMap<>();
            values.put(ByteString.copyFrom(key(true, 0, 1)), cell(version("n1", true, "A", false, 100)));
            values.put(ByteString.copyFrom(key(true, 0, 2)), cell(version("n2", true, "B", false, 100)));
            values.put(ByteString.copyFrom(key(false, 0, 3)), cell(version("e1", false, "knows", false, 100)));
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                for (var entry : values.entrySet()) batch.put(entry.getKey().toByteArray(), entry.getValue());
                EntityCountStore.update(db, batch, values);
                db.write(writes, batch);
            }
            assertEquals(2, EntityCountStore.read(db, 0).getTotalNodes());
            assertEquals(1, EntityCountStore.read(db, 0).getTotalEdges());
        }
    }

    private static void apply(RocksDB db, WriteOptions writes, byte[] key, byte[] value) throws Exception {
        Map<ByteString, byte[]> changes = new LinkedHashMap<>();
        changes.put(ByteString.copyFrom(key), value);
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            if (value == null) batch.delete(key); else batch.put(key, value);
            EntityCountStore.update(db, batch, changes);
            db.write(writes, batch);
        }
    }
    private static byte[] key(boolean node, long generation, long id) {
        return ByteBuffer.allocate(19).put((byte) 2).put(node ? (byte) 16 : (byte) 17)
                .putLong(generation).put((byte) 1).putLong(id).array();
    }
    private static CellVersion version(String tx, boolean node, String type, boolean deleted, long committed) {
        return CellVersion.newBuilder().setTransactionId(tx).setDeleted(deleted).setCommittedAtMs(committed)
                .setValue(Entity.newBuilder().setNode(node).setType(type).build().toByteString()).build();
    }
    private static byte[] cell(CellVersion... versions) {
        return VersionedCell.newBuilder().addAllVersions(java.util.List.of(versions)).build().toByteArray();
    }
}
