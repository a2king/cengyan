package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/** 索引列族上的范围扫描。每 64 个键看一次取消，避免重建或查询停不下来。 */
public final class IndexScan {
    private IndexScan() {
    }

    static List<byte[]> collect(
            RocksIterator iterator,
            byte[] indexPrefix,
            byte[] startInclusive,
            byte[] endExclusive,
            int limit,
            CancelToken cancel,
            IntConsumer reads) {
        List<byte[]> found = new ArrayList<>();
        iterator.seek(startInclusive);
        int examined = 0;
        while (iterator.isValid()) {
            if ((examined & 63) == 0 && cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            byte[] raw = iterator.key();
            byte[] key = Arrays.copyOf(raw, raw.length);
            if (!KeyCodec.startsWith(key, indexPrefix)) {
                break;
            }
            if (endExclusive != null && Arrays.compareUnsigned(key, endExclusive) >= 0) {
                break;
            }
            found.add(key);
            if (reads != null) {
                reads.accept(1);
            }
            examined++;
            if (limit >= 0 && found.size() >= limit) {
                break;
            }
            iterator.next();
        }
        finish(iterator, "索引扫描失败");
        return found;
    }

    static void visit(RocksIterator iterator, byte[] prefix, CancelToken cancel, Consumer<byte[]> consumer) {
        iterator.seek(prefix);
        int examined = 0;
        while (iterator.isValid()) {
            if ((examined & 63) == 0 && cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            byte[] raw = iterator.key();
            byte[] key = Arrays.copyOf(raw, raw.length);
            if (!KeyCodec.startsWith(key, prefix)) {
                break;
            }
            consumer.accept(key);
            examined++;
            iterator.next();
        }
        finish(iterator, "前缀扫描失败");
    }

    private static void finish(RocksIterator iterator, String message) {
        try {
            iterator.status();
        } catch (RocksDBException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, message + "：" + ex.getMessage(), ex);
        }
    }
}
