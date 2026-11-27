package io.cengyan.query;

import io.cengyan.distributed.ClusterException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SpillSorterTest {
    private static final SpillSorter.Codec<Integer> CODEC = new SpillSorter.Codec<>() {
        @Override public Integer read(DataInputStream in) throws IOException { return in.readInt(); }
        @Override public void write(DataOutputStream out, Integer value) throws IOException { out.writeInt(value); }
    };
    @TempDir Path work;

    @Test void 多轮归并保留重复值并取消后释放临时文件() throws IOException {
        var actual = new ArrayList<Integer>();
        var expected = new ArrayList<Integer>();
        try (var workspace = new SpillWorkspace(work, 8192)) {
            var sorter = new SpillSorter<>(workspace, CODEC, Comparator.naturalOrder(), 48, () -> {});
            for (int value = 511; value >= 0; value--) {
                sorter.add(value % 57, 16);
                expected.add(value % 57);
            }
            sorter.visit(actual::add);
        }
        expected.sort(Comparator.naturalOrder());
        assertEquals(expected, actual);
        assertThrows(ClusterException.class, () -> {
            try (var workspace = new SpillWorkspace(work, 8192)) {
                var sorter = new SpillSorter<>(workspace, CODEC, Comparator.naturalOrder(), 48, () -> {});
                for (int value = 0; value < 24; value++) sorter.add(value, 16);
                sorter.visit(value -> { throw new ClusterException("QUERY_CANCELED", "测试取消"); });
            }
        });
        try (var files = Files.list(work)) { assertEquals(List.of(), files.toList()); }
    }
}
