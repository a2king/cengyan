import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.PartitionRouter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 用服务端的 PartitionRouter / EntityId 生成分片哈希契约向量，写到 SDK 的测试资源里。
 * 不参与 Maven 构建；服务端分片算法变更后手动重跑（见 contract-gen/README.md）。
 *
 * 用法：java -cp cengyan-protocol.jar GeneratePartitionVectors.java <输出文件>
 */
public final class GeneratePartitionVectors {
    private static final int[] PARTITIONS = {1, 2, 3, 7, 16, 64, 1000, 65535};

    public static void main(String[] args) throws Exception {
        List<String> hashes = new ArrayList<>();
        Random random = new Random(20261118L);
        for (int length = 0; length <= 100; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            hashes.add("{\"hex\":\"" + hex(bytes) + "\",\"hash\":\"" + Long.toUnsignedString(PartitionRouter.hash(bytes)) + "\"}");
        }
        List<String> ids = new ArrayList<>();
        long[] ints = {0, 1, -1, 42, 123, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 32, -(1L << 40), 9_007_199_254_740_993L};
        for (long value : ints) ids.add(entry("INT64", Long.toString(value), new EntityId.Int64(value)));
        for (int i = 0; i < 40; i++) {
            long value = random.nextLong();
            ids.add(entry("INT64", Long.toString(value), new EntityId.Int64(value)));
        }
        String[] texts = {"", "a", "123", "e_1", "张三", "Ünïcödé", "😀emoji", "x".repeat(31), "y".repeat(32), "长".repeat(40),
                "org-0000000001", "spark-job-stage-partition-0"};
        for (String text : texts) ids.add(entry("STRING", quote(text), new EntityId.Text(text)));
        for (int i = 0; i < 40; i++) {
            StringBuilder text = new StringBuilder();
            int length = random.nextInt(60);
            for (int j = 0; j < length; j++) text.append((char) ('!' + random.nextInt(90)));
            ids.add(entry("STRING", quote(text.toString()), new EntityId.Text(text.toString())));
        }
        String json = "{\n  \"generator\": \"contract-gen/GeneratePartitionVectors.java (io.cengyan.protocol.PartitionRouter)\",\n"
                + "  \"hashes\": [\n    " + String.join(",\n    ", hashes) + "\n  ],\n"
                + "  \"ids\": [\n    " + String.join(",\n    ", ids) + "\n  ]\n}\n";
        Files.write(Path.of(args[0]), json.getBytes(StandardCharsets.UTF_8));
        System.out.println("hashes=" + hashes.size() + " ids=" + ids.size());
    }

    private static String entry(String type, String jsonValue, EntityId id) {
        StringBuilder parts = new StringBuilder();
        for (int i = 0; i < PARTITIONS.length; i++) {
            if (i > 0) parts.append(',');
            parts.append('"').append(PARTITIONS[i]).append("\":").append(PartitionRouter.partOf(id, PARTITIONS[i]));
        }
        return "{\"type\":\"" + type + "\",\"id\":" + jsonValue + ",\"canonicalHex\":\"" + hex(id.canonicalBytes())
                + "\",\"hash\":\"" + Long.toUnsignedString(PartitionRouter.hash(id.canonicalBytes())) + "\",\"parts\":{" + parts + "}}";
    }

    private static String hex(byte[] bytes) {
        StringBuilder text = new StringBuilder();
        for (byte b : bytes) text.append(String.format("%02x", b & 0xFF));
        return text.toString();
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') out.append('\\').append(c);
            else if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
            else out.append(c);
        }
        return out.append('"').toString();
    }
}
