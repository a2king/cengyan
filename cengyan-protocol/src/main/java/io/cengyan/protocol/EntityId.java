package io.cengyan.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** ID 编码固定，禁止使用 JVM 或 Python 的进程级 hash。 */
public sealed interface EntityId permits EntityId.Int64, EntityId.Text {
    byte[] canonicalBytes();

    record Int64(long value) implements EntityId {
        public byte[] canonicalBytes() {
            return ByteBuffer.allocate(9).put((byte) 1).putLong(value).array();
        }
    }

    record Text(String value) implements EntityId {
        public Text {
            Objects.requireNonNull(value, "ID 不能为空");
            // Java UTF-8 默认会替换孤立代理字符，必须拒绝以避免不同 ID 编码成相同字节。
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (Character.isHighSurrogate(c)) {
                    if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                        throw new IllegalArgumentException("ID 包含无效 Unicode 字符");
                    }
                } else if (Character.isLowSurrogate(c)) {
                    throw new IllegalArgumentException("ID 包含无效 Unicode 字符");
                }
            }
        }

        public byte[] canonicalBytes() {
            byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(1 + utf8.length).put((byte) 2).put(utf8).array();
        }
    }
}
