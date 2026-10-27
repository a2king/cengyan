package io.cengyan.query;

/** UTC 毫秒。返回给客户端之前改成 ISO 字符串。 */
final class UtcTime {
    private final long millis;

    UtcTime(long millis) {
        this.millis = millis;
    }

    long millis() {
        return millis;
    }
}
