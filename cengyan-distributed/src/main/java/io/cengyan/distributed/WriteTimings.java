package io.cengyan.distributed;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** 一次写入请求的分阶段耗时，挂在执行线程上，随响应返回。 */
public final class WriteTimings {
    private static final ThreadLocal<Map<String, Long>> CURRENT = new ThreadLocal<>();

    private WriteTimings() { }

    public static void begin() { CURRENT.set(new LinkedHashMap<>()); }

    public static void add(String name, long nanos) {
        Map<String, Long> current = CURRENT.get();
        if (current == null || nanos < 0) return;
        current.merge(name, TimeUnit.NANOSECONDS.toMillis(nanos), Long::sum);
    }

    public static Map<String, Long> finish() {
        Map<String, Long> current = CURRENT.get();
        CURRENT.remove();
        return current == null ? Map.of() : current;
    }
}
