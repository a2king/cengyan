package com.cengyan.core;

import java.util.LinkedHashMap;
import java.util.Map;

/** 点/边结果形状的判断（包内共用）。 */
final class Entities {
    private Entities() {
    }

    static boolean isEntity(Object value) {
        if (!(value instanceof Map)) return false;
        Map<?, ?> map = (Map<?, ?>) value;
        return map.containsKey("id") && map.get("type") instanceof String && map.get("properties") instanceof Map;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> properties(Map<?, ?> entity) {
        Object properties = entity.get("properties");
        return properties == null ? new LinkedHashMap<String, Object>() : (Map<String, Object>) properties;
    }

    static boolean same(Object a, Object b) {
        return a == null ? b == null : a.equals(b);
    }

    static String describe(Object value) {
        if (value == null) return "null";
        String text = String.valueOf(value);
        return value.getClass().getSimpleName() + " " + (text.length() > 120 ? text.substring(0, 120) + "..." : text);
    }
}
