package io.cengyan.studio.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import io.cengyan.studio.cluster.ClusterState.Role;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/** 可在网页上修改的参数：中文说明、默认值、取值范围。拓扑字段不在目录里，由 Studio 生成。 */
public final class ConfigCatalog {
    public record Entry(String key, String target, String label, String type, String defaultValue, Long min, Long max, String description) {
        public boolean env() { return "env".equals(target); }
    }
    record Raw(String key, String target, String label, String type, String description, Long min, Long max,
               @com.fasterxml.jackson.annotation.JsonProperty("default") String defaultValue) { }

    private final Map<Role, List<Entry>> entries;

    private ConfigCatalog(Map<Role, List<Entry>> entries) { this.entries = entries; }

    public static ConfigCatalog load() {
        try (InputStream input = ConfigCatalog.class.getClassLoader().getResourceAsStream("config-catalog.json")) {
            if (input == null) throw new IllegalStateException("缺少 config-catalog.json");
            Map<Role, List<Raw>> raw = ClusterStore.JSON.readValue(input, new TypeReference<>() { });
            Map<Role, List<Entry>> result = new EnumMap<>(Role.class);
            raw.forEach((role, list) -> result.put(role, list.stream().map(r -> new Entry(r.key(), r.target() == null ? "properties" : r.target(),
                    r.label(), r.type(), r.defaultValue() == null ? "" : r.defaultValue(), r.min(), r.max(), r.description())).toList()));
            return new ConfigCatalog(result);
        } catch (IOException ex) { throw new IllegalStateException("参数目录损坏", ex); }
    }

    public List<Entry> entries(Role role) { return entries.getOrDefault(role, List.of()); }

    public Optional<Entry> entry(Role role, String key) {
        return entries(role).stream().filter(e -> e.key().equals(key)).findFirst();
    }

    /** 节点最终生效的值：目录默认 ← 角色默认 ← 节点覆盖。 */
    public Map<String, String> effective(Role role, ClusterState.ConfigVersion config, String nodeId) {
        Map<String, String> values = new LinkedHashMap<>();
        for (Entry entry : entries(role)) values.put(entry.key(), entry.defaultValue());
        values.putAll(config.roleDefaults().getOrDefault(role, Map.of()));
        if (nodeId != null) values.putAll(config.nodeOverrides().getOrDefault(nodeId, Map.of()));
        return values;
    }

    /** 校验一组值；返回 键 → 错误说明，空表示通过。 */
    public Map<String, String> validate(Role role, Map<String, String> values) {
        Map<String, String> errors = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            Entry entry = entry(role, key).orElse(null);
            if (entry == null) { errors.put(key, "未知参数"); return; }
            String problem = check(entry, value == null ? "" : value.trim());
            if (problem != null) errors.put(key, problem);
        });
        return errors;
    }

    /** 跨参数约束，在合并后的生效值上检查。 */
    public Map<String, String> crossCheck(Role role, Map<String, String> effective) {
        Map<String, String> errors = new LinkedHashMap<>();
        if (role == Role.COMPUTE) {
            if (number(effective.get("query_memory_bytes")) > number(effective.get("node_memory_bytes")))
                errors.put("query_memory_bytes", "单查询内存上限不能超过节点查询内存上限");
            if (number(effective.get("query_temp_bytes")) > number(effective.get("node_temp_bytes")))
                errors.put("query_temp_bytes", "单查询临时文件上限不能超过节点临时文件上限");
        }
        return errors;
    }

    private static String check(Entry entry, String value) {
        switch (entry.type()) {
            case "int", "bytes" -> {
                long parsed;
                try { parsed = Long.parseLong(value); } catch (NumberFormatException ex) { return "必须是整数"; }
                if (entry.min() != null && parsed < entry.min()) return "不能小于 " + entry.min();
                if (entry.max() != null && parsed > entry.max()) return "不能大于 " + entry.max();
                return null;
            }
            case "label" -> { return value.matches("[A-Za-z0-9_.-]{0,64}") ? null : "只能包含字母、数字、下划线、点和短横线，最长 64 位"; }
            case "jvm" -> {
                if (value.isBlank()) return null;
                for (String token : value.split("\\s+"))
                    if (!token.matches("-(X|D|XX:)[A-Za-z0-9_.:=+,/@%-]+")) return "参数 " + token + " 不允许，只能使用 -X、-D、-XX: 开头且不含引号和空格的参数";
                return null;
            }
            default -> { return "未知参数类型"; }
        }
    }

    private static long number(String value) {
        try { return Long.parseLong(value == null ? "0" : value.trim()); } catch (NumberFormatException ex) { return 0; }
    }
}
