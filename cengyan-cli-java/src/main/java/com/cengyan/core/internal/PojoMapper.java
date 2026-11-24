package com.cengyan.core.internal;

import com.cengyan.core.GraphEdge;
import com.cengyan.core.GraphNode;
import com.cengyan.core.GraphPath;
import com.cengyan.core.exception.ResultException;
import com.cengyan.core.mapping.CengyanId;
import com.cengyan.core.mapping.CengyanIgnore;
import com.cengyan.core.mapping.CengyanProperty;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * POJO 与图数据之间的映射。规则：
 * <ul>
 *   <li>字段名即属性名/列名，{@code @CengyanProperty} 改名，{@code @CengyanIgnore} 排除，static/transient 字段不参与；</li>
 *   <li>主键是 {@code @CengyanId} 字段，否则是属性名为 {@code id} 的字段；</li>
 *   <li>读：按列名填字段；整行只有一列且是点/边时展开为 id + properties（边再加 src/dst）；
 *       目标是 String/Long 等值类型时，整行必须只有一列，直接转换该值；</li>
 *   <li>写：生成 {@code {id, [src, dst,] 属性...}}，null 属性也写出；</li>
 *   <li>需要无参构造函数（可以是 private）；结果里多出的列被忽略，缺的字段保持默认值。</li>
 * </ul>
 * 类的元数据按类缓存，线程安全。
 */
public final class PojoMapper {
    private static final ConcurrentMap<Class<?>, Meta> CACHE = new ConcurrentHashMap<Class<?>, Meta>();
    private static final Set<Class<?>> VALUE_TYPES = new LinkedHashSet<Class<?>>();

    static {
        Collections.addAll(VALUE_TYPES, String.class, Long.class, Integer.class, Short.class, Byte.class, Double.class, Float.class,
                Boolean.class, Character.class, BigDecimal.class, BigInteger.class, Number.class, Instant.class, OffsetDateTime.class,
                ZonedDateTime.class, LocalDateTime.class, LocalDate.class, Date.class, UUID.class, Object.class,
                GraphNode.class, GraphEdge.class, GraphPath.class);
    }

    private static final class Property {
        final Field field;
        final String name;

        Property(Field field, String name) {
            this.field = field;
            this.name = name;
        }
    }

    private static final class Meta {
        final Class<?> type;
        final Constructor<?> constructor;
        final List<Property> properties;
        final Map<String, Property> byName;
        final Property id;
        final String error;

        Meta(Class<?> type, Constructor<?> constructor, List<Property> properties, Map<String, Property> byName, Property id, String error) {
            this.type = type;
            this.constructor = constructor;
            this.properties = properties;
            this.byName = byName;
            this.id = id;
            this.error = error;
        }
    }

    private PojoMapper() {
    }

    // ------------------------------------------------------------------ 读

    /** 一行结果（列名 → 值）映射为 type。失败抛 {@link ResultException}（code {@code RESULT_MAPPING}）。 */
    public static <T> T fromRecord(Map<String, Object> row, Class<T> type) {
        try {
            if (isValueType(type)) {
                if (row.size() != 1) {
                    throw new IllegalArgumentException("映射为 " + type.getSimpleName() + " 时结果必须只有一列，实际 " + row.keySet());
                }
                return cast(type, convert(row.values().iterator().next(), type, "列 " + row.keySet().iterator().next()));
            }
            Map<String, Object> values = row;
            if (row.size() == 1) {
                Object only = row.values().iterator().next();
                if (GraphNode.isNode(only) || GraphEdge.isEdge(only)) values = expand((Map<?, ?>) only);
            }
            return fromMap(values, type);
        } catch (ResultException ex) {
            throw ex;
        } catch (IllegalArgumentException ex) {
            throw new ResultException(ex.getMessage(), "RESULT_MAPPING", null, ex);
        }
    }

    /** 点/边展开为属性 + id（+ src/dst）。 */
    static Map<String, Object> expand(Map<?, ?> entity) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        Object properties = entity.get("properties");
        if (properties instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) properties).entrySet()) values.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        values.put("id", entity.get("id"));
        if (entity.containsKey("src")) values.put("src", entity.get("src"));
        if (entity.containsKey("dst")) values.put("dst", entity.get("dst"));
        return values;
    }

    private static <T> T fromMap(Map<String, ?> values, Class<T> type) {
        Meta meta = meta(type);
        if (meta.constructor == null) throw new IllegalArgumentException(meta.error);
        Object instance;
        try {
            instance = meta.constructor.newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException("无法创建 " + type.getName() + "：" + ex, ex);
        }
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            Property property = meta.byName.get(entry.getKey());
            if (property == null) continue;
            Object value = entry.getValue();
            if (value == null && property.field.getType().isPrimitive()) continue;
            Object converted = convert(value, property.field.getGenericType(), type.getSimpleName() + "." + property.field.getName());
            try {
                property.field.set(instance, converted);
            } catch (IllegalAccessException ex) {
                throw new IllegalArgumentException("无法写入字段 " + property.field + "：" + ex, ex);
            }
        }
        return cast(type, instance);
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Class<T> type, Object value) {
        if (type.isPrimitive()) return (T) value;
        return type.cast(value);
    }

    static boolean isValueType(Class<?> type) {
        return type.isPrimitive() || type.isEnum() || type.isArray() || VALUE_TYPES.contains(type)
                || Map.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type);
    }

    /** 把结果值转换为目标类型。null 转为 null（基本类型由调用方跳过）。 */
    static Object convert(Object value, Type target, String context) {
        Class<?> raw = rawClass(target);
        if (value == null) {
            if (raw.isPrimitive()) throw new IllegalArgumentException(context + "：null 不能转换为 " + raw.getName());
            return null;
        }
        try {
            if (raw == Object.class) return value;
            if (raw == long.class || raw == Long.class) return Values.toLong(value);
            if (raw == int.class || raw == Integer.class) return Values.toInt(value);
            if (raw == short.class || raw == Short.class) return Values.toShort(value);
            if (raw == byte.class || raw == Byte.class) return Values.toByte(value);
            if (raw == double.class || raw == Double.class) return Values.toDouble(value);
            if (raw == float.class || raw == Float.class) return Values.toDouble(value).floatValue();
            if (raw == boolean.class || raw == Boolean.class) return Values.toBoolean(value);
            if (raw == char.class || raw == Character.class) {
                String text = Values.toText(value);
                if (text.length() != 1) throw new IllegalArgumentException("期望单个字符：" + text);
                return text.charAt(0);
            }
            if (raw == String.class) return Values.toText(value);
            if (raw == BigDecimal.class) {
                if (value instanceof Number || value instanceof String) return new BigDecimal(value.toString());
                throw Values.mismatch(value, "数值");
            }
            if (raw == BigInteger.class) {
                if (value instanceof BigInteger) return value;
                return BigInteger.valueOf(Values.toLong(value));
            }
            if (raw == Number.class) {
                if (value instanceof Number) return value;
                throw Values.mismatch(value, "数值");
            }
            if (raw == Instant.class) return Values.toInstant(value);
            if (raw == OffsetDateTime.class) return Values.toInstant(value).atOffset(ZoneOffset.UTC);
            if (raw == ZonedDateTime.class) return Values.toInstant(value).atZone(ZoneOffset.UTC);
            if (raw == LocalDateTime.class) return LocalDateTime.ofInstant(Values.toInstant(value), ZoneOffset.UTC);
            if (raw == LocalDate.class) {
                String text = Values.toText(value);
                return text.length() == 10 ? LocalDate.parse(text) : LocalDateTime.ofInstant(DateTimes.parse(text), ZoneOffset.UTC).toLocalDate();
            }
            if (raw == Date.class) return Date.from(Values.toInstant(value));
            if (raw == UUID.class) return UUID.fromString(Values.toText(value));
            if (raw.isEnum()) return enumValue(raw, Values.toText(value));
            if (raw == GraphNode.class) return GraphNode.from(value);
            if (raw == GraphEdge.class) return GraphEdge.from(value);
            if (raw == GraphPath.class) return GraphPath.from(value);
            if (raw.isArray()) return toArray(value, target, raw, context);
            if (Collection.class.isAssignableFrom(raw)) return toCollection(value, target, raw, context);
            if (Map.class.isAssignableFrom(raw)) return toMap(value, target, raw, context);
            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                @SuppressWarnings("unchecked")
                Map<String, Object> values = (GraphNode.isNode(map) || GraphEdge.isEdge(map)) ? expand(map) : (Map<String, Object>) map;
                return fromMap(values, raw);
            }
            if (raw.isInstance(value)) return value;
            throw Values.mismatch(value, raw.getSimpleName());
        } catch (IllegalArgumentException ex) {
            if (ex.getMessage() != null && ex.getMessage().startsWith(context)) throw ex;
            throw new IllegalArgumentException(context + "：" + ex.getMessage(), ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object enumValue(Class<?> type, String name) {
        try {
            return Enum.valueOf((Class) type, name);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(type.getSimpleName() + " 没有枚举值 " + name);
        }
    }

    private static Object toArray(Object value, Type target, Class<?> raw, String context) {
        if (!(value instanceof List)) throw Values.mismatch(value, "列表");
        List<?> list = (List<?>) value;
        Type component = target instanceof GenericArrayType ? ((GenericArrayType) target).getGenericComponentType() : raw.getComponentType();
        Object array = Array.newInstance(raw.getComponentType(), list.size());
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (item == null && raw.getComponentType().isPrimitive()) throw new IllegalArgumentException("基本类型数组不能含 null");
            Array.set(array, i, convert(item, component, context + "[" + i + "]"));
        }
        return array;
    }

    private static Object toCollection(Object value, Type target, Class<?> raw, String context) {
        if (!(value instanceof List)) throw Values.mismatch(value, "列表");
        Type element = typeArgument(target, 0);
        Collection<Object> result;
        if (raw.isAssignableFrom(ArrayList.class)) result = new ArrayList<Object>();
        else if (raw.isAssignableFrom(LinkedHashSet.class)) result = new LinkedHashSet<Object>();
        else throw new IllegalArgumentException("不支持的集合类型：" + raw.getName() + "（请声明为 List、Set 或 Collection）");
        int i = 0;
        for (Object item : (List<?>) value) result.add(convert(item, element, context + "[" + i++ + "]"));
        return result;
    }

    private static Object toMap(Object value, Type target, Class<?> raw, String context) {
        if (!(value instanceof Map)) throw Values.mismatch(value, "对象");
        if (!raw.isAssignableFrom(LinkedHashMap.class)) {
            throw new IllegalArgumentException("不支持的 Map 类型：" + raw.getName() + "（请声明为 Map）");
        }
        Type element = typeArgument(target, 1);
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            String key = String.valueOf(entry.getKey());
            result.put(key, convert(entry.getValue(), element, context + "." + key));
        }
        return result;
    }

    private static Type typeArgument(Type type, int index) {
        if (type instanceof ParameterizedType) {
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
            if (index < arguments.length) return arguments[index];
        }
        return Object.class;
    }

    private static Class<?> rawClass(Type type) {
        if (type instanceof Class) return (Class<?>) type;
        if (type instanceof ParameterizedType) return rawClass(((ParameterizedType) type).getRawType());
        if (type instanceof GenericArrayType) {
            return Array.newInstance(rawClass(((GenericArrayType) type).getGenericComponentType()), 0).getClass();
        }
        if (type instanceof WildcardType) {
            Type[] upper = ((WildcardType) type).getUpperBounds();
            return upper.length == 0 ? Object.class : rawClass(upper[0]);
        }
        return Object.class;
    }

    // ------------------------------------------------------------------ 写

    /**
     * 批量写入的一行。Map 原样返回；POJO 转成 {@code {id, [src, dst,] 属性...}}。
     * 类配置有误（没有 id 字段、边没有 src/dst 字段等）时抛 {@link IllegalArgumentException}。
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> toRow(Object row, boolean edge) {
        if (row instanceof Map) return (Map<String, Object>) row;
        if (row == null) throw new IllegalArgumentException("批量写入的行不能为 null");
        Class<?> type = row.getClass();
        if (isValueType(type)) throw new IllegalArgumentException("批量写入的行必须是 Map 或 POJO，实际是 " + type.getName());
        Meta meta = meta(type);
        if (meta.id == null) {
            throw new IllegalArgumentException(type.getName() + " 没有主键字段：用 @CengyanId 标注，或命名为 id");
        }
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("id", read(meta.id, row));
        if (edge) {
            for (String endpoint : new String[] {"src", "dst"}) {
                Property property = meta.byName.get(endpoint);
                if (property == null) throw new IllegalArgumentException(type.getName() + " 没有边端点字段 " + endpoint);
                values.put(endpoint, read(property, row));
            }
        }
        for (Property property : meta.properties) {
            if (!values.containsKey(property.name)) values.put(property.name, read(property, row));
        }
        return values;
    }

    private static Object read(Property property, Object instance) {
        try {
            return property.field.get(instance);
        } catch (IllegalAccessException ex) {
            throw new IllegalArgumentException("无法读取字段 " + property.field + "：" + ex, ex);
        }
    }

    // ------------------------------------------------------------------ 元数据

    private static Meta meta(Class<?> type) {
        Meta meta = CACHE.get(type);
        if (meta == null) {
            meta = build(type);
            Meta previous = CACHE.putIfAbsent(type, meta);
            if (previous != null) meta = previous;
        }
        return meta;
    }

    private static Meta build(Class<?> type) {
        List<Class<?>> chain = new ArrayList<Class<?>>();
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) chain.add(0, current);
        List<Property> properties = new ArrayList<Property>();
        Map<String, Property> byName = new LinkedHashMap<String, Property>();
        Property id = null;
        for (Class<?> owner : chain) {
            for (Field field : owner.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) || field.isSynthetic()
                        || field.isAnnotationPresent(CengyanIgnore.class)) {
                    continue;
                }
                boolean isId = field.isAnnotationPresent(CengyanId.class);
                CengyanProperty named = field.getAnnotation(CengyanProperty.class);
                if (isId && named != null && !"id".equals(named.value())) {
                    throw new IllegalArgumentException(field + "：@CengyanId 字段固定对应 id，不能再用 @CengyanProperty 改名");
                }
                String name = isId ? "id" : named != null ? named.value() : field.getName();
                if (name.isEmpty()) throw new IllegalArgumentException(field + "：@CengyanProperty 不能为空");
                Property existing = byName.get(name);
                if (existing != null) {
                    // 包括：两个 @CengyanId；@CengyanId 字段与名为 id 的字段并存；两个字段改成同一个名字。
                    throw new IllegalArgumentException(type.getName() + "：字段 " + existing.field.getName() + " 和 "
                            + field.getName() + " 对应同一属性 " + name);
                }
                field.setAccessible(true);
                Property property = new Property(field, name);
                properties.add(property);
                byName.put(name, property);
                if ("id".equals(name)) id = property;
            }
        }
        Constructor<?> constructor = null;
        String error = null;
        if (Modifier.isAbstract(type.getModifiers()) || type.isInterface()) {
            error = type.getName() + " 是抽象类或接口，无法创建实例";
        } else if (type.getEnclosingClass() != null && !Modifier.isStatic(type.getModifiers())) {
            error = type.getName() + " 是非静态内部类，无法创建实例（请声明为 static）";
        } else {
            try {
                constructor = type.getDeclaredConstructor();
                constructor.setAccessible(true);
            } catch (NoSuchMethodException ex) {
                error = type.getName() + " 没有无参构造函数";
            }
        }
        return new Meta(type, constructor, Collections.unmodifiableList(properties), Collections.unmodifiableMap(byName), id, error);
    }
}
