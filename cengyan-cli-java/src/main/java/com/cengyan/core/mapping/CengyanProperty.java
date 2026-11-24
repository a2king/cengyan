package com.cengyan.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 字段对应的图属性名 / 查询列名。不标注时用字段名。
 *
 * <pre>{@code
 * class Person {
 *     @CengyanId long id;
 *     @CengyanProperty("full_name") String name;   // 属性 full_name，或 RETURN ... AS full_name
 * }
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CengyanProperty {
    String value();
}
