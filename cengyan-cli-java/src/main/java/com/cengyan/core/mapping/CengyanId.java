package com.cengyan.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记主键字段，对应点/边的 {@code id}。一个类最多一个。
 * 没有标注时，属性名为 {@code id} 的字段（字段名 id，或 {@code @CengyanProperty("id")}）就是主键。
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CengyanId {
}
