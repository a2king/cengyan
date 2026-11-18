package com.cengyan.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 字段不参与映射：既不从结果填充，也不写入图。{@code static} 和 {@code transient} 字段本来就不参与。 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CengyanIgnore {
}
