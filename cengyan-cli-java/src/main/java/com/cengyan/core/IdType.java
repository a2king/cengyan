package com.cengyan.core;

/** 图的点/边主键类型，建图时确定，之后不能修改。 */
public enum IdType {
    /** 64 位有符号整数。 */
    INT64,
    /** UTF-8 字符串。 */
    STRING
}
