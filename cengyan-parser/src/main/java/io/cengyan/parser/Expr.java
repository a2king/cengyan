package io.cengyan.parser;

import java.util.List;

/** 表达式。日期时间先留原文，执行时再解析。 */
public sealed interface Expr {
    record Literal(Object value) implements Expr {
    }

    record Param(String name) implements Expr {
    }

    record Variable(String name) implements Expr {
    }

    record Prop(String variable, String field) implements Expr {
    }

    record Call(String name, List<Expr> args) implements Expr {
    }

    record Unary(String op, Expr inner) implements Expr {
    }

    record Binary(String op, Expr left, Expr right) implements Expr {
    }

    record In(Expr value, List<Expr> options, boolean negated) implements Expr {
    }

    record IsNull(Expr value, boolean negated) implements Expr {
    }
}
