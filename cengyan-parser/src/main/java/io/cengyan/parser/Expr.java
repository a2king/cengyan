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

    record Reduce(String accumulator, Expr initial, String item, Expr collection, Expr body) implements Expr {
    }

    record Unary(String op, Expr inner) implements Expr {
    }

    record Binary(String op, Expr left, Expr right) implements Expr {
    }

    record In(Expr value, List<Expr> options, boolean negated) implements Expr {
    }

    record IsNull(Expr value, boolean negated) implements Expr {
    }

    /** 右侧是一个列表值，而不是括号里的若干表达式。 */
    record Member(Expr value, Expr collection, boolean negated) implements Expr {
    }

    record ListLit(List<Expr> items) implements Expr {
    }

    record MapLit(List<String> keys, List<Expr> values) implements Expr {
    }

    /** {@code end} 为空是下标，不为空是切片，含起点、不含终点。 */
    record Index(Expr value, Expr start, Expr end) implements Expr {
    }

    record Cast(Expr value, String type) implements Expr {
    }

    record CaseExpr(Expr scrutinee, List<Expr> whens, List<Expr> thens, Expr otherwise) implements Expr {
    }

    record Exists(Stmt query) implements Expr {
    }

    record Typed(Expr value, String type, boolean negated) implements Expr {
    }

    record Labeled(Expr value, Stmt.Label label, boolean negated) implements Expr {
    }

    /** {@code source} 为真表示 {@code IS SOURCE OF}。 */
    record EndpointOf(Expr node, Expr edge, boolean source, boolean negated) implements Expr {
    }

    record Same(List<Expr> values, boolean different) implements Expr {
    }

    record PropExists(Expr value, String property) implements Expr {
    }

    static List<Expr> children(Expr expr) {
        return switch (expr) {
            case Literal ignored -> List.of();
            case Param ignored -> List.of();
            case Variable ignored -> List.of();
            case Prop ignored -> List.of();
            case Call call -> call.args();
            case Reduce reduce -> List.of(reduce.initial(), reduce.collection(), reduce.body());
            case Unary unary -> List.of(unary.inner());
            case Binary binary -> List.of(binary.left(), binary.right());
            case In in -> {
                List<Expr> all = new java.util.ArrayList<>();
                all.add(in.value());
                all.addAll(in.options());
                yield all;
            }
            case IsNull isNull -> List.of(isNull.value());
            case Member member -> List.of(member.value(), member.collection());
            case ListLit list -> list.items();
            case MapLit map -> map.values();
            case Index index -> index.end() == null
                    ? List.of(index.value(), index.start())
                    : List.of(index.value(), index.start(), index.end());
            case Cast cast -> List.of(cast.value());
            case CaseExpr item -> {
                List<Expr> all = new java.util.ArrayList<>();
                if (item.scrutinee() != null) {
                    all.add(item.scrutinee());
                }
                all.addAll(item.whens());
                all.addAll(item.thens());
                if (item.otherwise() != null) {
                    all.add(item.otherwise());
                }
                yield all;
            }
            case Exists ignored -> List.of();
            case Typed typed -> List.of(typed.value());
            case Labeled labeled -> List.of(labeled.value());
            case EndpointOf endpoint -> List.of(endpoint.node(), endpoint.edge());
            case Same same -> same.values();
            case PropExists prop -> List.of(prop.value());
        };
    }
}
