package io.cengyan.parser;

import java.util.StringJoiner;

/** 语法树的稳定文本，用来做快照。 */
final class TreeText {
    private TreeText() {
    }

    static String of(Stmt stmt) {
        return switch (stmt) {
            case Stmt.CreateGraph graph -> "(createGraph " + graph.name() + ")";
            case Stmt.Use use -> "(use " + use.name() + ")";
            case Stmt.CreateAlias alias -> "(createAlias " + alias.alias() + " " + alias.graph() + ")";
            case Stmt.DropAlias drop -> "(dropAlias " + drop.name() + ")";
            case Stmt.DropGraph drop -> "(dropGraph " + drop.name() + ")";
            case Stmt.Show show -> "(show " + show.kind() + ")";
            case Stmt.Describe describe -> "(describe " + (describe.node() ? "node " : "rel ") + describe.name() + ")";
            case Stmt.CreateType type -> "(createType " + (type.node() ? "node " : "rel ") + type.name()
                    + " " + fields(type.fields()) + ")";
            case Stmt.AlterType alter -> "(alter " + (alter.node() ? "node " : "rel ") + alter.typeName()
                    + " " + alter.property() + " " + alter.valueType() + ")";
            case Stmt.Insert insert -> "(insert " + node(insert.source())
                    + (insert.edge() == null ? "" : " " + edge(insert.edge()) + " " + node(insert.target())) + ")";
            case Stmt.Match match -> {
                StringBuilder text = new StringBuilder("(match ");
                text.append(node(match.source())).append(' ').append(edge(match.edge())).append(' ').append(node(match.target()));
                if (match.where() != null) {
                    text.append(" (where ").append(expr(match.where())).append(')');
                }
                text.append(" (return");
                if (match.distinct()) {
                    text.append(" distinct");
                }
                for (Stmt.Item item : match.items()) {
                    text.append(' ').append(item.alias() == null ? expr(item.expr()) : expr(item.expr()) + ":" + item.alias());
                }
                text.append(')');
                if (!match.orderBy().isEmpty()) {
                    text.append(" (order");
                    for (Stmt.Sort sort : match.orderBy()) {
                        text.append(' ').append(expr(sort.expr())).append(sort.descending() ? " desc" : "");
                    }
                    text.append(')');
                }
                if (match.skip() != null) {
                    text.append(" (skip ").append(match.skip()).append(')');
                }
                if (match.limit() != null) {
                    text.append(" (limit ").append(match.limit()).append(')');
                }
                text.append(')');
                yield text.toString();
            }
            case Stmt.Set set -> assignment(set.source(), set.edge(), set.target(), set.where(), set.assignments(), null);
            case Stmt.Delete delete -> assignment(delete.source(), delete.edge(), delete.target(), delete.where(), null, delete.variables());
        };
    }

    private static String assignment(
            Stmt.NodePat source,
            Stmt.EdgePat edge,
            Stmt.NodePat target,
            Expr where,
            java.util.List<Stmt.Assign> assignments,
            java.util.List<String> variables) {
        StringBuilder text = new StringBuilder(assignments == null ? "(delete " : "(set ");
        text.append(node(source));
        if (edge != null) {
            text.append(' ').append(edge(edge)).append(' ').append(node(target));
        }
        if (where != null) {
            text.append(" (where ").append(expr(where)).append(')');
        }
        if (assignments != null) {
            text.append(" (assign");
            for (Stmt.Assign assign : assignments) {
                text.append(' ').append(assign.variable()).append('.').append(assign.field())
                        .append('=').append(expr(assign.value()));
            }
            text.append(')');
        } else {
            text.append(" (vars");
            for (String variable : variables) {
                text.append(' ').append(variable);
            }
            text.append(')');
        }
        text.append(')');
        return text.toString();
    }

    private static String fields(java.util.List<Stmt.Field> fields) {
        StringJoiner joiner = new StringJoiner(" ", "(", ")");
        for (Stmt.Field field : fields) {
            joiner.add(field.name() + ":" + field.valueType() + (field.nullable() ? "?" : ""));
        }
        return joiner.toString();
    }

    private static String node(Stmt.NodePat node) {
        StringBuilder text = new StringBuilder("(node");
        if (node.variable() != null) {
            text.append(' ').append(node.variable());
        }
        if (node.typeName() != null) {
            text.append(':').append(node.typeName());
        }
        for (Stmt.Prop prop : node.properties()) {
            text.append(' ').append(prop.name()).append('=').append(expr(prop.value()));
        }
        text.append(')');
        return text.toString();
    }

    private static String edge(Stmt.EdgePat edge) {
        StringBuilder text = new StringBuilder("(edge ");
        text.append(edge.direction());
        if (edge.variable() != null) {
            text.append(' ').append(edge.variable());
        }
        text.append(':').append(edge.typeName());
        if (edge.rank() != null) {
            text.append('@').append(edge.rank());
        }
        if (edge.minHop() != null) {
            text.append('{').append(edge.minHop());
            if (!edge.minHop().equals(edge.maxHop())) {
                text.append(',').append(edge.maxHop());
            }
            text.append('}');
        }
        for (Stmt.Prop prop : edge.properties()) {
            text.append(' ').append(prop.name()).append('=').append(expr(prop.value()));
        }
        text.append(')');
        return text.toString();
    }

    private static String expr(Expr expr) {
        return switch (expr) {
            case Expr.Literal literal -> literal(literal.value());
            case Expr.Param param -> "$" + param.name();
            case Expr.Variable variable -> variable.name();
            case Expr.Prop prop -> prop.variable() + "." + prop.field();
            case Expr.Call call -> call.name() + args(call.args());
            case Expr.Unary unary -> "(" + unary.op() + " " + expr(unary.inner()) + ")";
            case Expr.Binary binary -> "(" + binary.op() + " " + expr(binary.left()) + " " + expr(binary.right()) + ")";
            case Expr.In in -> "(" + (in.negated() ? "not-in " : "in ") + expr(in.value()) + args(in.options()) + ")";
            case Expr.IsNull isNull -> "(" + (isNull.negated() ? "is-not-null " : "is-null ") + expr(isNull.value()) + ")";
        };
    }

    private static String args(java.util.List<Expr> args) {
        StringJoiner joiner = new StringJoiner(" ", "(", ")");
        for (Expr arg : args) {
            joiner.add(expr(arg));
        }
        return joiner.toString();
    }

    private static String literal(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String text) {
            return "'" + text.replace("'", "''") + "'";
        }
        if (value instanceof DateTimeText time) {
            return "datetime'" + time.text() + "'";
        }
        return String.valueOf(value);
    }
}
