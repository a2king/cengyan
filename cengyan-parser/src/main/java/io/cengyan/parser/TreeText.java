package io.cengyan.parser;

import java.util.StringJoiner;

/** 语法树的稳定文本，用来做快照。 */
final class TreeText {
    private TreeText() {
    }

    static String of(Stmt stmt) {
        return switch (stmt) {
            case Stmt.CreateGraph graph -> "(createGraph " + graph.name() + note(graph.comment())
                    + (graph.like() == null ? "" : " like " + graph.like())
                    + (graph.types().isEmpty() ? "" : " " + graph.types().stream().map(TreeText::of).toList()) + ")";
            case Stmt.Use use -> "(use " + use.name() + ")";
            case Stmt.UseQuery use -> "(use " + use.graph() + " " + of(use.body()) + ")";
            case Stmt.CreateAlias alias -> "(createAlias " + alias.alias() + " " + alias.graph() + ")";
            case Stmt.DropAlias drop -> "(dropAlias " + drop.name() + ")";
            case Stmt.DropGraph drop -> "(dropGraph " + drop.name() + ")";
            case Stmt.Show show -> "(show " + show.kind() + ")";
            case Stmt.Describe describe -> "(describe " + (describe.node() ? "node " : "rel ") + describe.name() + ")";
            case Stmt.CreateType type -> "(createType " + (type.node() ? "node " : "rel ") + type.name()
                    + note(type.comment()) + id(type) + " " + fields(type.fields()) + ")";
            case Stmt.CreateIndex index -> "(createIndex " + (index.unique() ? "unique " : "")
                    + index.name() + " " + (index.node() ? "node " : "rel ") + index.typeName()
                    + " " + String.join(",", index.properties()) + ")";
            case Stmt.DropIndex drop -> "(dropIndex " + drop.name() + ")";
            case Stmt.AlterType alter -> "(alter " + (alter.node() ? "node " : "rel ") + alter.typeName()
                    + " " + alter.property() + " " + alter.valueType() + note(alter.comment()) + ")";
            case Stmt.Insert insert -> "(insert " + node(insert.source())
                    + (insert.edge() == null ? "" : " " + edge(insert.edge()) + " " + node(insert.target())) + ")";
            case Stmt.Match match -> {
                StringBuilder text = new StringBuilder("(match ");
                if (match.pathVariable() != null) {
                    text.append(match.pathVariable()).append(" = ");
                }
                text.append(node(match.source()));
                if (match.edge() != null) {
                    text.append(' ').append(edge(match.edge())).append(' ').append(node(match.target()));
                }
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
                    text.append(" (skip ").append(countText(match.skip())).append(')');
                }
                if (match.limit() != null) {
                    text.append(" (limit ").append(countText(match.limit())).append(')');
                }
                text.append(')');
                yield text.toString();
            }
            case Stmt.Explain explain -> "(explain " + of(explain.query()) + ")";
            case Stmt.ExplainQuery explain -> "(explain " + of(explain.statement()) + ")";
            case Stmt.Set set -> assignment(set.source(), set.edge(), set.target(), set.where(), set.assignments(), null);
            case Stmt.Delete delete -> assignment(delete.source(), delete.edge(), delete.target(), delete.where(), null, delete.variables());
            case Stmt.DropType drop -> "(dropType " + (drop.node() ? "node " : "rel ") + drop.name()
                    + (drop.ifExists() ? " ifExists" : "") + ")";
            case Stmt.Select select -> "(select " + select.clauses().size() + " " + tail(select.tail()) + ")";
            case Stmt.AggregateFilter filter -> "(aggregateFilter " + of(filter.query()) + " " + expr(filter.condition()) + ")";
            case Stmt.Composite composite -> "(" + composite.op() + (composite.all() ? " all " : " ")
                    + of(composite.left()) + " " + of(composite.right()) + ")";
            case Stmt.Next next -> "(next " + of(next.left()) + " " + of(next.right()) + ")";
            case Stmt.InsertGraph insert -> "(insertGraph " + insert.elements().size()
                    + (insert.tail() == null ? "" : " " + tail(insert.tail())) + ")";
            case Stmt.InsertUsing insert -> "(insertUsing " + insert.elements().size()
                    + (insert.tail() == null ? "" : " " + tail(insert.tail())) + ")";
            case Stmt.Mutate mutate -> "(mutate"
                    + (mutate.assignments().isEmpty() ? "" : " set=" + mutate.assignments().size())
                    + (mutate.removals().isEmpty() ? "" : " remove=" + String.join(",", mutate.removals()))
                    + " deletes=" + mutate.deletes().size()
                    + " detach=" + mutate.detach()
                    + (mutate.tail() == null ? "" : " " + tail(mutate.tail())) + ")";
            case Stmt.Call call -> {
                StringBuilder text = new StringBuilder("(call ");
                text.append(call.name());
                for (Stmt.Arg arg : call.args()) {
                    text.append(' ').append(arg.name()).append('=').append(expr(arg.value()));
                }
                text.append(" (yield");
                for (int i = 0; i < call.yield().size(); i++) {
                    String column = call.yield().get(i);
                    text.append(' ').append(column);
                    if (!column.equals(call.as().get(i))) {
                        text.append(':').append(call.as().get(i));
                    }
                }
                text.append("))");
                yield text.toString();
            }
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
            joiner.add(field.name() + ":" + field.valueType() + (field.nullable() ? "?" : "") + note(field.comment()));
        }
        return joiner.toString();
    }

    private static String id(Stmt.CreateType type) {
        if (!type.node() || ("INT".equalsIgnoreCase(type.idType()) && type.idLength() == 64)) {
            return "";
        }
        return " id=" + type.idType().toUpperCase(java.util.Locale.ROOT) + "(" + type.idLength() + ")";
    }

    private static String note(String value) {
        return value == null ? "" : " comment='" + value.replace("'", "''") + "'";
    }

    private static String node(Stmt.NodePat node) {
        StringBuilder text = new StringBuilder("(node");
        if (node.variable() != null) {
            text.append(' ').append(node.variable());
        }
        if (node.typeName() != null) {
            text.append(':').append(node.typeName());
        } else if (node.label() != null) {
            text.append(':').append(label(node.label()));
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
        if (edge.label() != null && !(edge.label() instanceof Stmt.Label.One) && edge.typeNames().isEmpty()) {
            text.append(':').append(label(edge.label()));
        } else if (!edge.typeNames().isEmpty()) {
            text.append(':').append(String.join("|", edge.typeNames()));
        }
        if (edge.rank() != null) {
            text.append('@').append(edge.rank());
        }
        if (edge.minHop() != null) {
            text.append('{').append(edge.minHop());
            if (edge.maxHop() != null && edge.maxHop() < 0) {
                text.append(",}");
            } else if (!edge.minHop().equals(edge.maxHop())) {
                text.append(',').append(edge.maxHop());
            }
            text.append('}');
        }
        for (Stmt.Prop prop : edge.properties()) {
            text.append(' ').append(prop.name()).append('=').append(expr(prop.value()));
        }
        if (edge.where() != null) {
            text.append(" (where ").append(expr(edge.where())).append(')');
        }
        text.append(')');
        return text.toString();
    }

    private static String label(Stmt.Label label) {
        return switch (label) {
            case Stmt.Label.One one -> one.name();
            case Stmt.Label.Any ignored -> "%";
            case Stmt.Label.Not not -> "!" + label(not.inner());
            case Stmt.Label.Or or -> "(" + String.join("|", or.options().stream().map(TreeText::label).toList()) + ")";
            case Stmt.Label.And and -> "(" + String.join("&", and.options().stream().map(TreeText::label).toList()) + ")";
        };
    }

    private static String expr(Expr expr) {
        return switch (expr) {
            case Expr.Literal literal -> literal(literal.value());
            case Expr.Param param -> "$" + param.name();
            case Expr.Variable variable -> variable.name();
            case Expr.Prop prop -> prop.variable() + "." + prop.field();
            case Expr.Call call -> call.name() + args(call.args());
            case Expr.Reduce reduce -> "(reduce " + reduce.accumulator() + " " + expr(reduce.initial())
                    + " " + reduce.item() + " " + expr(reduce.collection()) + " " + expr(reduce.body()) + ")";
            case Expr.Unary unary -> "(" + unary.op() + " " + expr(unary.inner()) + ")";
            case Expr.Binary binary -> "(" + binary.op() + " " + expr(binary.left()) + " " + expr(binary.right()) + ")";
            case Expr.In in -> "(" + (in.negated() ? "not-in " : "in ") + expr(in.value()) + args(in.options()) + ")";
            case Expr.IsNull isNull -> "(" + (isNull.negated() ? "is-not-null " : "is-null ") + expr(isNull.value()) + ")";
            case Expr.Member member -> "(" + (member.negated() ? "not-in " : "in ") + expr(member.value())
                    + " " + expr(member.collection()) + ")";
            case Expr.ListLit list -> args(list.items());
            case Expr.MapLit map -> "map" + args(map.values());
            case Expr.Index index -> "(" + expr(index.value()) + "[" + expr(index.start())
                    + (index.end() == null ? "" : ":" + expr(index.end())) + "])";
            case Expr.Cast cast -> "(cast " + expr(cast.value()) + " " + cast.type() + ")";
            case Expr.CaseExpr item -> "(case " + (item.scrutinee() == null ? "" : expr(item.scrutinee()) + " ")
                    + item.whens().size() + ")";
            case Expr.Exists ignored -> "(exists)";
            case Expr.Typed typed -> "(typed " + expr(typed.value()) + ")";
            case Expr.Labeled labeled -> "(labeled " + expr(labeled.value()) + " " + label(labeled.label()) + ")";
            case Expr.EndpointOf endpoint -> "(endpoint " + expr(endpoint.node()) + ")";
            case Expr.Same same -> "(" + (same.different() ? "all-different " : "same ") + same.values().size() + ")";
            case Expr.PropExists prop -> "(prop-exists " + expr(prop.value()) + " " + prop.property() + ")";
            case Expr.Truth truth -> "(is" + (truth.negated() ? "-not " : " ")
                    + (truth.truth() == null ? "unknown" : truth.truth().toString()) + " " + expr(truth.value()) + ")";
            case Expr.Directed directed -> "(" + (directed.negated() ? "is-not-directed " : "is-directed ") + expr(directed.value()) + ")";
            case Expr.LetIn let -> "(let " + String.join(",", let.names()) + " " + expr(let.body()) + ")";
            case Expr.ValueQuery ignored -> "(value)";
            case Expr.Trim trim -> "(trim " + trim.spec() + (trim.chars() == null ? "" : " " + expr(trim.chars())) + " " + expr(trim.source()) + ")";
        };
    }

    private static String countText(Expr expr) {
        if (expr instanceof Expr.Literal literal && literal.value() instanceof Long number) {
            return number.toString();
        }
        return expr(expr);
    }

    private static String tail(Stmt.Tail tail) {
        if (tail.finish()) {
            return "finish";
        }
        return (tail.star() ? "*" : String.valueOf(tail.items().size()))
                + (tail.limit() == null ? "" : " limit " + countText(tail.limit()));
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
