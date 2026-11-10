package io.cengyan.parser;

import java.util.List;

/** 一条语句。字段名沿用用户写下的大小写。 */
public sealed interface Stmt {
    record CreateGraph(String name, int parts, String comment, boolean ifNotExists) implements Stmt {
        public CreateGraph(String name) { this(name, 1, null, false); }
        public CreateGraph(String name, String comment) { this(name, 1, comment, false); }
        public CreateGraph(String name, int parts, String comment) { this(name, parts, comment, false); }
    }

    record Use(String name) implements Stmt {
    }

    record CreateAlias(String alias, String graph) implements Stmt {
    }

    record DropAlias(String name, boolean ifExists) implements Stmt {
        public DropAlias(String name) { this(name, false); }
    }

    record DropGraph(String name, boolean ifExists) implements Stmt {
        public DropGraph(String name) { this(name, false); }
    }

    record DropType(boolean node, String name, boolean ifExists) implements Stmt {
    }

    record Show(Kind kind) implements Stmt {
        public enum Kind {
            GRAPHS, ALIASES, NODE_TYPES, REL_TYPES, INDEXES
        }
    }

    record Describe(boolean node, String name) implements Stmt {
    }

    record CreateType(boolean node, String name, String comment, String idType, int idLength, boolean idSpecified,
                      List<Field> fields, boolean ifNotExists) implements Stmt {
        public CreateType(boolean node, String name, String idType, int idLength, List<Field> fields) {
            this(node, name, null, idType, idLength, true, fields, false);
        }

        public CreateType(boolean node, String name, String comment, String idType, int idLength, List<Field> fields) {
            this(node, name, comment, idType, idLength, true, fields, false);
        }

        public CreateType(boolean node, String name, String comment, String idType, int idLength, List<Field> fields,
                          boolean ifNotExists) {
            this(node, name, comment, idType, idLength, true, fields, ifNotExists);
        }
    }

    record CreateIndex(boolean unique, String name, boolean node, String typeName, List<String> properties,
                       boolean ifNotExists) implements Stmt {
        public CreateIndex(boolean unique, String name, boolean node, String typeName, List<String> properties) {
            this(unique, name, node, typeName, properties, false);
        }
    }

    record DropIndex(String name, boolean ifExists) implements Stmt {
        public DropIndex(String name) { this(name, false); }
    }

    record AlterType(
            boolean node,
            String typeName,
            String action,
            String property,
            String valueType,
            String comment,
            String newName) implements Stmt {
        public AlterType(boolean node, String typeName, String property, String valueType) {
            this(node, typeName, "ADD", property, valueType, null, null);
        }

        public AlterType(boolean node, String typeName, String property, String valueType, String comment) {
            this(node, typeName, "ADD", property, valueType, comment, null);
        }
    }

    record Insert(NodePat source, EdgePat edge, NodePat target) implements Stmt {
    }

    record Match(
            NodePat source,
            EdgePat edge,
            NodePat target,
            Expr where,
            List<Item> items,
            List<Sort> orderBy,
            Expr skip,
            Expr limit,
            boolean distinct,
            String pathVariable) implements Stmt {
    }

    record Explain(Match query) implements Stmt {
    }

    record ExplainQuery(Stmt statement) implements Stmt {
    }

    record Assign(String variable, String field, Expr value) {
    }

    record Set(NodePat source, EdgePat edge, NodePat target, Expr where, List<Assign> assignments) implements Stmt {
    }

    record Delete(NodePat source, EdgePat edge, NodePat target, Expr where, List<String> variables, boolean detach)
            implements Stmt {
        public Delete(NodePat source, EdgePat edge, NodePat target, Expr where, List<String> variables) {
            this(source, edge, target, where, variables, false);
        }
    }

    record Arg(String name, Expr value) {
    }

    record Call(String name, List<Arg> args, List<String> yield) implements Stmt {
    }

    record Field(String name, String valueType, boolean nullable, String comment) {
        public Field(String name, String valueType, boolean nullable) { this(name, valueType, nullable, null); }
    }

    record NodePat(String variable, String typeName, List<Prop> properties, Expr where, Label label) {
        public NodePat(String variable, String typeName, List<Prop> properties) {
            this(variable, typeName, properties, null, null);
        }
    }

    /** 点上的标签表达式。单个名字时 {@link NodePat#typeName()} 同时有值。 */
    sealed interface Label {
        record One(String name) implements Label {
        }

        record Any() implements Label {
        }

        record Not(Label inner) implements Label {
        }

        record Or(List<Label> options) implements Label {
        }

        record And(List<Label> options) implements Label {
        }
    }

    record Select(List<Clause> clauses, Tail tail) implements Stmt {
    }

    record AggregateFilter(Stmt query, Expr condition) implements Stmt {
    }

    record Composite(String op, boolean all, Stmt left, Stmt right) implements Stmt {
    }

    record Next(Stmt left, Stmt right) implements Stmt {
    }

    record InsertGraph(List<Element> elements) implements Stmt {
    }

    record InsertUsing(List<Clause> clauses, List<Element> elements) implements Stmt {
    }

    /** MATCH 之后的写入：赋值、REMOVE、DELETE 至少有一种。 */
    record Mutate(
            List<Clause> clauses,
            List<Assign> assignments,
            List<Assign> replacements,
            List<String> removals,
            List<String> deletes,
            boolean detach) implements Stmt {
    }

    sealed interface Clause {
        record Match(
                boolean optional,
                String pathVariable,
                String search,
                Integer searchCount,
                String mode,
                List<Pattern> patterns,
                Expr where) implements Clause {
        }

        record Filter(Expr expr) implements Clause {
        }

        record Let(String name, Expr value) implements Clause {
        }

        record For(String name, Expr list) implements Clause {
        }

        record Call(Stmt.Call call) implements Clause {
        }
    }

    record Pattern(List<Element> elements) {
    }

    sealed interface Element {
        record Node(NodePat node) implements Element {
        }

        record Rel(EdgePat edge) implements Element {
        }

        record Group(List<Element> elements, Expr where, int min, int max) implements Element {
        }
    }

    record Tail(
            boolean distinct,
            boolean star,
            List<Item> items,
            List<Expr> groupBy,
            List<Sort> orderBy,
            Expr skip,
            Expr limit,
            Expr postFilter) {
        public Tail(boolean distinct, boolean star, List<Item> items, List<Expr> groupBy,
                    List<Sort> orderBy, Expr skip, Expr limit) {
            this(distinct, star, items, groupBy, orderBy, skip, limit, null);
        }
    }

    record EdgePat(
            Direction direction,
            String variable,
            List<String> typeNames,
            Long rank,
            List<Prop> properties,
            Integer minHop,
            Integer maxHop) {
        public EdgePat {
            typeNames = typeNames == null ? List.of() : List.copyOf(typeNames);
        }

        /** 正好一个类型时返回它。不写类型或写了多个类型时返回空。 */
        public String typeName() {
            return typeNames.size() == 1 ? typeNames.get(0) : null;
        }

        public enum Direction {
            OUT, IN, ANY
        }
    }

    record Prop(String name, Expr value) {
    }

    record Item(Expr expr, String alias, String text) {
    }

    record Sort(Expr expr, boolean descending) {
    }
}
