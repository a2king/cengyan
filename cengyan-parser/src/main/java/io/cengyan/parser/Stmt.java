package io.cengyan.parser;

import java.util.List;

/** 一条语句。字段名沿用用户写下的大小写。 */
public sealed interface Stmt {
    record CreateGraph(String name, int parts, String comment) implements Stmt {
        public CreateGraph(String name) { this(name, 1, null); }
        public CreateGraph(String name, String comment) { this(name, 1, comment); }
    }

    record Use(String name) implements Stmt {
    }

    record CreateAlias(String alias, String graph) implements Stmt {
    }

    record DropAlias(String name) implements Stmt {
    }

    record DropGraph(String name) implements Stmt {
    }

    record Show(Kind kind) implements Stmt {
        public enum Kind {
            GRAPHS, ALIASES, NODE_TYPES, REL_TYPES, INDEXES
        }
    }

    record Describe(boolean node, String name) implements Stmt {
    }

    record CreateType(boolean node, String name, String comment, String idType, int idLength, List<Field> fields) implements Stmt {
        public CreateType(boolean node, String name, String idType, int idLength, List<Field> fields) {
            this(node, name, null, idType, idLength, fields);
        }
    }

    record CreateIndex(boolean unique, String name, boolean node, String typeName, List<String> properties)
            implements Stmt {
    }

    record DropIndex(String name) implements Stmt {
    }

    record AlterType(boolean node, String typeName, String property, String valueType, String comment) implements Stmt {
        public AlterType(boolean node, String typeName, String property, String valueType) {
            this(node, typeName, property, valueType, null);
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
            Long skip,
            Long limit,
            boolean distinct) implements Stmt {
    }

    record Explain(Match query) implements Stmt {
    }

    record Assign(String variable, String field, Expr value) {
    }

    record Set(NodePat source, EdgePat edge, NodePat target, Expr where, List<Assign> assignments) implements Stmt {
    }

    record Delete(NodePat source, EdgePat edge, NodePat target, Expr where, List<String> variables) implements Stmt {
    }

    record Arg(String name, Expr value) {
    }

    record Call(String name, List<Arg> args, List<String> yield) implements Stmt {
    }

    record Field(String name, String valueType, boolean nullable, String comment) {
        public Field(String name, String valueType, boolean nullable) { this(name, valueType, nullable, null); }
    }

    record NodePat(String variable, String typeName, List<Prop> properties) {
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
