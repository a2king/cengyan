package io.cengyan.parser;

import java.util.List;

/** 一条语句。字段名沿用用户写下的大小写。 */
public sealed interface Stmt {
    record CreateGraph(String name) implements Stmt {
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

    record CreateType(boolean node, String name, List<Field> fields) implements Stmt {
    }

    record CreateIndex(boolean unique, String name, boolean node, String typeName, List<String> properties)
            implements Stmt {
    }

    record DropIndex(String name) implements Stmt {
    }

    record AlterType(boolean node, String typeName, String property, String valueType) implements Stmt {
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

    record Field(String name, String valueType, boolean nullable) {
    }

    record NodePat(String variable, String typeName, List<Prop> properties) {
    }

    record EdgePat(
            Direction direction,
            String variable,
            String typeName,
            Long rank,
            List<Prop> properties,
            Integer minHop,
            Integer maxHop) {
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
