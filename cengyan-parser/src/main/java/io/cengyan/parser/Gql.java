package io.cengyan.parser;

import java.util.List;

/** 文法入口。规则写在 {@code Gql.g4}，由 {@link GqlReader} 实现。 */
public final class Gql {
    private Gql() {
    }

    public static Stmt parse(String text) {
        List<Stmt> statements = GqlReader.parseAll(text);
        if (statements.size() != 1) {
            throw new ParseException("一次只能解析一条语句");
        }
        return statements.get(0);
    }

    public static List<Stmt> parseAll(String text) {
        return GqlReader.parseAll(text);
    }

    public static String tree(String text) {
        return TreeText.of(parse(text));
    }
}
