package io.cengyan.parser;

/** 文法入口。规则写在 {@code Gql.g4}，由 {@link GqlReader} 实现。 */
public final class Gql {
    private Gql() {
    }

    public static Stmt parse(String text) {
        return GqlReader.parse(text);
    }

    public static String tree(String text) {
        return TreeText.of(parse(text));
    }
}
