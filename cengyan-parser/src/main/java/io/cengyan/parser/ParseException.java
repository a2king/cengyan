package io.cengyan.parser;

/** 词法或文法不接受时抛出。查询层把它转成 SYNTAX。 */
public final class ParseException extends RuntimeException {
    public ParseException(String message) {
        super(message);
    }
}
