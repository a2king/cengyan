package io.cengyan.catalog;

/** SHOW ALIASES 的一行：别名、规范名、graphId。 */
public record AliasRow(String alias, String canonical, int graphId) {
}
