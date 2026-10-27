package io.cengyan.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GqlSyntaxTest {
    @Test
    void 关键字不区分大小写且标识符保留大小写() {
        assertEquals("(createGraph Company)", Gql.tree("create graph Company"));
        assertEquals("(use corp)", Gql.tree("USE corp"));
        assertEquals("(createAlias corp Company)", Gql.tree("CREATE ALIAS corp FOR GRAPH Company"));
        assertEquals("(show GRAPHS)", Gql.tree("show graphs"));
        assertEquals("(show ALIASES)", Gql.tree("SHOW ALIASES"));
        assertEquals("(describe node Company)", Gql.tree("DESCRIBE NODE TYPE Company"));
        assertEquals("(describe rel holder)", Gql.tree("describe rel type holder"));
    }

    @Test
    void 单跳匹配的语法树() {
        String tree = Gql.tree("""
                MATCH (a:Company {id: 1})-[:holder {ratio: 0.5}]->(b)
                WHERE b.id = $id AND NOT a.name IS NULL
                RETURN b.id AS id, id(b)
                ORDER BY id DESC
                LIMIT 10
                """);
        assertEquals("(match (node a:Company id=1) (edge OUT:holder ratio=0.5) (node b)"
                + " (where (AND (= b.id $id) (NOT (is-null a.name))))"
                + " (return b.id:id id(b))"
                + " (order id desc)"
                + " (limit 10))", tree);
    }

    @Test
    void 入边无向和插入() {
        assertTrue(Gql.tree("MATCH (a:Company {id:1})<-[:holder]-(b) RETURN b.id").contains("(edge IN:holder)"));
        assertTrue(Gql.tree("MATCH (a:Company {id:1})-[:holder]-(b) RETURN b.id").contains("(edge ANY:holder)"));
        assertEquals("(insert (node a:Company id=1 name='root') (edge OUT:holder@3 ratio=0.5) (node b id=2))",
                Gql.tree("INSERT (a:Company {id:1, name:'root'})-[:holder@3 {ratio:0.5}]->(b {id:2})"));
        assertEquals("(createType node Company (name:STRING capital:INT64?))",
                Gql.tree("CREATE NODE TYPE Company (name STRING, capital INT64 NULL)"));
        assertEquals("(alter rel holder note STRING)",
                Gql.tree("ALTER REL TYPE holder ADD note STRING NULL"));
    }

    @Test
    void 中文和空查询不能通过() {
        ParseException chinese = assertThrows(ParseException.class, () -> Gql.parse("CREATE GRAPH 股东"));
        assertTrue(chinese.getMessage().contains("无法识别"));
        ParseException empty = assertThrows(ParseException.class, () -> Gql.parse("   "));
        assertTrue(empty.getMessage().contains("不能为空"));
        assertInstanceOf(Stmt.Match.class, Gql.parse("match (a:Company {id:1})-[:holder]->(b) return b.id;"));
    }
}
