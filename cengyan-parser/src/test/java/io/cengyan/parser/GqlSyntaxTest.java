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
        assertTrue(Gql.tree("MATCH (a:Company {id:1})-[e:holder]->{1,10}(b) RETURN b.id")
                .contains("(edge OUT e:holder{1,10})"));
        assertTrue(Gql.tree("MATCH (a:Company {id:1})-[:holder]->{3}(b) RETURN b.id")
                .contains("(edge OUT:holder{3})"));
        assertEquals("(insert (node a:Company id=1 name='root') (edge OUT:holder@3 ratio=0.5) (node b id=2))",
                Gql.tree("INSERT (a:Company {id:1, name:'root'})-[:holder@3 {ratio:0.5}]->(b {id:2})"));
        assertEquals("(createType node Company (name:STRING capital:INT64?))",
                Gql.tree("CREATE NODE TYPE Company (name STRING, capital INT64 NULL)"));
        assertEquals("(alter rel holder note STRING)",
                Gql.tree("ALTER REL TYPE holder ADD note STRING NULL"));
        assertEquals("(set (node a:Company id=1) (edge OUT e:holder) (node b id=2) (assign e.ratio=0.2))",
                Gql.tree("MATCH (a:Company {id:1})-[e:holder]->(b {id:2}) SET e.ratio = 0.2"));
        assertEquals("(delete (node a id=1) (vars a))",
                Gql.tree("MATCH (a {id:1}) DELETE a"));
        assertTrue(Gql.tree("MATCH (a:Company {id:1})-[:holder]->(b) RETURN DISTINCT b.id").contains("distinct"));
        assertInstanceOf(Expr.Call.class, ((Stmt.Match) Gql.parse(
                "MATCH (a:Company {id:1})-[:holder]->(b) RETURN count(*)")).items().get(0).expr());
        assertEquals("(call path.shortest start=1 end=2 rel='holder' hops=10 (yield length vids))",
                Gql.tree("CALL path.shortest({start: 1, end: 2, rel: 'holder', hops: 10}) YIELD length, vids"));
        assertEquals("(call ownership.largest_holder start=1 rel='holder' ratioProp='ratio' hops=10 (yield vid ratio))",
                Gql.tree("CALL ownership.largest_holder({start: 1, rel: \"holder\", ratioProp: \"ratio\", hops: 10}) YIELD vid, ratio"));
        assertEquals("(createIndex unique by_name node Company name,capital)",
                Gql.tree("CREATE UNIQUE INDEX by_name ON NODE TYPE Company (name, capital)"));
        assertEquals("(createIndex by_ratio rel holder ratio)",
                Gql.tree("CREATE INDEX by_ratio ON REL TYPE holder (ratio)"));
        assertEquals("(dropIndex by_name)", Gql.tree("DROP INDEX by_name"));
        assertEquals("(show INDEXES)", Gql.tree("SHOW INDEXES"));
    }

    @Test
    void 所有路径直接拒绝() {
        ParseException all = assertThrows(ParseException.class,
                () -> Gql.parse("MATCH (a:Company {id:1})-[:holder]->*(b) RETURN b.id"));
        assertTrue(all.getMessage().contains("无上界"));
        assertTrue(all.getMessage().contains("LIMIT"));
        ParseException emptyIndex = assertThrows(ParseException.class,
                () -> Gql.parse("CREATE INDEX idx ON NODE TYPE Company ()"));
        assertTrue(emptyIndex.getMessage().contains("至少要有一列"));
    }

    @Test
    void 中文和空查询不能通过() {
        ParseException chinese = assertThrows(ParseException.class, () -> Gql.parse("CREATE GRAPH 股东"));
        assertTrue(chinese.getMessage().contains("无法识别"));
        ParseException empty = assertThrows(ParseException.class, () -> Gql.parse("   "));
        assertTrue(empty.getMessage().contains("不能为空"));
        assertInstanceOf(Stmt.Match.class, Gql.parse("match (a:Company {id:1})-[:holder]->(b) return b.id;"));
    }

    @Test
    void 多种边和不写边() {
        assertEquals("(match (node a:Company id=1) (edge OUT e) (node b) (return b.id))",
                Gql.tree("MATCH (a:Company {id:1})-[e]->(b) RETURN b.id"));
        assertEquals("(match (node a id=1) (edge OUT e:holder|works) (node b) (return b.id))",
                Gql.tree("MATCH (a {id:1})-[e:holder|:works]->(b) RETURN b.id"));
        assertEquals("(match (node a id=1) (edge OUT:holder|works) (node b) (return b.id))",
                Gql.tree("MATCH (a {id:1})-[:holder|works]->(b) RETURN b.id"));
        assertEquals("(match (node p:org id=100) (return p))",
                Gql.tree("MATCH (p:org {id: 100}) RETURN p"));
        assertEquals("(match p = (node a id=1) (edge OUT:holder{1,3}) (node b) (return p) (limit 20))",
                Gql.tree("MATCH p = (a {id:1})-[:holder]->{1,3}(b) RETURN p LIMIT 20"));
        ParseException duplicated = assertThrows(ParseException.class,
                () -> Gql.parse("MATCH (a {id:1})-[:holder|:holder]->(b) RETURN a"));
        assertTrue(duplicated.getMessage().contains("边类型重复"));
        assertInstanceOf(Stmt.Insert.class, Gql.parse("INSERT (a {id:1})-[e]->(b {id:2})"));
    }

    @Test
    void 关键字可以作为变量和函数名使用() {
        assertTrue(Gql.tree("MATCH (type:Company {id:1})-[type:holder]->(b) RETURN type(type)")
                .contains("type(type)"));
    }

    @Test
    void schema注释可以写在图类型和字段上() {
        Stmt.CreateGraph graph = (Stmt.CreateGraph) Gql.parse("CREATE GRAPH Company COMMENT '企业图谱'");
        assertEquals("企业图谱", graph.comment());
        Stmt.CreateType type = (Stmt.CreateType) Gql.parse(
                "CREATE NODE TYPE Company COMMENT '企业节点' ID STRING(32) (name STRING NULL COMMENT '名称')");
        assertEquals("企业节点", type.comment());
        assertEquals("名称", type.fields().get(0).comment());
    }
}
