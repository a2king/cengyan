package io.cengyan.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 对照 ISO/IEC 39075:2024：标准写法都要能解析，含义和标准一致。 */
class GqlStandardTest {
    private static Stmt.Select select(String gql) {
        return assertInstanceOf(Stmt.Select.class, Gql.parse(gql));
    }

    private static Stmt.Clause.Match firstMatch(String gql) {
        return assertInstanceOf(Stmt.Clause.Match.class, select(gql).clauses().get(0));
    }

    private static Expr where(String gql) {
        Stmt stmt = Gql.parse(gql);
        if (stmt instanceof Stmt.Match match) {
            return match.where();
        }
        return firstMatch(gql).where();
    }

    @Test
    void 两个减号是行注释() {
        Stmt.Match match = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (n:A) -- 注释\nRETURN n -- c"));
        assertEquals(1, match.items().size());
        assertEquals("n", match.items().get(0).text());
        assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (n:A) RETURN n // 也是注释"));
    }

    @Test
    void 属性默认可空写NOT_NULL才不能为空() {
        Stmt.CreateType type = assertInstanceOf(Stmt.CreateType.class,
                Gql.parse("CREATE NODE TYPE P (a STRING, b INT64 NOT NULL, c DOUBLE NULL)"));
        assertTrue(type.fields().get(0).nullable());
        assertFalse(type.fields().get(1).nullable());
        assertTrue(type.fields().get(2).nullable());
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("ALTER NODE TYPE P ADD d STRING NOT NULL"))
                .getMessage().contains("可空"));
        assertEquals("ADD", assertInstanceOf(Stmt.AlterType.class, Gql.parse("ALTER NODE TYPE P ADD d STRING")).action());
    }

    @Test
    void 标签表达式用与或非和括号() {
        Stmt.NodePat node = ((Stmt.Element.Node) firstMatch("MATCH (n:(A|B)&!C) RETURN n").patterns().get(0).elements().get(0)).node();
        Stmt.Label.And and = assertInstanceOf(Stmt.Label.And.class, node.label());
        assertInstanceOf(Stmt.Label.Or.class, and.options().get(0));
        assertInstanceOf(Stmt.Label.Not.class, and.options().get(1));
        // ! 高于 & 高于 |
        Stmt.Label.Or or = assertInstanceOf(Stmt.Label.Or.class,
                ((Stmt.Element.Node) firstMatch("MATCH (n:A&B|C) RETURN n").patterns().get(0).elements().get(0)).node().label());
        assertInstanceOf(Stmt.Label.And.class, or.options().get(0));
        // IS 可以代替冒号
        assertEquals("A", assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (n IS A) RETURN n")).source().typeName());
        // 边上也是完整的标签表达式
        Stmt.EdgePat edge = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-[e:!R]->(b) RETURN b")).edge();
        assertInstanceOf(Stmt.Label.Not.class, edge.label());
        assertTrue(edge.typeNames().isEmpty());
        Stmt.EdgePat union = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-[e:R|S]->(b) RETURN b")).edge();
        assertEquals(List.of("R", "S"), union.typeNames());
        assertNull(union.typeName());
    }

    @Test
    void USE是查询前缀SESSION_SET_GRAPH切换会话图() {
        Stmt.UseQuery use = assertInstanceOf(Stmt.UseQuery.class, Gql.parse("USE g MATCH (n) RETURN n"));
        assertEquals("g", use.graph());
        assertEquals("g", assertInstanceOf(Stmt.Use.class, Gql.parse("USE g")).name());
        assertEquals("g", assertInstanceOf(Stmt.Use.class, Gql.parse("SESSION SET GRAPH g")).name());
        assertEquals("g", assertInstanceOf(Stmt.Use.class, Gql.parse("SESSION SET PROPERTY GRAPH g")).name());
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("START TRANSACTION")).getMessage().contains("事务"));
    }

    @Test
    void 路径搜索前缀和每个路径模式自己的路径变量() {
        Stmt.Pattern any = firstMatch("MATCH p = ANY (a)-[:R]->+(b) RETURN p").patterns().get(0);
        assertEquals("ANY", any.search());
        assertEquals(1, any.searchCount());
        Stmt.Pattern all = firstMatch("MATCH p = ALL SHORTEST TRAIL PATHS (a)-[:R]->*(b) RETURN p").patterns().get(0);
        assertEquals("ALL_SHORTEST", all.search());
        assertEquals("TRAIL", all.mode());
        Stmt.Pattern groups = firstMatch("MATCH p = SHORTEST 2 GROUPS (a)-[:R]->{1,5}(b) RETURN p").patterns().get(0);
        assertEquals("SHORTEST_GROUPS", groups.search());
        assertEquals(2, groups.searchCount());
        assertNull(firstMatch("MATCH p = ALL (a)-[:R]->{1,3}(b) RETURN p").patterns().get(0).search(), "单独的 ALL 等于不写");
        Stmt.Clause.Match two = firstMatch("MATCH p = ANY SHORTEST (a)-[:R]->*(b), q = TRAIL (b)-[:S]->{1,2}(c) RETURN p, q");
        assertEquals("p", two.patterns().get(0).pathVariable());
        assertEquals("q", two.patterns().get(1).pathVariable());
        assertEquals("TRAIL", two.patterns().get(1).mode());
        assertNull(two.patterns().get(1).search());
    }

    @Test
    void 量词可以省略下界或写问号() {
        Stmt.EdgePat edge = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-[:R]->{,3}(b) RETURN b")).edge();
        assertEquals(0, edge.minHop());
        assertEquals(3, edge.maxHop());
        Stmt.EdgePat optional = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-[:R]->?(b) RETURN b")).edge();
        assertEquals(0, optional.minHop());
        assertEquals(1, optional.maxHop());
    }

    @Test
    void EXISTS里可以只写图模式或MATCH() {
        for (String gql : List.of(
                "MATCH (n:A) WHERE EXISTS { (n)-[:R]->(m) } RETURN n",
                "MATCH (n:A) WHERE EXISTS { MATCH (n)-[:R]->(m) } RETURN n",
                "MATCH (n:A) WHERE EXISTS ( MATCH (n)-[:R]->(m) ) RETURN n",
                "MATCH (n:A) WHERE EXISTS { MATCH (n)-[:R]->(m) RETURN m } RETURN n")) {
            assertInstanceOf(Expr.Exists.class, where(gql), gql);
        }
        assertTrue(assertThrows(ParseException.class,
                () -> Gql.parse("MATCH (n) RETURN VALUE { MATCH (m) SET m.x = 1 RETURN m }")).getMessage().contains("不能写入"));
    }

    @Test
    void OR和XOR同一优先级左结合() {
        Expr.Binary or = assertInstanceOf(Expr.Binary.class, where("MATCH (n:A) WHERE n.x = 1 XOR n.y = 2 AND n.z = 3 RETURN n"));
        assertEquals("XOR", or.op());
        assertEquals("AND", assertInstanceOf(Expr.Binary.class, or.right()).op());
        Expr.Binary mixed = assertInstanceOf(Expr.Binary.class, where("MATCH (n:A) WHERE n.a OR n.b XOR n.c RETURN n"));
        assertEquals("XOR", mixed.op());
        assertEquals("OR", assertInstanceOf(Expr.Binary.class, mixed.left()).op());
    }

    @Test
    void 写入之后可以RETURN_REMOVE可以删属性() {
        Stmt.Mutate set = assertInstanceOf(Stmt.Mutate.class, Gql.parse("MATCH (n:A) SET n.x = 1, n.y = 2 REMOVE n.z RETURN n"));
        assertEquals(2, set.assignments().size());
        assertEquals(List.of("n.z"), set.removals());
        assertEquals(1, set.tail().items().size());
        Stmt.Mutate remove = assertInstanceOf(Stmt.Mutate.class, Gql.parse("MATCH (n:A) REMOVE n.age, n.name"));
        assertEquals(List.of("n.age", "n.name"), remove.removals());
        Stmt.Mutate label = assertInstanceOf(Stmt.Mutate.class, Gql.parse("MATCH (n:A) SET n:A REMOVE n IS B"));
        assertEquals("A", label.labelSets().get(0).field());
        assertEquals("B", label.labelRemovals().get(0).field());
        Stmt.InsertGraph insert = assertInstanceOf(Stmt.InsertGraph.class, Gql.parse("INSERT (a:A {id: 1}) RETURN a"));
        assertEquals(1, insert.tail().items().size());
        assertInstanceOf(Stmt.InsertUsing.class, Gql.parse("MATCH (a:A {id: 1}) INSERT (a)-[:R {id: 9}]->(b:A {id: 2}) RETURN b"));
        assertInstanceOf(Stmt.Mutate.class, Gql.parse("MATCH (n:A) DETACH DELETE n FINISH"));
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("MATCH (n:A) SET n.x = 1 MATCH (m) RETURN m"))
                .getMessage().contains("写入之后"));
    }

    @Test
    void NEXT和集合运算右边可以是任意线性查询() {
        Stmt.Next next = assertInstanceOf(Stmt.Next.class, Gql.parse("MATCH (n:A) RETURN n NEXT FILTER n.x > 1 RETURN n"));
        assertInstanceOf(Stmt.Select.class, next.right());
        Stmt.Next yield = assertInstanceOf(Stmt.Next.class, Gql.parse("MATCH (n:A) RETURN n, n.x AS x NEXT YIELD x AS y RETURN y"));
        assertEquals("y", yield.yield().get(0).alias());
        assertInstanceOf(Stmt.Composite.class, Gql.parse("MATCH (n:A) RETURN n.x AS v UNION ALL RETURN 1 AS v"));
        Stmt.Composite otherwise = assertInstanceOf(Stmt.Composite.class, Gql.parse("MATCH (n:A) RETURN n OTHERWISE MATCH (m:B) RETURN m AS n"));
        assertEquals("OTHERWISE", otherwise.op());
        // NEXT 比 UNION 结合得松：(A UNION B) NEXT C
        Stmt.Next loose = assertInstanceOf(Stmt.Next.class, Gql.parse("RETURN 1 AS v UNION RETURN 2 AS v NEXT RETURN v"));
        assertInstanceOf(Stmt.Composite.class, loose.left());
    }

    @Test
    void 管道语句可以写在任何位置() {
        assertEquals(1, select("LET x = 1 RETURN x").clauses().size());
        assertEquals(2, select("MATCH (n:A) LET x = 1, y = x + 1 RETURN y").clauses().size() - 1);
        Stmt.Clause.For loop = assertInstanceOf(Stmt.Clause.For.class, select("FOR x IN [1, 2] WITH ORDINALITY i RETURN x, i").clauses().get(0));
        assertEquals("i", loop.position());
        assertFalse(loop.zeroBased());
        assertTrue(((Stmt.Clause.For) select("FOR x IN [1] WITH OFFSET i RETURN i").clauses().get(0)).zeroBased());
        assertInstanceOf(Stmt.Clause.OrderPage.class, select("MATCH (n:A) ORDER BY n.x DESC LIMIT 3 RETURN n").clauses().get(1));
        assertInstanceOf(Stmt.Clause.OrderPage.class, select("MATCH (n:A) OFFSET 2 RETURN n").clauses().get(1));
        assertInstanceOf(Stmt.Clause.Filter.class, select("MATCH (n:A) FILTER WHERE n.x > 1 RETURN n").clauses().get(1));
        assertInstanceOf(Stmt.Clause.Call.class, select("MATCH (n:A) CALL path.shortest({start: 1, end: 2, rel: 'r'}) YIELD length AS l RETURN l").clauses().get(1));
        Stmt.Clause.Inline inline = assertInstanceOf(Stmt.Clause.Inline.class,
                select("MATCH (n:A) CALL (n) { MATCH (n)-[:R]->(m) RETURN count(*) AS c } RETURN n, c").clauses().get(1));
        assertEquals(List.of("n"), inline.scope());
        assertTrue(assertInstanceOf(Stmt.Clause.Inline.class,
                select("MATCH (n:A) OPTIONAL CALL { RETURN 1 AS one } RETURN one").clauses().get(1)).optional());
        assertInstanceOf(Stmt.Clause.OptionalBlock.class, select("MATCH (n:A) OPTIONAL { MATCH (n)-[:R]->(m) MATCH (m)-[:S]->(k) } RETURN k").clauses().get(1));
        Stmt.Call call = assertInstanceOf(Stmt.Call.class, Gql.parse("CALL path.shortest({start: 1, end: 2, rel: 'r'})"));
        assertTrue(call.yield().isEmpty(), "YIELD 可以不写");
        assertTrue(select("MATCH (n:A&B) FINISH").tail().finish());
        assertTrue(select("MATCH (n:A&B) RETURN NO BINDINGS").tail().finish());
    }

    @Test
    void 缩写边和左右边() {
        assertEquals(Stmt.EdgePat.Direction.OUT, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)->(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.IN, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)<-(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.ANY, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.ANY, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)<->(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.ANY, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)<-[e:R]->(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.IN, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)<~[e:R]~(b) RETURN b")).edge().direction());
        assertEquals(Stmt.EdgePat.Direction.OUT, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)~[e:R]~>(b) RETURN b")).edge().direction());
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("MATCH (a)~[e:R]~(b) RETURN b")).getMessage().contains("无向"));
        assertEquals(Stmt.EdgePat.Direction.OUT, assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)->{1,3}(b) RETURN b")).edge().direction());
        Stmt.EdgePat filtered = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (a)-[e:R WHERE e.w > 1]->(b) RETURN b")).edge();
        assertInstanceOf(Expr.Binary.class, filtered.where());
        // a<-1 是 a < -1
        Expr.Binary less = assertInstanceOf(Expr.Binary.class, where("MATCH (n:A) WHERE n.x<-1 RETURN n"));
        assertEquals("<", less.op());
    }

    @Test
    void 路径并集和匹配模式() {
        Stmt.Pattern union = firstMatch("MATCH (a:A)-[:R]->(b) | (a:A)-[:S]->(b) RETURN b").patterns().get(0);
        assertEquals(1, union.union().size());
        assertFalse(union.multiset());
        assertTrue(firstMatch("MATCH (a:A)-[:R]->(b) |+| (a:A)-[:S]->(b) RETURN b").patterns().get(0).multiset());
        assertEquals("DIFFERENT_EDGES", firstMatch("MATCH DIFFERENT EDGES (a)-[e]->(b), (b)-[f]->(c) RETURN c").matchMode());
        assertEquals("REPEATABLE_ELEMENTS", firstMatch("MATCH REPEATABLE ELEMENTS (a)-[e]->(b) RETURN b").matchMode());
    }

    @Test
    void 谓词和表达式() {
        assertInstanceOf(Expr.Labeled.class, where("MATCH (n:A) WHERE n:A|B RETURN n"));
        Expr.Binary labeled = assertInstanceOf(Expr.Binary.class, where("MATCH (n:A) WHERE n:A AND n.x = 1 RETURN n"));
        assertInstanceOf(Expr.Labeled.class, labeled.left());
        assertInstanceOf(Expr.Truth.class, where("MATCH (n:A) WHERE n.ok IS NOT UNKNOWN RETURN n"));
        assertInstanceOf(Expr.Directed.class, where("MATCH (a)-[e:R]->(b) WHERE e IS DIRECTED RETURN b"));
        Stmt.Match match = assertInstanceOf(Stmt.Match.class, Gql.parse(
                "MATCH (n:A) RETURN SAME(n, n), ALL_DIFFERENT(n, n), PROPERTY_EXISTS(n, name), TRIM(LEADING 'x' FROM n.s), "
                        + "LET a = 1, b = a + 1 IN b END, LIST[1, 2], RECORD {k: 1}, CURRENT_TIMESTAMP, 1.5d, n.xs[1:2]"));
        assertInstanceOf(Expr.Same.class, match.items().get(0).expr());
        assertTrue(((Expr.Same) match.items().get(1).expr()).different());
        assertEquals("name", assertInstanceOf(Expr.PropExists.class, match.items().get(2).expr()).property());
        assertEquals("LEADING", assertInstanceOf(Expr.Trim.class, match.items().get(3).expr()).spec());
        assertInstanceOf(Expr.LetIn.class, match.items().get(4).expr());
        assertInstanceOf(Expr.ListLit.class, match.items().get(5).expr());
        assertInstanceOf(Expr.MapLit.class, match.items().get(6).expr());
        assertInstanceOf(Expr.Call.class, match.items().get(7).expr());
        assertEquals(1.5d, assertInstanceOf(Expr.Literal.class, match.items().get(8).expr()).value());
        assertInstanceOf(Expr.Index.class, match.items().get(9).expr());
        assertInstanceOf(Expr.ValueQuery.class, assertInstanceOf(Stmt.Select.class,
                Gql.parse("MATCH (n:A&B) RETURN VALUE { MATCH (n)-[:R]->(m) RETURN count(*) } AS c")).tail().items().get(0).expr());
        assertInstanceOf(Expr.Typed.class, where("MATCH (n:A) WHERE n.x IS :: INT64 RETURN n"));
    }

    @Test
    void 名字可以是Unicode字母() {
        Stmt.Match match = assertInstanceOf(Stmt.Match.class, Gql.parse("MATCH (公司:企业 {名称: '层岩'}) RETURN 公司.名称 AS 名称"));
        assertEquals("公司", match.source().variable());
        assertEquals("企业", match.source().typeName());
        assertEquals("名称", match.items().get(0).alias());
    }

    @Test
    void 标准建图写法() {
        Stmt.CreateGraph graph = assertInstanceOf(Stmt.CreateGraph.class, Gql.parse("""
                CREATE GRAPH IF NOT EXISTS g {
                  (p :Person {name STRING NOT NULL, age INT64}),
                  (:Company {name STRING}),
                  (:Person)-[:works {since INT64}]->(:Company),
                  NODE City {name STRING},
                  DIRECTED EDGE lives {since INT} CONNECTING (Person -> City)
                } PARTS 3 COMMENT 'x'"""));
        assertTrue(graph.ifNotExists());
        assertEquals(3, graph.parts());
        assertEquals(List.of("Person", "Company", "works", "City", "lives"), graph.types().stream().map(Stmt.CreateType::name).toList());
        assertFalse(graph.types().get(0).fields().get(0).nullable());
        assertTrue(graph.types().get(0).fields().get(1).nullable());
        assertFalse(graph.types().get(2).node());
        assertEquals("other", assertInstanceOf(Stmt.CreateGraph.class, Gql.parse("CREATE PROPERTY GRAPH g LIKE other")).like());
        assertTrue(assertInstanceOf(Stmt.CreateGraph.class, Gql.parse("CREATE GRAPH g ANY")).types().isEmpty());
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("CREATE GRAPH g { (:A&B) }")).getMessage().contains("一个标签"));
        assertEquals("INT64", assertInstanceOf(Stmt.CreateType.class, Gql.parse("CREATE NODE TYPE P (a BIGINT)")).fields().get(0).valueType());
        assertEquals("DOUBLE", assertInstanceOf(Stmt.CreateType.class, Gql.parse("CREATE NODE TYPE P (a FLOAT64)")).fields().get(0).valueType());
        assertEquals("DATETIME", assertInstanceOf(Stmt.CreateType.class, Gql.parse("CREATE NODE TYPE P (a ZONED DATETIME)")).fields().get(0).valueType());
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("CREATE NODE TYPE P (a INT8)")).getMessage().contains("INT64"));
        assertTrue(assertThrows(ParseException.class, () -> Gql.parse("CREATE NODE TYPE P (a DATE)")).getMessage().contains("DATE"));
    }
}
