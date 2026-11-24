package io.cengyan.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 手写的 GQL 解析器，规则对照 ISO/IEC 39075:2024 与 {@code Gql.g4}。
 * 关键字不区分大小写，标识符保留原来的大小写；除少数保留字外，关键字都可以当名字用。
 */
final class GqlReader {
    private static final Map<String, Kind> KEYWORDS = Map.ofEntries(
            Map.entry("bool", Kind.BOOL),
            Map.entry("boolean", Kind.BOOLEAN),
            Map.entry("int", Kind.INT),
            Map.entry("int64", Kind.INT64),
            Map.entry("integer", Kind.INTEGER_WORD),
            Map.entry("double", Kind.DOUBLE),
            Map.entry("string", Kind.STRING),
            Map.entry("id", Kind.ID),
            Map.entry("datetime", Kind.DATETIME),
            Map.entry("comment", Kind.COMMENT),
            Map.entry("create", Kind.CREATE),
            Map.entry("graph", Kind.GRAPH),
            Map.entry("parts", Kind.PARTS),
            Map.entry("replicas", Kind.REPLICAS),
            Map.entry("graphs", Kind.GRAPHS),
            Map.entry("use", Kind.USE),
            Map.entry("alias", Kind.ALIAS),
            Map.entry("aliases", Kind.ALIASES),
            Map.entry("for", Kind.FOR),
            Map.entry("drop", Kind.DROP),
            Map.entry("show", Kind.SHOW),
            Map.entry("node", Kind.NODE),
            Map.entry("rel", Kind.REL),
            Map.entry("types", Kind.TYPES),
            Map.entry("type", Kind.TYPE),
            Map.entry("describe", Kind.DESCRIBE),
            Map.entry("alter", Kind.ALTER),
            Map.entry("add", Kind.ADD),
            Map.entry("null", Kind.NULL),
            Map.entry("insert", Kind.INSERT),
            Map.entry("match", Kind.MATCH),
            Map.entry("explain", Kind.EXPLAIN),
            Map.entry("where", Kind.WHERE),
            Map.entry("return", Kind.RETURN),
            Map.entry("order", Kind.ORDER),
            Map.entry("by", Kind.BY),
            Map.entry("limit", Kind.LIMIT),
            Map.entry("skip", Kind.SKIP),
            Map.entry("and", Kind.AND),
            Map.entry("or", Kind.OR),
            Map.entry("not", Kind.NOT),
            Map.entry("is", Kind.IS),
            Map.entry("in", Kind.IN),
            Map.entry("as", Kind.AS),
            Map.entry("asc", Kind.ASC),
            Map.entry("ascending", Kind.ASC),
            Map.entry("desc", Kind.DESC),
            Map.entry("descending", Kind.DESC),
            Map.entry("set", Kind.SET),
            Map.entry("delete", Kind.DELETE),
            Map.entry("distinct", Kind.DISTINCT),
            Map.entry("call", Kind.CALL),
            Map.entry("yield", Kind.YIELD),
            Map.entry("index", Kind.INDEX),
            Map.entry("indexes", Kind.INDEXES),
            Map.entry("unique", Kind.UNIQUE),
            Map.entry("on", Kind.ON),
            Map.entry("true", Kind.TRUE),
            Map.entry("false", Kind.FALSE),
            Map.entry("unknown", Kind.UNKNOWN),
            Map.entry("if", Kind.IF),
            Map.entry("exists", Kind.EXISTS),
            Map.entry("optional", Kind.OPTIONAL),
            Map.entry("filter", Kind.FILTER),
            Map.entry("let", Kind.LET),
            Map.entry("group", Kind.GROUP),
            Map.entry("groups", Kind.GROUPS),
            Map.entry("union", Kind.UNION),
            Map.entry("except", Kind.EXCEPT),
            Map.entry("intersect", Kind.INTERSECT),
            Map.entry("otherwise", Kind.OTHERWISE),
            Map.entry("all", Kind.ALL),
            Map.entry("any", Kind.ANY),
            Map.entry("next", Kind.NEXT),
            Map.entry("offset", Kind.OFFSET),
            Map.entry("detach", Kind.DETACH),
            Map.entry("nodetach", Kind.NODETACH),
            Map.entry("remove", Kind.REMOVE),
            Map.entry("rename", Kind.RENAME),
            Map.entry("to", Kind.TO),
            Map.entry("cast", Kind.CAST),
            Map.entry("case", Kind.CASE),
            Map.entry("when", Kind.WHEN),
            Map.entry("then", Kind.THEN),
            Map.entry("else", Kind.ELSE),
            Map.entry("end", Kind.END),
            Map.entry("walk", Kind.WALK),
            Map.entry("trail", Kind.TRAIL),
            Map.entry("simple", Kind.SIMPLE),
            Map.entry("acyclic", Kind.ACYCLIC),
            Map.entry("shortest", Kind.SHORTEST),
            Map.entry("path", Kind.PATH),
            Map.entry("paths", Kind.PATHS),
            Map.entry("xor", Kind.XOR),
            Map.entry("typed", Kind.TYPED),
            Map.entry("labeled", Kind.LABELED),
            Map.entry("source", Kind.SOURCE),
            Map.entry("destination", Kind.DESTINATION),
            Map.entry("directed", Kind.DIRECTED),
            Map.entry("of", Kind.OF),
            Map.entry("same", Kind.SAME),
            Map.entry("session", Kind.SESSION),
            Map.entry("property", Kind.PROPERTY),
            Map.entry("start", Kind.START),
            Map.entry("transaction", Kind.TRANSACTION),
            Map.entry("commit", Kind.COMMIT),
            Map.entry("rollback", Kind.ROLLBACK),
            Map.entry("finish", Kind.FINISH),
            Map.entry("no", Kind.NO),
            Map.entry("bindings", Kind.BINDINGS),
            Map.entry("repeatable", Kind.REPEATABLE),
            Map.entry("different", Kind.DIFFERENT),
            Map.entry("element", Kind.ELEMENT),
            Map.entry("elements", Kind.ELEMENTS),
            Map.entry("edge", Kind.EDGE),
            Map.entry("edges", Kind.EDGES),
            Map.entry("relationship", Kind.EDGE),
            Map.entry("relationships", Kind.EDGES),
            Map.entry("keep", Kind.KEEP),
            Map.entry("with", Kind.WITH),
            Map.entry("ordinality", Kind.ORDINALITY),
            Map.entry("value", Kind.VALUE),
            Map.entry("list", Kind.LIST),
            Map.entry("array", Kind.LIST),
            Map.entry("record", Kind.RECORD),
            Map.entry("like", Kind.LIKE),
            Map.entry("typed_graph", Kind.IDENT),
            Map.entry("label", Kind.LABEL),
            Map.entry("labels", Kind.LABELS),
            Map.entry("connecting", Kind.CONNECTING),
            Map.entry("undirected", Kind.UNDIRECTED),
            Map.entry("leading", Kind.LEADING),
            Map.entry("trailing", Kind.TRAILING),
            Map.entry("both", Kind.BOTH),
            Map.entry("from", Kind.FROM),
            Map.entry("replace", Kind.REPLACE));

    /** 写成名字时当作无参函数调用的标准函数，例如 {@code RETURN CURRENT_DATE}。 */
    private static final java.util.Set<String> NILADIC = java.util.Set.of(
            "current_date", "current_time", "current_timestamp", "local_time", "local_timestamp", "local_datetime");

    private final String source;
    private final List<Tok> tokens;
    private int cursor;
    /** 大于 0 时不把 {@code x:Label} 当成标签谓词，用于下标切片 {@code list[a:b]}。 */
    private int sliceDepth;
    /** 大于 0 时不把 {@code x IN ...} 当成列表成员谓词，用于 {@code LET a = ... IN 表达式 END} 的绑定值。 */
    private int letDepth;

    private GqlReader(String source) {
        this.source = source;
        this.tokens = tokenize(source);
    }

    static List<Stmt> parseAll(String source) {
        if (source == null || source.isBlank()) {
            throw new ParseException("查询不能为空");
        }
        return new GqlReader(source).script();
    }

    private List<Stmt> script() {
        List<Stmt> statements = new ArrayList<>();
        statements.add(statement());
        while (take(Kind.SEMI) && peek() != Kind.EOF) {
            statements.add(statement());
        }
        take(Kind.SEMI);
        expect(Kind.EOF, "语句后面还有多余内容");
        return statements;
    }

    private Stmt statement() {
        return switch (peek()) {
            case CREATE -> create();
            case USE -> {
                next();
                String graph = graphName();
                if (queryStart(peek())) {
                    yield new Stmt.UseQuery(graph, block());
                }
                yield new Stmt.Use(graph);
            }
            case SESSION -> session();
            case START, COMMIT, ROLLBACK -> fail("不支持跨语句事务：每条语句单独提交，失败不留下半截结果");
            case DROP -> drop();
            case SHOW -> show();
            case DESCRIBE -> describe();
            case ALTER -> alter();
            case EXPLAIN -> {
                next();
                Stmt explained = statement();
                if (explained instanceof Stmt.Match match) {
                    yield new Stmt.Explain(match);
                }
                yield new Stmt.ExplainQuery(explained);
            }
            default -> {
                if (queryStart(peek())) {
                    yield block();
                }
                yield fail("无法识别的语句");
            }
        };
    }

    /** 能开始一段线性查询的关键字。 */
    private boolean queryStart(Kind kind) {
        return switch (kind) {
            case MATCH, OPTIONAL, RETURN, FILTER, LET, ORDER, OFFSET, SKIP, LIMIT, CALL, INSERT, FINISH -> true;
            case FOR -> look(1) != Kind.EQ;
            default -> false;
        };
    }

    /** 图名：标准允许写目录路径，这里只有一层名字。 */
    private String graphName() {
        if (peek() == Kind.SLASH) {
            return fail("层岩没有目录和模式，图名直接写名字");
        }
        return ident();
    }

    private Stmt session() {
        expect(Kind.SESSION, "缺少 SESSION");
        if (take(Kind.SET)) {
            take(Kind.PROPERTY);
            if (take(Kind.GRAPH)) {
                return new Stmt.Use(graphName());
            }
        }
        return fail("层岩不保存服务端会话状态，只支持 SESSION SET GRAPH");
    }

    // ---------------------------------------------------------------- 查询的组合

    /** {@code 复合查询 (NEXT [YIELD ...] 复合查询)*}。NEXT 比集合运算结合得松。 */
    private Stmt block() {
        Stmt left = composite();
        while (take(Kind.NEXT)) {
            List<Stmt.Item> yield = List.of();
            if (take(Kind.YIELD)) {
                yield = items();
            }
            if (!queryStart(peek())) {
                return fail("NEXT 后面要写查询");
            }
            left = new Stmt.Next(left, composite(), yield);
        }
        return left;
    }

    /** {@code 线性查询 ((UNION | EXCEPT | INTERSECT) [ALL | DISTINCT] | OTHERWISE) 线性查询 ...}，左结合。 */
    private Stmt composite() {
        Stmt left = linear();
        while (peek() == Kind.UNION || peek() == Kind.EXCEPT || peek() == Kind.INTERSECT || peek() == Kind.OTHERWISE) {
            String op = next().text.toUpperCase(Locale.ROOT);
            boolean all = false;
            if (!op.equals("OTHERWISE")) {
                all = take(Kind.ALL);
                if (!all) {
                    take(Kind.DISTINCT);
                }
            }
            if (!queryStart(peek())) {
                return fail("集合运算后面要写查询");
            }
            left = new Stmt.Composite(op, all, left, linear());
        }
        return left;
    }

    /**
     * 一段线性查询：若干查询语句（MATCH、OPTIONAL、FILTER、LET、FOR、ORDER BY / OFFSET / LIMIT、CALL），
     * 以 RETURN / FINISH 结尾，或者以写入语句（INSERT、SET、REMOVE、DELETE）结尾，写入之后可以再写 RETURN。
     */
    private Stmt linear() {
        List<Stmt.Clause> clauses = new ArrayList<>();
        Stmt.Call standalone = null;
        while (true) {
            Kind kind = peek();
            if (kind == Kind.MATCH) {
                next();
                clauses.add(matchClause(false));
            } else if (kind == Kind.OPTIONAL) {
                next();
                if (take(Kind.MATCH)) {
                    clauses.add(matchClause(true));
                } else if (peek() == Kind.LC || peek() == Kind.LP) {
                    clauses.add(optionalBlock());
                } else if (peek() == Kind.CALL) {
                    clauses.add(callClause(true));
                } else {
                    return fail("OPTIONAL 后面要写 MATCH、CALL 或 { MATCH ... }");
                }
            } else if (kind == Kind.FILTER) {
                next();
                take(Kind.WHERE);
                clauses.add(new Stmt.Clause.Filter(expr()));
            } else if (kind == Kind.LET) {
                next();
                do {
                    String name = ident();
                    expect(Kind.EQ, "LET 后面要写等号");
                    clauses.add(new Stmt.Clause.Let(name, expr()));
                } while (take(Kind.COMMA));
            } else if (kind == Kind.FOR && look(1) != Kind.EQ) {
                next();
                clauses.add(forClause());
            } else if (kind == Kind.ORDER || kind == Kind.OFFSET || kind == Kind.SKIP || kind == Kind.LIMIT) {
                clauses.add(orderPage());
            } else if (kind == Kind.CALL) {
                Stmt.Clause clause = callClause(false);
                clauses.add(clause);
                if (clauses.size() == 1 && clause instanceof Stmt.Clause.Call named) {
                    standalone = named.call();
                }
            } else {
                break;
            }
        }
        if (peek() == Kind.INSERT) {
            next();
            List<Stmt.Element> elements = insertElements();
            Stmt.Tail tail = optionalTail();
            mutationEnd();
            if (clauses.isEmpty()) {
                return insert(elements, tail);
            }
            return new Stmt.InsertUsing(List.copyOf(clauses), elements, tail);
        }
        if (peek() == Kind.SET || peek() == Kind.REMOVE || peek() == Kind.DELETE || peek() == Kind.DETACH
                || peek() == Kind.NODETACH) {
            if (clauses.isEmpty()) {
                return fail("写入前面要先用 MATCH 绑定变量");
            }
            return mutation(clauses);
        }
        if (peek() == Kind.RETURN || peek() == Kind.FINISH) {
            Stmt.Tail tail = tail();
            Expr aggregateFilter = take(Kind.FILTER) ? expr() : null;
            Stmt query = lower(clauses, tail);
            Stmt selected = query == null ? new Stmt.Select(List.copyOf(clauses), tail) : query;
            return aggregateFilter == null ? selected : new Stmt.AggregateFilter(selected, aggregateFilter);
        }
        if (standalone != null && clauses.size() == 1) {
            return standalone;
        }
        if (clauses.isEmpty()) {
            return fail("无法识别的语句");
        }
        return fail("缺少 RETURN");
    }

    private Stmt insert(List<Stmt.Element> elements, Stmt.Tail tail) {
        if (tail == null && elements.size() == 1 && elements.get(0) instanceof Stmt.Element.Node node) {
            return new Stmt.Insert(node.node(), null, null);
        }
        if (tail == null && elements.size() == 3
                && elements.get(0) instanceof Stmt.Element.Node source
                && elements.get(1) instanceof Stmt.Element.Rel rel
                && elements.get(2) instanceof Stmt.Element.Node target) {
            return new Stmt.Insert(source.node(), rel.edge(), target.node());
        }
        return new Stmt.InsertGraph(elements, tail);
    }

    /** 写入语句之后只能接 RETURN / FINISH，不能再写查询。 */
    private void mutationEnd() {
        if (peek() == Kind.MATCH || peek() == Kind.OPTIONAL || peek() == Kind.LET || peek() == Kind.FILTER
                || peek() == Kind.INSERT || peek() == Kind.CALL || (peek() == Kind.FOR && look(1) != Kind.EQ)) {
            fail("写入之后只能接 RETURN 或 FINISH，暂不支持在写入后继续查询");
        }
    }

    private Stmt.Tail optionalTail() {
        return peek() == Kind.RETURN || peek() == Kind.FINISH ? tail() : null;
    }

    private Stmt.Clause forClause() {
        String name = ident();
        expect(Kind.IN, "FOR 后面要写 IN");
        Expr list = expr();
        String position = null;
        boolean zeroBased = false;
        if (take(Kind.WITH)) {
            if (take(Kind.ORDINALITY)) {
                position = ident();
            } else if (take(Kind.OFFSET)) {
                position = ident();
                zeroBased = true;
            } else {
                return fail("FOR ... WITH 后面要写 ORDINALITY 或 OFFSET");
            }
        }
        return new Stmt.Clause.For(name, list, position, zeroBased);
    }

    private Stmt.Clause orderPage() {
        List<Stmt.Sort> order = List.of();
        if (take(Kind.ORDER)) {
            expect(Kind.BY, "ORDER 后面要写 BY");
            order = sorts();
        }
        Expr skip = null;
        if (take(Kind.SKIP) || take(Kind.OFFSET)) {
            skip = expr();
        }
        Expr limit = take(Kind.LIMIT) ? expr() : null;
        return new Stmt.Clause.OrderPage(order, skip, limit);
    }

    private Stmt.Clause optionalBlock() {
        boolean brace = take(Kind.LC);
        if (!brace) {
            expect(Kind.LP, "OPTIONAL 块缺少左括号");
        }
        List<Stmt.Clause> clauses = new ArrayList<>();
        while (true) {
            if (take(Kind.MATCH)) {
                clauses.add(matchClause(false));
            } else if (peek() == Kind.OPTIONAL && look(1) == Kind.MATCH) {
                next();
                next();
                clauses.add(matchClause(true));
            } else {
                break;
            }
        }
        if (clauses.isEmpty()) {
            return fail("OPTIONAL 块里要写 MATCH");
        }
        expect(brace ? Kind.RC : Kind.RP, "OPTIONAL 块没有结束");
        return new Stmt.Clause.OptionalBlock(List.copyOf(clauses));
    }

    /** 嵌套查询 {@code { ... }} 的内容。{@code needReturn} 为假时可以不写 RETURN（EXISTS 用）。 */
    private Stmt nested(boolean needReturn) {
        if (!needReturn && peek() == Kind.LP) {
            // EXISTS { (a)-[]->(b) }：直接写图模式。
            return new Stmt.Select(List.of(matchClause(false)), null);
        }
        if (needReturn) {
            return readOnly(block(), "子查询");
        }
        List<Stmt.Clause> clauses = new ArrayList<>();
        while (peek() == Kind.MATCH || peek() == Kind.OPTIONAL || peek() == Kind.FILTER || peek() == Kind.LET
                || (peek() == Kind.FOR && look(1) != Kind.EQ)) {
            if (take(Kind.MATCH)) {
                clauses.add(matchClause(false));
            } else if (take(Kind.OPTIONAL)) {
                expect(Kind.MATCH, "OPTIONAL 后面要写 MATCH");
                clauses.add(matchClause(true));
            } else if (take(Kind.FILTER)) {
                take(Kind.WHERE);
                clauses.add(new Stmt.Clause.Filter(expr()));
            } else if (take(Kind.LET)) {
                do {
                    String name = ident();
                    expect(Kind.EQ, "LET 后面要写等号");
                    clauses.add(new Stmt.Clause.Let(name, expr()));
                } while (take(Kind.COMMA));
            } else {
                next();
                clauses.add(forClause());
            }
        }
        if (clauses.isEmpty()) {
            return fail("子查询里要写 MATCH");
        }
        Stmt.Tail tail = optionalTail();
        return new Stmt.Select(List.copyOf(clauses), tail);
    }

    /** 嵌套查询只能读：写入在这里被拒绝，否则权限检查会把整条语句当成只读。 */
    private Stmt readOnly(Stmt statement, String where) {
        boolean write = switch (statement) {
            case Stmt.Insert ignored -> true;
            case Stmt.InsertGraph ignored -> true;
            case Stmt.InsertUsing ignored -> true;
            case Stmt.Set ignored -> true;
            case Stmt.Delete ignored -> true;
            case Stmt.Mutate ignored -> true;
            case Stmt.Composite composite -> {
                readOnly(composite.left(), where);
                readOnly(composite.right(), where);
                yield false;
            }
            case Stmt.Next next -> {
                readOnly(next.left(), where);
                readOnly(next.right(), where);
                yield false;
            }
            default -> false;
        };
        if (write) {
            fail(where + "里不能写入");
        }
        return statement;
    }

    private Stmt mutation(List<Stmt.Clause> clauses) {
        List<Stmt.Assign> assignments = new ArrayList<>();
        List<Stmt.Assign> replacements = new ArrayList<>();
        List<String> removals = new ArrayList<>();
        List<Stmt.Assign> labelSets = new ArrayList<>();
        List<Stmt.Assign> labelRemovals = new ArrayList<>();
        List<String> deletes = new ArrayList<>();
        boolean detach = false;
        boolean any = true;
        while (any) {
            any = false;
            if (take(Kind.SET)) {
                any = true;
                do {
                    String variable = ident();
                    if (take(Kind.EQ)) {
                        replacements.add(new Stmt.Assign(variable, "", expr()));
                    } else if (take(Kind.COLON) || take(Kind.IS)) {
                        labelSets.add(new Stmt.Assign(variable, ident(), null));
                    } else {
                        expect(Kind.DOT, "SET 要写成 变量.属性 = 值、变量 = {...} 或 变量:标签");
                        String field = ident();
                        expect(Kind.EQ, "SET 缺少等号");
                        assignments.add(new Stmt.Assign(variable, field, expr()));
                    }
                } while (take(Kind.COMMA));
            }
            if (take(Kind.REMOVE)) {
                any = true;
                do {
                    String variable = ident();
                    if (take(Kind.COLON) || take(Kind.IS)) {
                        labelRemovals.add(new Stmt.Assign(variable, ident(), null));
                    } else {
                        expect(Kind.DOT, "REMOVE 要写成 变量.属性 或 变量:标签");
                        removals.add(variable + "." + ident());
                    }
                } while (take(Kind.COMMA));
            }
            if (peek() == Kind.DETACH || peek() == Kind.NODETACH || peek() == Kind.DELETE) {
                any = true;
                if (take(Kind.DETACH)) {
                    detach = true;
                    expect(Kind.DELETE, "DETACH 后面要写 DELETE");
                } else if (take(Kind.NODETACH)) {
                    expect(Kind.DELETE, "NODETACH 后面要写 DELETE");
                } else {
                    next();
                }
                deletes.addAll(variables());
            }
        }
        Stmt.Tail tail = optionalTail();
        mutationEnd();
        if (tail == null && replacements.isEmpty() && removals.isEmpty() && labelSets.isEmpty() && labelRemovals.isEmpty()) {
            Stmt legacy = lowerMutation(clauses, assignments, deletes, detach);
            if (legacy != null) {
                return legacy;
            }
        }
        return new Stmt.Mutate(List.copyOf(clauses), List.copyOf(assignments), List.copyOf(replacements), List.copyOf(removals),
                List.copyOf(deletes), detach, labelSets, labelRemovals, tail);
    }

    // ---------------------------------------------------------------- MATCH 与图模式

    private Stmt.Clause.Match matchClause(boolean optional) {
        String matchMode = null;
        if (take(Kind.REPEATABLE)) {
            if (take(Kind.ELEMENT)) {
                take(Kind.BINDINGS);
            } else {
                expect(Kind.ELEMENTS, "REPEATABLE 后面要写 ELEMENTS");
            }
            matchMode = "REPEATABLE_ELEMENTS";
        } else if (take(Kind.DIFFERENT)) {
            if (take(Kind.EDGE)) {
                take(Kind.BINDINGS);
            } else {
                expect(Kind.EDGES, "DIFFERENT 后面要写 EDGES");
            }
            matchMode = "DIFFERENT_EDGES";
        }
        List<Stmt.Pattern> patterns = new ArrayList<>();
        patterns.add(pathPattern());
        while (take(Kind.COMMA)) {
            patterns.add(pathPattern());
        }
        if (peek() == Kind.KEEP) {
            return fail("暂不支持 KEEP，请把路径搜索前缀写在每个路径模式前面");
        }
        Expr where = take(Kind.WHERE) ? expr() : null;
        Stmt.Pattern first = patterns.get(0);
        return new Stmt.Clause.Match(optional, first.pathVariable(), first.search(), first.searchCount(), first.mode(),
                List.copyOf(patterns), where, matchMode);
    }

    /** {@code [p =] [ALL | ANY [k] | ANY SHORTEST | ALL SHORTEST | SHORTEST k [GROUPS]] [WALK | TRAIL | SIMPLE | ACYCLIC] [PATH | PATHS] 模式}。 */
    private Stmt.Pattern pathPattern() {
        String pathVariable = null;
        if (isName(peek()) && look(1) == Kind.EQ) {
            pathVariable = name();
            expect(Kind.EQ, "路径变量后面要写等号");
        }
        String search = null;
        Integer count = null;
        if (take(Kind.ALL)) {
            if (take(Kind.SHORTEST)) {
                search = "ALL_SHORTEST";
            }
            // 单独的 ALL 就是全部路径，和不写前缀相同。
        } else if (take(Kind.ANY)) {
            if (take(Kind.SHORTEST)) {
                search = "ANY_SHORTEST";
            } else {
                search = "ANY";
                count = peek() == Kind.INTEGER ? (int) nonNegative("路径数量") : 1;
            }
        } else if (take(Kind.SHORTEST)) {
            search = "SHORTEST";
            count = peek() == Kind.INTEGER ? (int) nonNegative("路径数量") : null;
        }
        String mode = null;
        if (peek() == Kind.WALK || peek() == Kind.TRAIL || peek() == Kind.SIMPLE || peek() == Kind.ACYCLIC) {
            mode = next().text.toUpperCase(Locale.ROOT);
        }
        if (!take(Kind.PATH)) {
            take(Kind.PATHS);
        }
        if ("SHORTEST".equals(search) && (take(Kind.GROUP) || take(Kind.GROUPS))) {
            search = "SHORTEST_GROUPS";
        }
        if ("SHORTEST".equals(search) && count == null) {
            count = 1;
        }
        if ("SHORTEST_GROUPS".equals(search) && count == null) {
            count = 1;
        }
        List<Stmt.Element> elements = patternTerm();
        List<List<Stmt.Element>> union = new ArrayList<>();
        boolean multiset = false;
        while (peek() == Kind.PIPE || peek() == Kind.MULTISET) {
            boolean isMultiset = next().kind == Kind.MULTISET;
            if (!union.isEmpty() && isMultiset != multiset) {
                return fail("同一个路径模式里不能混用 | 和 |+|");
            }
            multiset = isMultiset;
            union.add(patternTerm());
        }
        return new Stmt.Pattern(pathVariable, search, count, mode, List.copyOf(elements), union, multiset);
    }

    /** 点、边、量化路径的序列。量化路径可以和点直接相邻：{@code (s) ((a)-[e]->(b)){1,3} (t)}，相邻的点是同一个点。 */
    private List<Stmt.Element> patternTerm() {
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(patternElement(true));
        while (true) {
            if (edgeStart()) {
                elements.add(new Stmt.Element.Rel(edge()));
                elements.add(patternElement(false));
            } else if (peek() == Kind.LP && look(1) == Kind.LP) {
                elements.add(group());
            } else if (peek() == Kind.LP && elements.get(elements.size() - 1) instanceof Stmt.Element.Group) {
                elements.add(new Stmt.Element.Node(node()));
            } else {
                break;
            }
        }
        return elements;
    }

    private boolean edgeStart() {
        Kind kind = peek();
        return kind == Kind.MINUS || kind == Kind.LARROW || kind == Kind.ARROW || kind == Kind.TILDE
                || (kind == Kind.LT && look(1) == Kind.TILDE);
    }

    private Stmt.Element patternElement(boolean nodeFirst) {
        if (peek() == Kind.LP && look(1) == Kind.LP) {
            return group();
        }
        if (!nodeFirst && peek() != Kind.LP) {
            return fail("路径缺少节点");
        }
        return new Stmt.Element.Node(node());
    }

    private Stmt.Element.Group group() {
        expect(Kind.LP, "量化路径缺少左括号");
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(new Stmt.Element.Node(node()));
        while (edgeStart()) {
            elements.add(new Stmt.Element.Rel(edge()));
            elements.add(new Stmt.Element.Node(node()));
        }
        Expr where = take(Kind.WHERE) ? expr() : null;
        expect(Kind.RP, "量化路径缺少右括号");
        int[] range = quantifier();
        if (range == null) {
            return fail("量化路径后面要写跳数");
        }
        int max = range[1] < 0 ? Integer.MAX_VALUE : range[1];
        return new Stmt.Element.Group(List.copyOf(elements), where, range[0], max);
    }

    private Stmt.Tail tail() {
        if (take(Kind.FINISH)) {
            return new Stmt.Tail(false, false, List.of(), List.of(), List.of(), null, null, null, true);
        }
        expect(Kind.RETURN, "缺少 RETURN");
        if (take(Kind.NO)) {
            expect(Kind.BINDINGS, "RETURN NO 后面要写 BINDINGS");
            return new Stmt.Tail(false, false, List.of(), List.of(), List.of(), null, null, null, true);
        }
        boolean distinct = take(Kind.DISTINCT);
        if (!distinct) {
            take(Kind.ALL);
        }
        boolean star = false;
        List<Stmt.Item> items = List.of();
        if (take(Kind.STAR)) {
            star = true;
        } else {
            items = items();
        }
        List<Expr> groupBy = List.of();
        if (take(Kind.GROUP)) {
            expect(Kind.BY, "GROUP 后面要写 BY");
            if (peek() == Kind.LP && look(1) == Kind.RP) {
                next();
                next();
            } else {
                groupBy = new ArrayList<>();
                groupBy.add(expr());
                while (take(Kind.COMMA)) {
                    groupBy.add(expr());
                }
                groupBy = List.copyOf(groupBy);
            }
        }
        Expr postFilter = take(Kind.FILTER) ? expr() : null;
        List<Stmt.Sort> order = List.of();
        if (take(Kind.ORDER)) {
            expect(Kind.BY, "ORDER 后面要写 BY");
            order = sorts();
        }
        Expr skip = null;
        if (take(Kind.SKIP) || take(Kind.OFFSET)) {
            skip = expr();
        }
        Expr limit = take(Kind.LIMIT) ? expr() : null;
        return new Stmt.Tail(distinct, star, items, groupBy, order, skip, limit, postFilter, false);
    }

    /** 只有一条普通 MATCH、没有路径前缀时降成单跳快速路径 {@link Stmt.Match}。 */
    private Stmt lower(List<Stmt.Clause> clauses, Stmt.Tail tail) {
        if (clauses.size() != 1 || !(clauses.get(0) instanceof Stmt.Clause.Match match)) {
            return null;
        }
        if (match.optional() || match.matchMode() != null || match.patterns().size() != 1) {
            return null;
        }
        Stmt.Pattern pattern = match.patterns().get(0);
        // 带路径变量的 MATCH 要走路径搜索才能绑定路径，不能降成单跳快速路径。
        if (pattern.pathVariable() != null || pattern.search() != null || pattern.mode() != null || !pattern.union().isEmpty()) {
            return null;
        }
        if (tail.star() || tail.finish() || !tail.groupBy().isEmpty()) {
            return null;
        }
        List<Stmt.Element> elements = pattern.elements();
        if (elements.size() != 1 && elements.size() != 3) {
            return null;
        }
        if (!(elements.get(0) instanceof Stmt.Element.Node source)) {
            return null;
        }
        if (source.node().where() != null || complex(source.node().label())) {
            return null;
        }
        Stmt.EdgePat edge = null;
        Stmt.NodePat target = null;
        if (elements.size() == 3) {
            if (!(elements.get(1) instanceof Stmt.Element.Rel rel) || !(elements.get(2) instanceof Stmt.Element.Node node)) {
                return null;
            }
            if (node.node().where() != null || complex(node.node().label())) {
                return null;
            }
            if (rel.edge().maxHop() != null && rel.edge().maxHop() < 0 && rel.edge().variable() != null) {
                return null;
            }
            edge = rel.edge();
            target = node.node();
        }
        return new Stmt.Match(
                source.node(), edge, target, match.where(), tail.items(), tail.orderBy(),
                tail.skip(), tail.limit(), tail.distinct(), null);
    }

    private Stmt lowerMutation(List<Stmt.Clause> clauses, List<Stmt.Assign> assignments, List<String> deletes, boolean detach) {
        if (clauses.size() != 1 || !(clauses.get(0) instanceof Stmt.Clause.Match match)) {
            return null;
        }
        if (match.optional() || match.matchMode() != null || match.patterns().size() != 1) {
            return null;
        }
        Stmt.Pattern pattern = match.patterns().get(0);
        if (pattern.search() != null || pattern.mode() != null || !pattern.union().isEmpty()) {
            return null;
        }
        List<Stmt.Element> elements = pattern.elements();
        if (elements.size() != 1 && elements.size() != 3) {
            return null;
        }
        if (!(elements.get(0) instanceof Stmt.Element.Node source) || source.node().where() != null || complex(source.node().label())) {
            return null;
        }
        Stmt.EdgePat edge = null;
        Stmt.NodePat target = null;
        if (elements.size() == 3) {
            if (!(elements.get(1) instanceof Stmt.Element.Rel rel) || !(elements.get(2) instanceof Stmt.Element.Node node)) {
                return null;
            }
            if (rel.edge().minHop() != null || rel.edge().where() != null || complex(rel.edge().label())
                    || node.node().where() != null || complex(node.node().label())) {
                return null;
            }
            edge = rel.edge();
            target = node.node();
        }
        if (!deletes.isEmpty() && assignments.isEmpty()) {
            return new Stmt.Delete(source.node(), edge, target, match.where(), List.copyOf(deletes), detach);
        }
        if (!assignments.isEmpty() && deletes.isEmpty()) {
            return new Stmt.Set(source.node(), edge, target, match.where(), List.copyOf(assignments));
        }
        return null;
    }

    private static boolean complex(Stmt.Label label) {
        return label != null && !(label instanceof Stmt.Label.One);
    }

    // ---------------------------------------------------------------- DDL

    private Stmt create() {
        expect(Kind.CREATE, "缺少 CREATE");
        if (peek() == Kind.OR) {
            return fail("暂不支持 CREATE OR REPLACE：请先 DROP GRAPH 再建");
        }
        if (peek() == Kind.PROPERTY && look(1) == Kind.GRAPH) {
            next();
        }
        if (take(Kind.GRAPH)) {
            if (take(Kind.TYPE)) {
                return fail("暂不支持单独的图类型对象，请直接写 CREATE GRAPH 名字 { 类型定义 }");
            }
            boolean ifNotExists = ifNotExists();
            String name = graphName();
            ifNotExists |= ifNotExists();
            return createGraph(name, ifNotExists);
        }
        if (take(Kind.ALIAS)) {
            String alias = ident();
            expect(Kind.FOR, "别名后面要写 FOR GRAPH");
            expect(Kind.GRAPH, "别名后面要写 FOR GRAPH");
            return new Stmt.CreateAlias(alias, ident());
        }
        if (peek() == Kind.UNIQUE || peek() == Kind.INDEX) {
            return createIndex(take(Kind.UNIQUE));
        }
        boolean node = nodeOrRel();
        expect(Kind.TYPE, "缺少 TYPE");
        String name = ident();
        boolean ifNotExists = ifNotExists();
        String comment = comment();
        String idType = "INT";
        int idLength = 64;
        boolean idSpecified = false;
        if (node && take(Kind.ID)) {
            idSpecified = true;
            if (take(Kind.STRING)) {
                idType = "STRING";
                idLength = stringIdLength();
            } else if (take(Kind.INT) || take(Kind.INT64) || take(Kind.INTEGER_WORD)) {
                idType = "INT";
                if (take(Kind.LP)) return fail("INT64 主键固定为 64 位，不要写长度");
                idLength = 64;
            } else {
                return fail("ID 后面要写 INT 或 STRING");
            }
        }
        boolean brace = take(Kind.LC);
        if (!brace) {
            expect(Kind.LP, "类型定义缺少左括号");
        }
        List<Stmt.Field> fields = fields(brace ? Kind.RC : Kind.RP);
        expect(brace ? Kind.RC : Kind.RP, "类型定义缺少右括号");
        return new Stmt.CreateType(node, name, comment, idType, idLength, idSpecified, fields, ifNotExists);
    }

    /** 图的选项顺序不限：层岩扩展 PARTS / REPLICAS / ID / COMMENT，和标准的图类型 {@code { ... }}、{@code ANY}、{@code LIKE g}。 */
    private Stmt createGraph(String name, boolean ifNotExists) {
        int parts = 1;
        int replicas = 1;
        String idType = "INT";
        int idLength = 0;
        String comment = null;
        String like = null;
        List<Stmt.CreateType> types = List.of();
        boolean typed = false;
        while (true) {
            if (take(Kind.PARTS)) {
                parts = positiveInt("分片数量");
            } else if (take(Kind.REPLICAS)) {
                replicas = positiveInt("副本数量");
            } else if (take(Kind.ID)) {
                if (take(Kind.STRING)) {
                    idType = "STRING";
                    idLength = stringIdLength();
                } else if (take(Kind.INT) || take(Kind.INT64) || take(Kind.INTEGER_WORD)) {
                    idType = "INT";
                    if (take(Kind.LP)) return fail("INT64 主键固定为 64 位，不要写长度");
                    idLength = 64;
                } else {
                    return fail("ID 后面要写 INT64 或 STRING");
                }
            } else if (peek() == Kind.COMMENT) {
                comment = comment();
            } else if (!typed && (peek() == Kind.LC || peek() == Kind.TYPED || peek() == Kind.COLON)) {
                typed = true;
                if (take(Kind.TYPED) || take(Kind.COLON)) {
                    take(Kind.COLON);
                    if (peek() != Kind.LC) {
                        return fail("暂不支持引用已命名的图类型，请直接写 { 类型定义 }");
                    }
                }
                types = graphTypeSpec();
            } else if (!typed && take(Kind.ANY)) {
                typed = true;
                take(Kind.PROPERTY);
                take(Kind.GRAPH);
            } else if (!typed && take(Kind.LIKE)) {
                typed = true;
                like = graphName();
            } else if (peek() == Kind.AS) {
                return fail("暂不支持 AS COPY OF：复制数据请用导出再导入");
            } else {
                break;
            }
        }
        return new Stmt.CreateGraph(name, parts, comment, ifNotExists, replicas, idType, idLength, types, like);
    }

    /**
     * 标准的嵌套图类型：{@code { (Person :Person {name STRING}), (:Person)-[:knows {since INT}]->(:Person) }}，
     * 也接受 {@code NODE [TYPE] Person {...}} 和 {@code [DIRECTED] EDGE [TYPE] knows {...} CONNECTING (Person -> Person)}。
     * 层岩的类型名就是标签，一个类型只有一个标签。
     */
    private List<Stmt.CreateType> graphTypeSpec() {
        expect(Kind.LC, "图类型缺少左花括号");
        List<Stmt.CreateType> types = new ArrayList<>();
        java.util.Map<String, String> aliases = new java.util.HashMap<>();
        if (!take(Kind.RC)) {
            do {
                types.add(elementType(aliases));
            } while (take(Kind.COMMA));
            expect(Kind.RC, "图类型缺少右花括号");
        }
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Stmt.CreateType type : types) {
            if (!seen.add((type.node() ? "node/" : "rel/") + type.name())) {
                return fail("图类型里重复定义了 " + type.name());
            }
        }
        return List.copyOf(types);
    }

    private Stmt.CreateType elementType(java.util.Map<String, String> aliases) {
        if (take(Kind.NODE)) {
            take(Kind.TYPE);
            String name = isName(peek()) && peek() != Kind.LABEL && peek() != Kind.LABELS ? ident() : null;
            String label = typeLabel();
            String typeName = resolveTypeName(name, label);
            if (name != null) aliases.put(name, typeName);
            return new Stmt.CreateType(true, typeName, null, "INT", 64, false, typePropertiesOrEmpty(), false);
        }
        if (peek() == Kind.DIRECTED || peek() == Kind.EDGE || peek() == Kind.UNDIRECTED) {
            if (take(Kind.UNDIRECTED)) {
                return fail("层岩的边都有方向，不支持 UNDIRECTED EDGE");
            }
            take(Kind.DIRECTED);
            expect(Kind.EDGE, "DIRECTED 后面要写 EDGE");
            take(Kind.TYPE);
            String name = isName(peek()) && peek() != Kind.LABEL && peek() != Kind.LABELS ? ident() : null;
            String label = typeLabel();
            String typeName = resolveTypeName(name, label);
            List<Stmt.Field> fields = typePropertiesOrEmpty();
            expect(Kind.CONNECTING, "边类型要写 CONNECTING (起点 -> 终点)");
            expect(Kind.LP, "CONNECTING 缺少左括号");
            endpointType(aliases);
            if (!take(Kind.ARROW)) {
                if (take(Kind.TO)) {
                    // CONNECTING (A TO B)
                } else {
                    return fail("CONNECTING 里要写 -> 或 TO");
                }
            }
            endpointType(aliases);
            expect(Kind.RP, "CONNECTING 缺少右括号");
            return new Stmt.CreateType(false, typeName, null, "INT", 64, false, fields, false);
        }
        // 模式写法：(name :Label {...}) 或 (:A)-[:R {...}]->(:B)
        expect(Kind.LP, "这里要写点类型 ( ... ) 或边类型 (:A)-[:R]->(:B)");
        String name = isName(peek()) && peek() != Kind.LABEL && peek() != Kind.LABELS ? ident() : null;
        String label = typeLabel();
        List<Stmt.Field> fields = typePropertiesOrEmpty();
        expect(Kind.RP, "点类型缺少右括号");
        if (peek() != Kind.MINUS && peek() != Kind.LARROW && peek() != Kind.TILDE) {
            String typeName = resolveTypeName(name, label);
            if (name != null) aliases.put(name, typeName);
            return new Stmt.CreateType(true, typeName, null, "INT", 64, false, fields, false);
        }
        String sourceType = name != null && label == null ? aliases.get(name) : label;
        if (sourceType == null) {
            return fail("边类型的起点要写已定义的点类型");
        }
        if (peek() == Kind.TILDE) {
            return fail("层岩的边都有方向，不支持无向边类型");
        }
        boolean reverse = take(Kind.LARROW);
        if (!reverse) {
            expect(Kind.MINUS, "边类型缺少 -[");
        }
        expect(Kind.LB, "边类型缺少左方括号");
        String edgeName = isName(peek()) && peek() != Kind.LABEL && peek() != Kind.LABELS ? ident() : null;
        String edgeLabel = typeLabel();
        List<Stmt.Field> edgeFields = typePropertiesOrEmpty();
        expect(Kind.RB, "边类型缺少右方括号");
        if (reverse) {
            expect(Kind.MINUS, "入边类型的箭头不完整");
        } else {
            expect(Kind.ARROW, "边类型要写 ]->");
        }
        expect(Kind.LP, "边类型的终点缺少左括号");
        String targetName = isName(peek()) && peek() != Kind.LABEL && peek() != Kind.LABELS ? ident() : null;
        String targetLabel = typeLabel();
        expect(Kind.RP, "边类型的终点缺少右括号");
        String targetType = targetName != null && targetLabel == null ? aliases.get(targetName) : targetLabel;
        if (targetType == null) {
            return fail("边类型的终点要写已定义的点类型");
        }
        return new Stmt.CreateType(false, resolveTypeName(edgeName, edgeLabel), null, "INT", 64, false, edgeFields, false);
    }

    private void endpointType(java.util.Map<String, String> aliases) {
        String name = ident();
        if (!aliases.containsKey(name) && !aliases.containsValue(name)) {
            fail("边类型引用了没有定义的点类型 " + name);
        }
    }

    /** {@code :L}、{@code IS L}、{@code LABEL L}；{@code LABELS A & B} 和多个标签不支持。 */
    private String typeLabel() {
        if (take(Kind.COLON) || take(Kind.IS) || take(Kind.LABEL)) {
            String label = ident();
            if (peek() == Kind.AMP) {
                return fail("层岩一个类型只有一个标签，不支持多标签类型");
            }
            return label;
        }
        if (take(Kind.LABELS)) {
            String label = ident();
            if (peek() == Kind.AMP) {
                return fail("层岩一个类型只有一个标签，不支持多标签类型");
            }
            return label;
        }
        return null;
    }

    private String resolveTypeName(String name, String label) {
        if (label == null && name == null) {
            return fail("类型要写名字或标签");
        }
        if (label == null) {
            return name;
        }
        return label;
    }

    private List<Stmt.Field> typePropertiesOrEmpty() {
        if (!take(Kind.LC)) {
            return List.of();
        }
        List<Stmt.Field> fields = fields(Kind.RC);
        expect(Kind.RC, "属性类型缺少右花括号");
        return fields;
    }

    private int stringIdLength() {
        int value = idLength();
        if (value > 64) return fail("STRING ID 长度不能超过 64");
        return value;
    }

    private int idLength() {
        expect(Kind.LP, "STRING ID 后面要写长度，例如 STRING(64)");
        int value = positiveInt("ID 长度");
        expect(Kind.RP, "ID 长度缺少右括号");
        return value;
    }

    private int positiveInt(String label) {
        long value = integer();
        if (value <= 0 || value > 4096) {
            return fail(label + "必须在 1 到 4096 之间");
        }
        return (int) value;
    }

    private long nonNegative(String label) {
        long value = integer();
        if (value < 0 || value > Integer.MAX_VALUE) {
            return fail(label + "超出范围");
        }
        return value;
    }

    private Stmt createIndex(boolean unique) {
        expect(Kind.INDEX, "缺少 INDEX");
        boolean ifNotExists = ifNotExists();
        String name = ident();
        expect(Kind.ON, "索引后面要写 ON");
        boolean node = nodeOrRel();
        expect(Kind.TYPE, "缺少 TYPE");
        String typeName = ident();
        expect(Kind.LP, "索引列缺少左括号");
        if (peek() == Kind.RP) {
            fail("索引至少要有一列");
        }
        List<String> properties = new ArrayList<>();
        properties.add(ident());
        while (take(Kind.COMMA)) {
            properties.add(ident());
        }
        expect(Kind.RP, "索引列缺少右括号");
        return new Stmt.CreateIndex(unique, name, node, typeName, properties, ifNotExists);
    }

    private Stmt drop() {
        expect(Kind.DROP, "缺少 DROP");
        if (take(Kind.ALIAS)) {
            return new Stmt.DropAlias(ident(), ifExists());
        }
        if (take(Kind.INDEX)) {
            return new Stmt.DropIndex(ident(), ifExists());
        }
        if (peek() == Kind.NODE || peek() == Kind.REL) {
            boolean node = nodeOrRel();
            expect(Kind.TYPE, "缺少 TYPE");
            return new Stmt.DropType(node, ident(), ifExists());
        }
        if (peek() == Kind.PROPERTY && look(1) == Kind.GRAPH) {
            next();
        }
        expect(Kind.GRAPH, "只能 DROP GRAPH、DROP ALIAS、DROP INDEX 或 DROP TYPE");
        if (peek() == Kind.TYPE) {
            return fail("暂不支持单独的图类型对象");
        }
        boolean ifExists = ifExists();
        String name = graphName();
        return new Stmt.DropGraph(name, ifExists || ifExists());
    }

    private Stmt show() {
        expect(Kind.SHOW, "缺少 SHOW");
        if (take(Kind.GRAPHS)) {
            return new Stmt.Show(Stmt.Show.Kind.GRAPHS);
        }
        if (take(Kind.ALIASES)) {
            return new Stmt.Show(Stmt.Show.Kind.ALIASES);
        }
        if (take(Kind.INDEXES)) {
            return new Stmt.Show(Stmt.Show.Kind.INDEXES);
        }
        if (take(Kind.NODE)) {
            expect(Kind.TYPES, "SHOW NODE 后面要写 TYPES");
            return new Stmt.Show(Stmt.Show.Kind.NODE_TYPES);
        }
        expect(Kind.REL, "无法识别的 SHOW");
        expect(Kind.TYPES, "SHOW REL 后面要写 TYPES");
        return new Stmt.Show(Stmt.Show.Kind.REL_TYPES);
    }

    private Stmt describe() {
        expect(Kind.DESCRIBE, "缺少 DESCRIBE");
        boolean node = nodeOrRel();
        expect(Kind.TYPE, "缺少 TYPE");
        return new Stmt.Describe(node, ident());
    }

    private Stmt alter() {
        expect(Kind.ALTER, "缺少 ALTER");
        boolean node = nodeOrRel();
        expect(Kind.TYPE, "缺少 TYPE");
        String typeName = ident();
        if (take(Kind.DROP)) {
            return new Stmt.AlterType(node, typeName, "DROP", ident(), null, null, null);
        }
        if (take(Kind.RENAME)) {
            String property = ident();
            expect(Kind.TO, "RENAME 后面要写 TO");
            return new Stmt.AlterType(node, typeName, "RENAME", property, null, null, ident());
        }
        expect(Kind.ADD, "只能增加、删除或重命名列");
        String property = ident();
        take(Kind.TYPED);
        if (peek() == Kind.COLON && look(1) == Kind.COLON) {
            next();
            next();
        }
        String valueType = typeName();
        if (peek() == Kind.NOT && look(1) == Kind.NULL) {
            return fail("已有数据没有这一列的值，只能增加可空列");
        }
        take(Kind.NULL);
        return new Stmt.AlterType(node, typeName, property, valueType, comment());
    }

    // ---------------------------------------------------------------- CALL

    /** {@code CALL 过程(参数) [YIELD 列 [AS 别名], ...]} 或内联 {@code CALL [(变量, ...)] { 查询 }}。 */
    private Stmt.Clause callClause(boolean optional) {
        expect(Kind.CALL, "缺少 CALL");
        if (peek() == Kind.LC || peek() == Kind.LP) {
            List<String> scope = null;
            if (take(Kind.LP)) {
                scope = new ArrayList<>();
                if (!take(Kind.RP)) {
                    scope.add(ident());
                    while (take(Kind.COMMA)) {
                        scope.add(ident());
                    }
                    expect(Kind.RP, "变量列表缺少右括号");
                }
                scope = List.copyOf(scope);
            }
            expect(Kind.LC, "内联过程缺少左花括号");
            Stmt body = readOnly(block(), "内联 CALL");
            expect(Kind.RC, "内联过程缺少右花括号");
            return new Stmt.Clause.Inline(body, scope, optional);
        }
        return new Stmt.Clause.Call(call(optional));
    }

    private Stmt.Call call(boolean optional) {
        String procedure = name();
        while (take(Kind.DOT)) {
            procedure = procedure + "." + name();
        }
        expect(Kind.LP, "过程缺少左括号");
        List<Stmt.Arg> args = new ArrayList<>();
        if (peek() == Kind.LC && (look(1) == Kind.RC || look(2) == Kind.COLON)) {
            next();
            if (!take(Kind.RC)) {
                args.add(arg(args));
                while (take(Kind.COMMA)) {
                    args.add(arg(args));
                }
                expect(Kind.RC, "过程参数缺少右花括号");
            }
            expect(Kind.RP, "过程缺少右括号");
        } else if (!take(Kind.RP)) {
            args.add(new Stmt.Arg("", expr()));
            while (take(Kind.COMMA)) {
                args.add(new Stmt.Arg("", expr()));
            }
            expect(Kind.RP, "过程缺少右括号");
        }
        List<String> columns = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        if (take(Kind.YIELD)) {
            do {
                String column = name();
                columns.add(column);
                aliases.add(take(Kind.AS) ? ident() : column);
            } while (take(Kind.COMMA));
        }
        return new Stmt.Call(procedure, List.copyOf(args), List.copyOf(columns), List.copyOf(aliases), optional);
    }

    private Stmt.Arg arg(List<Stmt.Arg> args) {
        String key = name();
        for (Stmt.Arg existing : args) {
            if (existing.name().equals(key)) {
                return fail("参数 " + key + " 写了两次");
            }
        }
        expect(Kind.COLON, "参数名后面要写冒号");
        return new Stmt.Arg(key, expr());
    }

    private List<String> variables() {
        List<String> names = new ArrayList<>();
        names.add(ident());
        while (take(Kind.COMMA)) {
            names.add(ident());
        }
        return List.copyOf(names);
    }

    private boolean nodeOrRel() {
        if (take(Kind.NODE)) {
            return true;
        }
        if (take(Kind.EDGE)) {
            return false;
        }
        expect(Kind.REL, "这里要写 NODE 或 REL");
        return false;
    }

    private List<Stmt.Field> fields(Kind close) {
        if (peek() == close) {
            return List.of();
        }
        List<Stmt.Field> fields = new ArrayList<>();
        java.util.Set<String> names = new java.util.HashSet<>();
        do {
            Stmt.Field field = field();
            if (!names.add(field.name())) {
                return fail("属性 " + field.name() + " 写了两次");
            }
            fields.add(field);
        } while (take(Kind.COMMA));
        return List.copyOf(fields);
    }

    /** {@code 名字 [::|TYPED] 类型 [NOT NULL | NULL] [COMMENT '...']}。不写时可空，和标准一致。 */
    private Stmt.Field field() {
        String name = ident();
        take(Kind.TYPED);
        if (peek() == Kind.COLON && look(1) == Kind.COLON) {
            next();
            next();
        }
        String valueType = typeName();
        boolean nullable = true;
        if (take(Kind.NOT)) {
            expect(Kind.NULL, "NOT 后面要写 NULL");
            nullable = false;
        } else {
            take(Kind.NULL);
        }
        return new Stmt.Field(name, valueType, nullable, comment());
    }

    private String comment() {
        if (!take(Kind.COMMENT)) return null;
        if (peek() != Kind.TEXT) return fail("COMMENT 后面要写字符串");
        return next().text;
    }

    /**
     * 属性和 CAST 的类型名，统一成存储能识别的名字：BOOL、INT64、DOUBLE、STRING、DATETIME。
     * 标准里其他写法按含义对应；存储还没有的类型直接报错，不悄悄换成别的类型。
     */
    private String typeName() {
        Tok token = at();
        String word = token.text.toUpperCase(Locale.ROOT);
        if (!(token.kind == Kind.IDENT || KEYWORDS.containsKey(token.text.toLowerCase(Locale.ROOT)))) {
            return fail("无法识别的列类型");
        }
        next();
        switch (word) {
            case "BOOL", "BOOLEAN" -> {
                return token.text;
            }
            case "INT", "INTEGER", "INT64", "INTEGER64", "BIGINT" -> {
                return word.equals("INT") || word.equals("INT64") || word.equals("INTEGER") ? token.text : "INT64";
            }
            case "SIGNED" -> {
                String inner = at().text.toUpperCase(Locale.ROOT);
                if (inner.equals("INTEGER64") || inner.equals("BIGINT") || inner.equals("INTEGER") || inner.equals("INT64")) {
                    next();
                    return "INT64";
                }
                return fail("只支持 64 位整数");
            }
            case "INT8", "INT16", "INT32", "INT128", "INT256", "INTEGER8", "INTEGER16", "INTEGER32", "INTEGER128", "INTEGER256",
                 "SMALLINT", "UINT", "UINT8", "UINT16", "UINT32", "UINT64", "UINT128", "UINT256", "USMALLINT", "UBIGINT",
                 "UNSIGNED" -> {
                return fail("暂不支持 " + token.text + "：整数只有 64 位有符号一种，请写 INT64");
            }
            case "DOUBLE" -> {
                if (at().text.equalsIgnoreCase("PRECISION")) {
                    next();
                }
                return "DOUBLE";
            }
            case "FLOAT", "FLOAT64" -> {
                if (take(Kind.LP)) {
                    return fail("暂不支持指定 FLOAT 精度，请写 DOUBLE");
                }
                return "DOUBLE";
            }
            case "REAL", "FLOAT16", "FLOAT32", "FLOAT128", "FLOAT256", "DECIMAL", "DEC" -> {
                return fail("暂不支持 " + token.text + "：小数只有 64 位浮点一种，请写 DOUBLE");
            }
            case "STRING", "VARCHAR", "CHAR" -> {
                if (peek() == Kind.LP) {
                    return fail("暂不支持字符串长度约束，请直接写 STRING");
                }
                return word.equals("STRING") ? token.text : "STRING";
            }
            case "DATETIME" -> {
                return token.text;
            }
            case "ZONED" -> {
                if (!at().text.equalsIgnoreCase("DATETIME")) {
                    return fail("暂不支持 ZONED TIME，日期时间请写 ZONED DATETIME");
                }
                next();
                return "DATETIME";
            }
            case "TIMESTAMP" -> {
                if (take(Kind.WITH)) {
                    expectWord("TIME");
                    expectWord("ZONE");
                }
                return "DATETIME";
            }
            case "DATE", "TIME", "LOCAL", "DURATION", "BYTES", "BINARY", "VARBINARY", "LIST", "ARRAY", "RECORD", "PATH",
                 "NODE", "EDGE", "VERTEX", "RELATIONSHIP", "ANY", "NOTHING" -> {
                return fail("存储暂不支持 " + token.text + " 类型的属性，现有类型：BOOL、INT64、DOUBLE、STRING、DATETIME");
            }
            default -> {
                return fail("无法识别的列类型");
            }
        }
    }

    private void expectWord(String word) {
        if (!at().text.equalsIgnoreCase(word)) {
            fail("这里要写 " + word);
        }
        next();
    }

    // ---------------------------------------------------------------- 节点、边和标签

    private Stmt.NodePat node() {
        expect(Kind.LP, "节点缺少左括号");
        String variable = null;
        if (isName(peek()) && peek() != Kind.WHERE) {
            variable = next().text;
        }
        String typeName = null;
        Stmt.Label label = null;
        if (take(Kind.COLON) || take(Kind.IS)) {
            label = label(true);
            if (label instanceof Stmt.Label.One one) {
                typeName = one.name();
            }
        }
        List<Stmt.Prop> props = props();
        Expr where = take(Kind.WHERE) ? expr() : null;
        expect(Kind.RP, "节点缺少右括号");
        return new Stmt.NodePat(variable, typeName, props, where, label);
    }

    /**
     * 标签表达式，优先级 {@code !} 高于 {@code &} 高于 {@code |}，可以加括号，{@code %} 表示任意标签。
     * {@code keywordAnd} 为真时（点和边模式里）也接受关键字 AND 作为 {@code &}，这是层岩旧写法；
     * 表达式里的 {@code n:A AND ...} 中的 AND 是逻辑与，不能吞掉。
     */
    private Stmt.Label label(boolean keywordAnd) {
        List<Stmt.Label> options = new ArrayList<>();
        options.add(labelConjunction(keywordAnd));
        while (peek() == Kind.PIPE) {
            next();
            take(Kind.COLON);
            options.add(labelConjunction(keywordAnd));
        }
        return options.size() == 1 ? options.get(0) : new Stmt.Label.Or(List.copyOf(options));
    }

    private Stmt.Label labelConjunction(boolean keywordAnd) {
        List<Stmt.Label> options = new ArrayList<>();
        options.add(labelTerm(keywordAnd));
        while (peek() == Kind.AMP || (keywordAnd && peek() == Kind.AND)) {
            next();
            options.add(labelTerm(keywordAnd));
        }
        return options.size() == 1 ? options.get(0) : new Stmt.Label.And(List.copyOf(options));
    }

    private Stmt.Label labelTerm(boolean keywordAnd) {
        if (take(Kind.PERCENT)) {
            return new Stmt.Label.Any();
        }
        if (take(Kind.BANG) || take(Kind.NOT)) {
            return new Stmt.Label.Not(labelTerm(keywordAnd));
        }
        if (take(Kind.LP)) {
            Stmt.Label inner = label(keywordAnd);
            expect(Kind.RP, "标签表达式缺少右括号");
            return inner;
        }
        return new Stmt.Label.One(ident());
    }

    /** 只由名字和 {@code |} 组成时列出这些类型；否则为空，执行时读全部类型再过滤。 */
    private List<String> typeNames(Stmt.Label label) {
        if (label == null) {
            return List.of();
        }
        if (label instanceof Stmt.Label.One one) {
            return List.of(one.name());
        }
        if (label instanceof Stmt.Label.Or or) {
            List<String> names = new ArrayList<>();
            for (Stmt.Label option : or.options()) {
                if (!(option instanceof Stmt.Label.One one)) {
                    return List.of();
                }
                if (names.contains(one.name())) {
                    return fail("边类型重复：" + one.name());
                }
                names.add(one.name());
            }
            return List.copyOf(names);
        }
        return List.of();
    }

    private List<Stmt.Element> insertElements() {
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(new Stmt.Element.Node(node()));
        while (peek() == Kind.COMMA || edgeStart()) {
            if (take(Kind.COMMA)) {
                elements.add(new Stmt.Element.Node(node()));
                continue;
            }
            Stmt.EdgePat edge = edge();
            if (edge.minHop() != null) {
                return fail("插入边不能是多跳");
            }
            if (edge.direction() == Stmt.EdgePat.Direction.ANY) {
                return fail("插入的边要写方向");
            }
            if (edge.label() != null && !(edge.label() instanceof Stmt.Label.One)) {
                return fail("插入的边只能有一个类型");
            }
            if (edge.where() != null) {
                return fail("插入的边不能写 WHERE");
            }
            elements.add(new Stmt.Element.Rel(edge));
            elements.add(new Stmt.Element.Node(node()));
        }
        for (Stmt.Element element : elements) {
            if (element instanceof Stmt.Element.Node node) {
                if (node.node().label() != null && !(node.node().label() instanceof Stmt.Label.One)) {
                    return fail("层岩一个点只有一个类型，插入时只能写一个标签");
                }
                if (node.node().where() != null) {
                    return fail("插入的点不能写 WHERE");
                }
            }
        }
        return List.copyOf(elements);
    }

    /**
     * 边模式，完整写法和缩写都接受：
     * {@code -[ ]->}、{@code <-[ ]-}、{@code -[ ]-}、{@code <-[ ]->}、{@code <~[ ]~}、{@code ~[ ]~>}，
     * 缩写 {@code ->}、{@code <-}、{@code -}、{@code <->}、{@code <~}、{@code ~>}。
     * 层岩只有有向边：「左或无向」等于左，「无向或右」等于右，单纯的无向 {@code ~[ ]~} 报错。
     */
    private Stmt.EdgePat edge() {
        Stmt.EdgePat.Direction direction;
        Stmt.EdgePat body;
        if (take(Kind.LARROW)) {
            if (take(Kind.LB)) {
                body = edgeBody();
                expect(Kind.RB, "边缺少右方括号");
                if (take(Kind.ARROW)) {
                    direction = Stmt.EdgePat.Direction.ANY;
                } else {
                    expect(Kind.MINUS, "入边的箭头不完整");
                    direction = Stmt.EdgePat.Direction.IN;
                }
            } else {
                body = emptyEdge();
                direction = take(Kind.GT) ? Stmt.EdgePat.Direction.ANY : Stmt.EdgePat.Direction.IN;
            }
        } else if (peek() == Kind.LT && look(1) == Kind.TILDE) {
            next();
            next();
            if (take(Kind.LB)) {
                body = edgeBody();
                expect(Kind.RB, "边缺少右方括号");
                expect(Kind.TILDE, "<~[ 要以 ]~ 结束");
            } else {
                body = emptyEdge();
            }
            direction = Stmt.EdgePat.Direction.IN;
        } else if (take(Kind.TILDE)) {
            if (take(Kind.LB)) {
                body = edgeBody();
                expect(Kind.RB, "边缺少右方括号");
                expect(Kind.TILDE, "~[ 要以 ]~ 或 ]~> 结束");
            } else {
                body = emptyEdge();
            }
            if (!take(Kind.GT)) {
                return fail("层岩的边都有方向，不支持无向边 ~");
            }
            direction = Stmt.EdgePat.Direction.OUT;
        } else if (take(Kind.ARROW)) {
            body = emptyEdge();
            direction = Stmt.EdgePat.Direction.OUT;
        } else {
            expect(Kind.MINUS, "边缺少方向");
            if (take(Kind.LB)) {
                body = edgeBody();
                expect(Kind.RB, "边缺少右方括号");
                if (take(Kind.ARROW)) {
                    direction = Stmt.EdgePat.Direction.OUT;
                } else if (take(Kind.MINUS)) {
                    direction = Stmt.EdgePat.Direction.ANY;
                } else {
                    return fail("边的方向不完整");
                }
            } else {
                body = emptyEdge();
                direction = Stmt.EdgePat.Direction.ANY;
            }
        }
        int[] range = quantifier();
        return new Stmt.EdgePat(direction, body.variable(), body.typeNames(), body.rank(), body.properties(),
                range == null ? null : range[0], range == null ? null : range[1], body.label(), body.where());
    }

    private static Stmt.EdgePat emptyEdge() {
        return new Stmt.EdgePat(Stmt.EdgePat.Direction.OUT, null, List.of(), null, List.of(), null, null, null, null);
    }

    /** 量词：{@code *}、{@code +}、{@code ?}、{@code {n}}、{@code {m,}}、{@code {,n}}、{@code {m,n}}。上界 -1 表示不限。 */
    private int[] quantifier() {
        if (take(Kind.STAR)) {
            return new int[] {0, -1};
        }
        if (take(Kind.PLUS)) {
            return new int[] {1, -1};
        }
        if (take(Kind.QUESTION)) {
            return new int[] {0, 1};
        }
        if (peek() != Kind.LC || (look(1) != Kind.INTEGER && look(1) != Kind.COMMA)) {
            return null;
        }
        next();
        long min = peek() == Kind.COMMA ? 0 : integer();
        long max = min;
        if (take(Kind.COMMA)) {
            max = peek() == Kind.RC ? -1 : integer();
        }
        expect(Kind.RC, "跳数缺少右花括号");
        if (min > Integer.MAX_VALUE || min < 0 || (max > Integer.MAX_VALUE)) {
            return fail("跳数超出范围");
        }
        if (max >= 0 && max < min) {
            return fail("跳数范围不正确");
        }
        return new int[] {(int) min, (int) max};
    }

    private Stmt.EdgePat edgeBody() {
        String variable = null;
        if (isName(peek()) && peek() != Kind.WHERE) {
            variable = next().text;
        }
        Stmt.Label label = null;
        if (take(Kind.COLON) || take(Kind.IS)) {
            label = label(true);
        }
        Long rank = null;
        if (take(Kind.AT)) {
            rank = integer();
        }
        List<Stmt.Prop> props = props();
        Expr where = take(Kind.WHERE) ? expr() : null;
        return new Stmt.EdgePat(Stmt.EdgePat.Direction.OUT, variable, typeNames(label), rank, props, null, null, label, where);
    }

    private List<Stmt.Prop> props() {
        if (!take(Kind.LC)) {
            return List.of();
        }
        if (take(Kind.RC)) {
            return List.of();
        }
        List<Stmt.Prop> props = new ArrayList<>();
        props.add(prop());
        while (take(Kind.COMMA)) {
            props.add(prop());
        }
        expect(Kind.RC, "属性缺少右花括号");
        return List.copyOf(props);
    }

    private Stmt.Prop prop() {
        String name = ident();
        expect(Kind.COLON, "属性名后面要写冒号");
        return new Stmt.Prop(name, expr());
    }

    private List<Stmt.Item> items() {
        List<Stmt.Item> items = new ArrayList<>();
        items.add(item());
        while (take(Kind.COMMA)) {
            items.add(item());
        }
        return List.copyOf(items);
    }

    private Stmt.Item item() {
        int start = at().start;
        Expr expr = expr();
        String text = source.substring(start, previousEnd());
        String alias = null;
        if (take(Kind.AS)) {
            alias = ident();
        }
        return new Stmt.Item(expr, alias, text);
    }

    private List<Stmt.Sort> sorts() {
        List<Stmt.Sort> sorts = new ArrayList<>();
        sorts.add(sort());
        while (take(Kind.COMMA)) {
            sorts.add(sort());
        }
        return List.copyOf(sorts);
    }

    private Stmt.Sort sort() {
        Expr expr = expr();
        boolean descending = false;
        if (take(Kind.DESC)) {
            descending = true;
        } else {
            take(Kind.ASC);
        }
        if (at().kind == Kind.IDENT && at().text.equalsIgnoreCase("NULLS")) {
            fail("暂不支持 NULLS FIRST / NULLS LAST");
        }
        return new Stmt.Sort(expr, descending);
    }

    // ---------------------------------------------------------------- 表达式

    private Expr expr() {
        return or();
    }

    /** 标准里 OR 和 XOR 同一优先级，左结合。 */
    private Expr or() {
        Expr left = and();
        while (peek() == Kind.OR || peek() == Kind.XOR) {
            String op = next().kind == Kind.OR ? "OR" : "XOR";
            left = new Expr.Binary(op, left, and());
        }
        return left;
    }

    private Expr and() {
        Expr left = not();
        while (take(Kind.AND)) {
            left = new Expr.Binary("AND", left, not());
        }
        return left;
    }

    private Expr not() {
        if (take(Kind.NOT)) {
            return new Expr.Unary("NOT", not());
        }
        return predicate();
    }

    private Expr predicate() {
        Expr left = add();
        Kind kind = peek();
        if (kind == Kind.EQ || kind == Kind.NEQ || kind == Kind.LT || kind == Kind.LE
                || kind == Kind.GT || kind == Kind.GE) {
            String op = next().text;
            return new Expr.Binary(op, left, add());
        }
        if (kind == Kind.LARROW) {
            // a<-1 是 a < -1，词法上和左箭头连在一起。
            next();
            return new Expr.Binary("<", left, addFrom(new Expr.Unary("-", unary())));
        }
        if (kind == Kind.NOT && look(1) == Kind.IN && letDepth == 0) {
            next();
            return in(left, true);
        }
        if (peek() == Kind.IN && letDepth == 0) {
            return in(left, false);
        }
        if (kind == Kind.COLON && sliceDepth == 0 && left instanceof Expr.Variable) {
            next();
            return new Expr.Labeled(left, label(false), false);
        }
        if (take(Kind.IS)) {
            boolean negated = take(Kind.NOT);
            if (take(Kind.NULL)) {
                return new Expr.IsNull(left, negated);
            }
            if (take(Kind.TRUE)) {
                return new Expr.Truth(left, true, negated);
            }
            if (take(Kind.FALSE)) {
                return new Expr.Truth(left, false, negated);
            }
            if (take(Kind.UNKNOWN)) {
                return new Expr.Truth(left, null, negated);
            }
            if (peek() == Kind.COLON && look(1) == Kind.COLON) {
                next();
                next();
                return new Expr.Typed(left, typeName(), negated);
            }
            if (take(Kind.TYPED)) {
                return new Expr.Typed(left, typeName(), negated);
            }
            if (take(Kind.LABELED) || take(Kind.COLON)) {
                return new Expr.Labeled(left, label(false), negated);
            }
            if (take(Kind.DIRECTED)) {
                return new Expr.Directed(left, negated);
            }
            if (take(Kind.SOURCE)) {
                expect(Kind.OF, "SOURCE 后面要写 OF");
                return new Expr.EndpointOf(left, add(), true, negated);
            }
            if (take(Kind.DESTINATION)) {
                expect(Kind.OF, "DESTINATION 后面要写 OF");
                return new Expr.EndpointOf(left, add(), false, negated);
            }
            return fail("IS 后面无法识别");
        }
        return left;
    }

    private Expr in(Expr left, boolean negated) {
        expect(Kind.IN, "缺少 IN");
        if (peek() == Kind.LP) {
            next();
            List<Expr> options = new ArrayList<>();
            options.add(expr());
            while (take(Kind.COMMA)) {
                options.add(expr());
            }
            expect(Kind.RP, "IN 缺少右括号");
            return new Expr.In(left, List.copyOf(options), negated);
        }
        if (peek() == Kind.LB) {
            next();
            List<Expr> options = new ArrayList<>();
            if (!take(Kind.RB)) {
                options.add(expr());
                while (take(Kind.COMMA)) {
                    options.add(expr());
                }
                expect(Kind.RB, "IN 缺少右方括号");
            }
            return new Expr.In(left, List.copyOf(options), negated);
        }
        // 右边和比较运算的右边同一优先级：x IN $list AND y 里的 AND 不属于列表。
        return new Expr.Member(left, add(), negated);
    }

    private Expr add() {
        return addFrom(unary());
    }

    /** 已经读出第一个一元项时，接着读乘除和加减。 */
    private Expr addFrom(Expr first) {
        Expr left = mulFrom(first);
        while (peek() == Kind.PLUS || peek() == Kind.MINUS || peek() == Kind.CONCAT) {
            String op = next().text;
            left = new Expr.Binary(op, left, mul());
        }
        return left;
    }

    private Expr mul() {
        return mulFrom(unary());
    }

    private Expr mulFrom(Expr first) {
        Expr left = first;
        while (peek() == Kind.STAR || peek() == Kind.SLASH) {
            String op = next().text;
            left = new Expr.Binary(op, left, unary());
        }
        return left;
    }

    private Expr unary() {
        if (take(Kind.MINUS)) {
            return new Expr.Unary("-", unary());
        }
        if (take(Kind.PLUS)) {
            return unary();
        }
        return primary();
    }

    private Expr primary() {
        Expr expr;
        if (take(Kind.LP)) {
            int saved = letDepth;
            letDepth = 0;
            Expr inner;
            try {
                inner = expr();
            } finally {
                letDepth = saved;
            }
            expect(Kind.RP, "表达式缺少右括号");
            expr = inner;
        } else if (take(Kind.DOLLAR)) {
            expr = new Expr.Param(ident());
        } else if (peek() == Kind.DATETIME && look(1) == Kind.TEXT) {
            next();
            expr = new Expr.Literal(new DateTimeText(next().text));
        } else if (peek() == Kind.IDENT && look(1) == Kind.TEXT && at().text.equalsIgnoreCase("TIMESTAMP")) {
            next();
            expr = new Expr.Literal(new DateTimeText(next().text));
        } else if (peek() == Kind.IDENT && at().text.equalsIgnoreCase("ZONED") && look(1) == Kind.DATETIME && look(2) == Kind.TEXT) {
            next();
            next();
            expr = new Expr.Literal(new DateTimeText(next().text));
        } else if (peek() == Kind.IDENT && look(1) == Kind.TEXT
                && java.util.Set.of("DATE", "TIME", "DURATION", "LOCAL").contains(at().text.toUpperCase(Locale.ROOT))) {
            return fail("暂不支持 " + at().text.toUpperCase(Locale.ROOT) + " 类型的值，日期时间请写 DATETIME '...'");
        } else if (peek() == Kind.INTEGER) {
            expr = new Expr.Literal(integer());
        } else if (peek() == Kind.REAL) {
            expr = new Expr.Literal(Double.valueOf(next().text));
        } else if (peek() == Kind.TEXT) {
            expr = new Expr.Literal(next().text);
        } else if (take(Kind.TRUE)) {
            expr = new Expr.Literal(true);
        } else if (take(Kind.FALSE)) {
            expr = new Expr.Literal(false);
        } else if (take(Kind.NULL)) {
            expr = new Expr.Literal(null);
        } else if (take(Kind.UNKNOWN)) {
            expr = new Expr.Literal(null);
        } else if (peek() == Kind.CASE && look(1) != Kind.LP) {
            next();
            expr = caseExpr();
        } else if (peek() == Kind.CAST && look(1) == Kind.LP) {
            next();
            expect(Kind.LP, "CAST 缺少左括号");
            Expr value = expr();
            expect(Kind.AS, "CAST 缺少 AS");
            String type = typeName();
            expect(Kind.RP, "CAST 缺少右括号");
            expr = new Expr.Cast(value, type);
        } else if (peek() == Kind.IDENT && "REDUCE".equalsIgnoreCase(at().text) && look(1) == Kind.LP) {
            next();
            expr = reduceExpr();
        } else if (take(Kind.EXISTS)) {
            if (take(Kind.LC)) {
                Stmt query = nested(false);
                expect(Kind.RC, "EXISTS 缺少右花括号");
                expr = new Expr.Exists(query);
            } else if (peek() == Kind.LP && (look(1) == Kind.LP || look(1) == Kind.MATCH || look(1) == Kind.OPTIONAL)) {
                next();
                Stmt query = nested(false);
                expect(Kind.RP, "EXISTS 缺少右括号");
                expr = new Expr.Exists(query);
            } else {
                expr = callExpr("exists");
            }
        } else if (peek() == Kind.VALUE && look(1) == Kind.LC) {
            next();
            next();
            Stmt query = nested(true);
            expect(Kind.RC, "VALUE 子查询缺少右花括号");
            expr = new Expr.ValueQuery(query);
        } else if (peek() == Kind.LET && isName(look(1)) && look(2) == Kind.EQ) {
            next();
            expr = letExpr();
        } else if (peek() == Kind.LIST && look(1) == Kind.LB) {
            next();
            next();
            expr = listLit();
        } else if (peek() == Kind.RECORD && look(1) == Kind.LC) {
            next();
            next();
            expr = mapLit();
        } else if (take(Kind.LB)) {
            expr = listLit();
        } else if (take(Kind.LC)) {
            expr = mapLit();
        } else if (isName(peek())) {
            String name = next().text;
            String lower = name.toLowerCase(Locale.ROOT);
            if (take(Kind.LP)) {
                expr = switch (lower) {
                    case "trim" -> trimExpr();
                    case "same" -> new Expr.Same(callArgs(2, "SAME"), false);
                    case "all_different" -> new Expr.Same(callArgs(2, "ALL_DIFFERENT"), true);
                    case "property_exists" -> {
                        Expr value = expr();
                        expect(Kind.COMMA, "PROPERTY_EXISTS 要写成 PROPERTY_EXISTS(变量, 属性名)");
                        String property = peek() == Kind.TEXT ? next().text : ident();
                        expect(Kind.RP, "PROPERTY_EXISTS 缺少右括号");
                        yield new Expr.PropExists(value, property);
                    }
                    default -> arguments(name);
                };
            } else if (take(Kind.DOT)) {
                expr = new Expr.Prop(name, ident());
            } else if (NILADIC.contains(lower)) {
                expr = new Expr.Call(name, List.of());
            } else {
                expr = new Expr.Variable(name);
            }
        } else {
            return fail("无法识别的表达式");
        }
        return postfix(expr);
    }

    /** 至少 {@code min} 个参数，左括号已经读过。 */
    private List<Expr> callArgs(int min, String name) {
        List<Expr> values = new ArrayList<>();
        values.add(expr());
        while (take(Kind.COMMA)) {
            values.add(expr());
        }
        expect(Kind.RP, name + " 缺少右括号");
        if (values.size() < min) {
            fail(name + " 至少要 " + min + " 个参数");
        }
        return List.copyOf(values);
    }

    /** {@code TRIM([BOTH|LEADING|TRAILING] [字符] FROM 文本)} 或 {@code TRIM(文本)}，左括号已经读过。 */
    private Expr trimExpr() {
        String spec = "BOTH";
        boolean explicit = false;
        if (peek() == Kind.BOTH || peek() == Kind.LEADING || peek() == Kind.TRAILING) {
            spec = next().text.toUpperCase(Locale.ROOT);
            explicit = true;
        }
        Expr chars = null;
        Expr source;
        if (take(Kind.FROM)) {
            source = expr();
        } else {
            Expr first = expr();
            if (take(Kind.FROM)) {
                chars = first;
                source = expr();
            } else if (explicit) {
                return fail("TRIM 写了 BOTH / LEADING / TRAILING 时要写 FROM");
            } else if (take(Kind.COMMA)) {
                // 层岩旧写法 TRIM(文本, 字符)
                source = first;
                chars = expr();
            } else {
                source = first;
            }
        }
        expect(Kind.RP, "TRIM 缺少右括号");
        return new Expr.Trim(spec, chars, source);
    }

    private Expr letExpr() {
        List<String> names = new ArrayList<>();
        List<Expr> values = new ArrayList<>();
        do {
            names.add(ident());
            expect(Kind.EQ, "LET 后面要写等号");
            letDepth++;
            try {
                values.add(expr());
            } finally {
                letDepth--;
            }
        } while (take(Kind.COMMA));
        expect(Kind.IN, "LET 表达式要写成 LET 名字 = 值 IN 表达式 END");
        int saved = letDepth;
        letDepth = 0;
        Expr body;
        try {
            body = expr();
        } finally {
            letDepth = saved;
        }
        expect(Kind.END, "LET 表达式缺少 END");
        return new Expr.LetIn(List.copyOf(names), List.copyOf(values), body);
    }

    private Expr reduceExpr() {
        expect(Kind.LP, "REDUCE 缺少左括号");
        String accumulator = ident();
        expect(Kind.EQ, "REDUCE 初始值缺少等号");
        Expr initial = expr();
        expect(Kind.COMMA, "REDUCE 初始值后面要写逗号");
        String item = ident();
        expect(Kind.IN, "REDUCE 迭代变量后面要写 IN");
        Expr collection = expr();
        expect(Kind.PIPE, "REDUCE 集合后面要写 | ");
        Expr body = expr();
        expect(Kind.RP, "REDUCE 缺少右括号");
        return new Expr.Reduce(accumulator, initial, item, collection, body);
    }

    private Expr postfix(Expr expr) {
        while (true) {
            if (take(Kind.LB)) {
                sliceDepth++;
                try {
                    Expr start = expr();
                    Expr end = null;
                    if (take(Kind.COLON)) {
                        end = peek() == Kind.RB ? null : expr();
                    }
                    expect(Kind.RB, "下标缺少右方括号");
                    expr = new Expr.Index(expr, start, end);
                } finally {
                    sliceDepth--;
                }
            } else {
                return expr;
            }
        }
    }

    private Expr listLit() {
        if (take(Kind.RB)) {
            return new Expr.ListLit(List.of());
        }
        List<Expr> items = new ArrayList<>();
        items.add(expr());
        while (take(Kind.COMMA)) {
            items.add(expr());
        }
        expect(Kind.RB, "列表缺少右方括号");
        return new Expr.ListLit(List.copyOf(items));
    }

    private Expr mapLit() {
        if (take(Kind.RC)) {
            return new Expr.MapLit(List.of(), List.of());
        }
        List<String> keys = new ArrayList<>();
        List<Expr> values = new ArrayList<>();
        do {
            String key = ident();
            if (keys.contains(key)) {
                return fail("映射键 " + key + " 写了两次");
            }
            keys.add(key);
            expect(Kind.COLON, "映射键后面要写冒号");
            values.add(expr());
        } while (take(Kind.COMMA));
        expect(Kind.RC, "映射缺少右花括号");
        return new Expr.MapLit(List.copyOf(keys), List.copyOf(values));
    }

    private Expr caseExpr() {
        Expr scrutinee = peek() == Kind.WHEN ? null : expr();
        List<Expr> whens = new ArrayList<>();
        List<Expr> thens = new ArrayList<>();
        if (!take(Kind.WHEN)) {
            return fail("CASE 后面要写 WHEN");
        }
        do {
            whens.add(expr());
            expect(Kind.THEN, "WHEN 后面要写 THEN");
            thens.add(expr());
        } while (take(Kind.WHEN));
        Expr otherwise = take(Kind.ELSE) ? expr() : null;
        expect(Kind.END, "CASE 缺少 END");
        return new Expr.CaseExpr(scrutinee, List.copyOf(whens), List.copyOf(thens), otherwise);
    }

    private Expr callExpr(String name) {
        expect(Kind.LP, "函数缺少左括号");
        return arguments(name);
    }

    private Expr arguments(String name) {
        List<Expr> args = new ArrayList<>();
        if (take(Kind.DISTINCT)) {
            args.add(new Expr.Unary("DISTINCT", expr()));
            expect(Kind.RP, "函数调用缺少右括号");
            return new Expr.Call(name, List.copyOf(args));
        }
        take(Kind.ALL);
        if (take(Kind.STAR)) {
            args.add(new Expr.Literal("*"));
            expect(Kind.RP, "函数调用缺少右括号");
            return new Expr.Call(name, List.copyOf(args));
        }
        if (!take(Kind.RP)) {
            args.add(expr());
            while (take(Kind.COMMA)) {
                args.add(expr());
            }
            expect(Kind.RP, "函数调用缺少右括号");
        }
        return new Expr.Call(name, List.copyOf(args));
    }

    // ---------------------------------------------------------------- 词法辅助

    private String ident() {
        if (!isName(peek())) {
            return fail("这里要写名字");
        }
        return next().text;
    }

    private boolean ifNotExists() {
        if (peek() != Kind.IF) {
            return false;
        }
        next();
        expect(Kind.NOT, "IF 后面要写 NOT EXISTS");
        expect(Kind.EXISTS, "IF NOT 后面要写 EXISTS");
        return true;
    }

    private boolean ifExists() {
        if (!take(Kind.IF)) {
            return false;
        }
        expect(Kind.EXISTS, "IF 后面要写 EXISTS");
        return true;
    }

    private boolean isName(Kind kind) {
        if (kind == Kind.AND || kind == Kind.OR || kind == Kind.XOR || kind == Kind.NOT
                || kind == Kind.TRUE || kind == Kind.FALSE || kind == Kind.NULL || kind == Kind.UNKNOWN
                || kind == Kind.AS || kind == Kind.IN || kind == Kind.IS) {
            return false;
        }
        if (kind == Kind.IDENT || kind == Kind.ID || kind == Kind.TYPE) {
            return true;
        }
        Tok token = at();
        return token.kind == kind && KEYWORDS.containsKey(token.text.toLowerCase(Locale.ROOT));
    }

    /** 参数名和过程名可以和关键字重合，例如 {@code rel}。 */
    private String name() {
        Tok token = at();
        if (token.kind == Kind.IDENT || KEYWORDS.containsKey(token.text.toLowerCase(Locale.ROOT))) {
            return next().text;
        }
        return fail("这里要写名字");
    }

    private long integer() {
        if (peek() != Kind.INTEGER) {
            fail("这里要写整数");
        }
        try {
            return Long.parseLong(next().text);
        } catch (NumberFormatException ex) {
            throw new ParseException("数字超出范围");
        }
    }

    private Kind peek() {
        return at().kind;
    }

    private Kind look(int ahead) {
        int index = cursor + ahead;
        if (index >= tokens.size()) {
            return Kind.EOF;
        }
        return tokens.get(index).kind;
    }

    private boolean take(Kind kind) {
        if (peek() != kind) {
            return false;
        }
        next();
        return true;
    }

    private void expect(Kind kind, String message) {
        if (!take(kind)) {
            fail(message);
        }
    }

    private Tok next() {
        Tok token = at();
        if (cursor < tokens.size() - 1) {
            cursor++;
        }
        return token;
    }

    private Tok at() {
        return tokens.get(cursor);
    }

    private int previousEnd() {
        int index = Math.max(0, cursor - 1);
        return tokens.get(index).end;
    }

    private <T> T fail(String message) {
        Tok token = at();
        throw new ParseException("第 " + token.line + " 行第 " + token.column + " 列" + message);
    }

    private static List<Tok> tokenize(String source) {
        List<Tok> tokens = new ArrayList<>();
        int index = 0;
        int line = 1;
        int column = 1;
        int length = source.length();
        while (index < length) {
            char current = source.charAt(index);
            if (current == ' ' || current == '\t') {
                index++;
                column++;
                continue;
            }
            if (current == '\r') {
                index++;
                column = 1;
                continue;
            }
            if (current == '\n') {
                index++;
                line++;
                column = 1;
                continue;
            }
            if ((current == '/' || current == '-') && index + 1 < length && source.charAt(index + 1) == current) {
                // 标准的两种行注释：// 和 --
                while (index < length && source.charAt(index) != '\n') {
                    index++;
                }
                continue;
            }
            if (current == '/' && index + 1 < length && source.charAt(index + 1) == '*') {
                int startLine = line;
                int startColumn = column;
                index += 2;
                column += 2;
                boolean closed = false;
                while (index < length) {
                    if (source.charAt(index) == '*' && index + 1 < length && source.charAt(index + 1) == '/') {
                        index += 2;
                        column += 2;
                        closed = true;
                        break;
                    }
                    if (source.charAt(index) == '\n') {
                        line++;
                        column = 1;
                        index++;
                    } else {
                        index++;
                        column++;
                    }
                }
                if (!closed) {
                    throw new ParseException("第 " + startLine + " 行第 " + startColumn + " 列注释没有结束");
                }
                continue;
            }
            if (current == '`') {
                int startColumn = column;
                index++;
                column++;
                StringBuilder body = new StringBuilder();
                boolean closed = false;
                while (index < length) {
                    char next = source.charAt(index);
                    if (next == '`') {
                        if (index + 1 < length && source.charAt(index + 1) == '`') {
                            body.append('`');
                            index += 2;
                            column += 2;
                            continue;
                        }
                        index++;
                        column++;
                        closed = true;
                        break;
                    }
                    if (next == '\n' || next == '\r') {
                        throw new ParseException("第 " + line + " 行第 " + column + " 列名字没有结束");
                    }
                    body.append(next);
                    index++;
                    column++;
                }
                if (!closed || body.isEmpty()) {
                    throw new ParseException("第 " + line + " 行第 " + startColumn + " 列名字没有结束");
                }
                tokens.add(new Tok(Kind.IDENT, body.toString(), index, index, line, startColumn));
                continue;
            }
            int codePoint = source.codePointAt(index);
            if (identifierStart(codePoint)) {
                int start = index;
                int startColumn = column;
                index += Character.charCount(codePoint);
                column++;
                while (index < length) {
                    int next = source.codePointAt(index);
                    if (identifierPart(next)) {
                        index += Character.charCount(next);
                        column++;
                    } else {
                        break;
                    }
                }
                String text = source.substring(start, index);
                Kind keyword = KEYWORDS.get(text.toLowerCase(Locale.ROOT));
                tokens.add(new Tok(keyword == null ? Kind.IDENT : keyword, text, start, index, line, startColumn));
                continue;
            }
            if (current == '.' && index + 1 < length && Character.isDigit(source.charAt(index + 1))
                    || Character.isDigit(current)) {
                int startColumn = column;
                String number = readNumber(source, index);
                int consumed = number.length();
                String plain = number.replace("_", "");
                boolean real = plain.indexOf('.') >= 0 || plain.indexOf('e') >= 0 || plain.indexOf('E') >= 0;
                String text = plain;
                if (!real && plain.length() > 2 && (plain.startsWith("0x") || plain.startsWith("0X")
                        || plain.startsWith("0o") || plain.startsWith("0O")
                        || plain.startsWith("0b") || plain.startsWith("0B"))) {
                    text = String.valueOf(parseRadix(plain));
                }
                boolean radix = plain.length() > 2 && plain.charAt(0) == '0' && "xXoObB".indexOf(plain.charAt(1)) >= 0;
                int after = index + consumed;
                if (!radix && after < length && "dDfFmM".indexOf(source.charAt(after)) >= 0
                        && (after + 1 >= length || !identifierPart(source.codePointAt(after + 1)))) {
                    // 数字后缀：d / f 是浮点，m 是精确数。
                    char suffix = Character.toLowerCase(source.charAt(after));
                    if (suffix == 'm' && real) {
                        throw new ParseException("第 " + line + " 行第 " + startColumn + " 列暂不支持 DECIMAL 精确小数");
                    }
                    if (suffix != 'm') {
                        real = true;
                        text = String.valueOf(Double.parseDouble(plain));
                    }
                    consumed++;
                } else if (after < length && identifierPart(source.codePointAt(after))) {
                    throw new ParseException("第 " + line + " 行第 " + startColumn + " 列数字后面不能直接跟名字");
                }
                tokens.add(new Tok(real ? Kind.REAL : Kind.INTEGER, text, index, index + consumed, line, startColumn));
                index += consumed;
                column += consumed;
                continue;
            }
            if (current == '\'' || current == '"') {
                char quote = current;
                int startColumn = column;
                int bodyStart = index + 1;
                index++;
                column++;
                StringBuilder body = new StringBuilder();
                boolean closed = false;
                while (index < length) {
                    char next = source.charAt(index);
                    if (next == '\n' || next == '\r') {
                        throw new ParseException("第 " + line + " 行第 " + column + " 列字符串没有结束");
                    }
                    if (next == '\\') {
                        if (index + 1 >= length) {
                            throw new ParseException("第 " + line + " 行第 " + column + " 列字符串没有结束");
                        }
                        char escaped = source.charAt(index + 1);
                        body.append(switch (escaped) {
                            case 'n' -> '\n';
                            case 't' -> '\t';
                            case 'r' -> '\r';
                            case '\\' -> '\\';
                            case '\'' -> '\'';
                            case '"' -> '"';
                            case 'u' -> {
                                if (index + 5 >= length) {
                                    throw new ParseException("第 " + line + " 行第 " + column + " 列 unicode 转义不完整");
                                }
                                String hex = source.substring(index + 2, index + 6);
                                index += 4;
                                column += 4;
                                yield (char) Integer.parseInt(hex, 16);
                            }
                            default -> escaped;
                        });
                        index += 2;
                        column += 2;
                        continue;
                    }
                    if (next == quote) {
                        if (index + 1 < length && source.charAt(index + 1) == quote) {
                            body.append(quote);
                            index += 2;
                            column += 2;
                            continue;
                        }
                        index++;
                        column++;
                        closed = true;
                        break;
                    }
                    body.append(next);
                    index++;
                    column++;
                }
                if (!closed) {
                    throw new ParseException("第 " + line + " 行第 " + startColumn + " 列字符串没有结束");
                }
                tokens.add(new Tok(Kind.TEXT, body.toString(), bodyStart, index, line, startColumn));
                continue;
            }
            Tok symbol = symbol(source, index, line, column);
            if (symbol == null) {
                throw new ParseException("第 " + line + " 行第 " + column + " 列无法识别");
            }
            tokens.add(symbol);
            int width = symbol.end - symbol.start;
            index += width;
            column += width;
        }
        tokens.add(new Tok(Kind.EOF, "", source.length(), source.length(), line, column));
        return tokens;
    }

    private static String readNumber(String source, int index) {
        int start = index;
        int length = source.length();
        char current = source.charAt(index);
        if (current == '0' && index + 1 < length) {
            char mark = source.charAt(index + 1);
            if (mark == 'x' || mark == 'X' || mark == 'o' || mark == 'O' || mark == 'b' || mark == 'B') {
                index += 2;
                while (index < length) {
                    char digit = source.charAt(index);
                    if (digit == '_' || Character.digit(digit, mark == 'b' || mark == 'B' ? 2 : mark == 'o' || mark == 'O' ? 8 : 16) >= 0) {
                        index++;
                    } else {
                        break;
                    }
                }
                return source.substring(start, index);
            }
        }
        if (current != '.') {
            index = digits(source, index);
        }
        if (index < length && source.charAt(index) == '.') {
            index++;
            index = digits(source, index);
        }
        if (index < length && (source.charAt(index) == 'e' || source.charAt(index) == 'E')) {
            int exponent = index + 1;
            if (exponent < length && (source.charAt(exponent) == '+' || source.charAt(exponent) == '-')) {
                exponent++;
            }
            if (exponent < length && Character.isDigit(source.charAt(exponent))) {
                index = digits(source, exponent);
            }
        }
        return source.substring(start, index);
    }

    private static int digits(String source, int index) {
        while (index < source.length()) {
            char digit = source.charAt(index);
            if (Character.isDigit(digit) || digit == '_') {
                index++;
            } else {
                break;
            }
        }
        return index;
    }

    private static long parseRadix(String text) {
        int radix = switch (text.charAt(1)) {
            case 'x', 'X' -> 16;
            case 'o', 'O' -> 8;
            case 'b', 'B' -> 2;
            default -> 10;
        };
        return Long.parseLong(text.substring(2), radix);
    }

    /** 标准的正则标识符：以字母或下划线开头，后面是字母、数字、下划线和组合符号。 */
    private static boolean identifierStart(int codePoint) {
        return codePoint == '_' || Character.isLetter(codePoint);
    }

    private static boolean identifierPart(int codePoint) {
        return codePoint == '_' || Character.isLetterOrDigit(codePoint)
                || (Character.isUnicodeIdentifierPart(codePoint) && !Character.isIdentifierIgnorable(codePoint) && codePoint > 127);
    }

    private static Tok symbol(String source, int index, int line, int column) {
        char current = source.charAt(index);
        char next = index + 1 < source.length() ? source.charAt(index + 1) : '\0';
        if (current == '|' && next == '+' && index + 2 < source.length() && source.charAt(index + 2) == '|') {
            return new Tok(Kind.MULTISET, "|+|", index, index + 3, line, column);
        }
        if (current == '|' && next == '|') {
            return new Tok(Kind.CONCAT, "||", index, index + 2, line, column);
        }
        if (current == '<' && next == '-') {
            return new Tok(Kind.LARROW, "<-", index, index + 2, line, column);
        }
        if (current == '<' && next == '=') {
            return new Tok(Kind.LE, "<=", index, index + 2, line, column);
        }
        if (current == '<' && next == '>') {
            return new Tok(Kind.NEQ, "<>", index, index + 2, line, column);
        }
        if (current == '-' && next == '>') {
            return new Tok(Kind.ARROW, "->", index, index + 2, line, column);
        }
        if (current == '>' && next == '=') {
            return new Tok(Kind.GE, ">=", index, index + 2, line, column);
        }
        Kind kind = switch (current) {
            case '<' -> Kind.LT;
            case '>' -> Kind.GT;
            case '=' -> Kind.EQ;
            case '+' -> Kind.PLUS;
            case '-' -> Kind.MINUS;
            case '*' -> Kind.STAR;
            case '/' -> Kind.SLASH;
            case '(' -> Kind.LP;
            case ')' -> Kind.RP;
            case '[' -> Kind.LB;
            case ']' -> Kind.RB;
            case '{' -> Kind.LC;
            case '}' -> Kind.RC;
            case ',' -> Kind.COMMA;
            case ':' -> Kind.COLON;
            case ';' -> Kind.SEMI;
            case '.' -> Kind.DOT;
            case '@' -> Kind.AT;
            case '$' -> Kind.DOLLAR;
            case '|' -> Kind.PIPE;
            case '!' -> Kind.BANG;
            case '%' -> Kind.PERCENT;
            case '&' -> Kind.AMP;
            case '~' -> Kind.TILDE;
            case '?' -> Kind.QUESTION;
            default -> null;
        };
        if (kind == null) {
            return null;
        }
        return new Tok(kind, String.valueOf(current), index, index + 1, line, column);
    }

    private enum Kind {
        IDENT, INTEGER, REAL, TEXT, STRING,
        BOOL, BOOLEAN, INT, INT64, INTEGER_WORD, DOUBLE, DATETIME, ID,
        CREATE, GRAPH, GRAPHS, PARTS, REPLICAS, USE, ALIAS, ALIASES, FOR, DROP, SHOW, NODE, REL, TYPES, TYPE,
        DESCRIBE, ALTER, ADD, NULL, INSERT, MATCH, WHERE, RETURN, ORDER, BY, LIMIT, SKIP,
        SET, DELETE, DISTINCT, CALL, YIELD, INDEX, INDEXES, UNIQUE, ON, COMMENT, EXPLAIN,
        AND, OR, XOR, NOT, IS, IN, AS, ASC, DESC, TRUE, FALSE,
        IF, EXISTS, OPTIONAL, FILTER, LET, GROUP, UNION, EXCEPT, INTERSECT, ALL, ANY, NEXT, OFFSET,
        DETACH, NODETACH, REMOVE, RENAME, TO, CAST, CASE, WHEN, THEN, ELSE, END,
        WALK, TRAIL, SIMPLE, ACYCLIC, SHORTEST, TYPED, LABELED, SOURCE, DESTINATION, OF, SAME,
        UNKNOWN, GROUPS, OTHERWISE, PATH, PATHS, DIRECTED, SESSION, PROPERTY, START, TRANSACTION, COMMIT, ROLLBACK,
        FINISH, NO, BINDINGS, REPEATABLE, DIFFERENT, ELEMENT, ELEMENTS, EDGE, EDGES, KEEP, WITH, ORDINALITY, VALUE,
        LIST, RECORD, LIKE, LABEL, LABELS, CONNECTING, UNDIRECTED, LEADING, TRAILING, BOTH, FROM, REPLACE,
        ARROW, LARROW, LE, GE, NEQ, EQ, LT, GT, PLUS, MINUS, STAR, SLASH, CONCAT, BANG, PERCENT, AMP, TILDE, QUESTION, MULTISET,
        LP, RP, LB, RB, LC, RC, COMMA, COLON, SEMI, DOT, AT, DOLLAR, PIPE, EOF
    }

    private record Tok(Kind kind, String text, int start, int end, int line, int column) {
    }
}
