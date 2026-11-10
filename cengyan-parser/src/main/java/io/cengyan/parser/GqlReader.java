package io.cengyan.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code Gql.g4} 的直接实现。生成器不可用时仍按同一套规则解析，
 * 关键字不区分大小写，标识符保留原来的大小写。
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
            Map.entry("desc", Kind.DESC),
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
            Map.entry("if", Kind.IF),
            Map.entry("exists", Kind.EXISTS),
            Map.entry("optional", Kind.OPTIONAL),
            Map.entry("filter", Kind.FILTER),
            Map.entry("let", Kind.LET),
            Map.entry("group", Kind.GROUP),
            Map.entry("union", Kind.UNION),
            Map.entry("except", Kind.EXCEPT),
            Map.entry("intersect", Kind.INTERSECT),
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
            Map.entry("xor", Kind.XOR),
            Map.entry("typed", Kind.TYPED),
            Map.entry("labeled", Kind.LABELED),
            Map.entry("source", Kind.SOURCE),
            Map.entry("destination", Kind.DESTINATION),
            Map.entry("of", Kind.OF),
            Map.entry("same", Kind.SAME));

    private final String source;
    private final List<Tok> tokens;
    private int cursor;

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
                yield new Stmt.Use(ident());
            }
            case DROP -> drop();
            case SHOW -> show();
            case DESCRIBE -> describe();
            case ALTER -> alter();
            case INSERT -> insert();
            case MATCH -> query(false);
            case RETURN -> new Stmt.Select(List.of(), tail());
            case OPTIONAL -> {
                next();
                expect(Kind.MATCH, "OPTIONAL 后面要写 MATCH");
                yield query(true);
            }
            case EXPLAIN -> {
                next();
                Stmt explained = query(false);
                if (!(explained instanceof Stmt.Match match)) {
                    yield new Stmt.ExplainQuery(explained);
                }
                yield new Stmt.Explain(match);
            }
            case CALL -> call();
            default -> fail("无法识别的语句");
        };
    }

    private Stmt create() {
        expect(Kind.CREATE, "缺少 CREATE");
        if (take(Kind.GRAPH)) {
            String name = ident();
            boolean ifNotExists = ifNotExists();
            int parts = take(Kind.PARTS) ? positiveInt("分片数量") : 1;
            return new Stmt.CreateGraph(name, parts, comment(), ifNotExists);
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
        int idLength = 19;
        boolean idSpecified = false;
        if (node && take(Kind.ID)) {
            idSpecified = true;
            if (take(Kind.STRING)) {
                idType = "STRING";
                idLength = idLength();
            } else if (take(Kind.INT) || take(Kind.INT64) || take(Kind.INTEGER_WORD)) {
                idType = "INT";
                if (take(Kind.LP)) {
                    idLength = positiveInt("ID 长度");
                    expect(Kind.RP, "ID 长度缺少右括号");
                }
                if (idLength > 19) {
                    return fail("INT ID 长度不能超过 19 位");
                }
            } else {
                return fail("ID 后面要写 INT 或 STRING");
            }
        }
        expect(Kind.LP, "类型定义缺少左括号");
        List<Stmt.Field> fields = fields();
        expect(Kind.RP, "类型定义缺少右括号");
        return new Stmt.CreateType(node, name, comment, idType, idLength, idSpecified, fields, ifNotExists);
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
        expect(Kind.GRAPH, "只能 DROP GRAPH、DROP ALIAS、DROP INDEX 或 DROP TYPE");
        return new Stmt.DropGraph(ident(), ifExists());
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
        String valueType = typeName();
        expect(Kind.NULL, "只能在末尾增加可空列");
        return new Stmt.AlterType(node, typeName, property, valueType, comment());
    }

    private Stmt insert() {
        expect(Kind.INSERT, "缺少 INSERT");
        List<Stmt.Element> elements = insertElements();
        if (elements.size() == 1 && elements.get(0) instanceof Stmt.Element.Node node) {
            return new Stmt.Insert(node.node(), null, null);
        }
        if (elements.size() == 3
                && elements.get(0) instanceof Stmt.Element.Node source
                && elements.get(1) instanceof Stmt.Element.Rel rel
                && elements.get(2) instanceof Stmt.Element.Node target) {
            return new Stmt.Insert(source.node(), rel.edge(), target.node());
        }
        return new Stmt.InsertGraph(elements);
    }

    private Stmt query(boolean optional) {
        take(Kind.MATCH);
        List<Stmt.Clause> clauses = new ArrayList<>();
        boolean nextOptional = optional;
        while (true) {
            if (!clauses.isEmpty()) {
                if (take(Kind.OPTIONAL)) {
                    nextOptional = true;
                    expect(Kind.MATCH, "OPTIONAL 后面要写 MATCH");
                } else if (take(Kind.MATCH)) {
                    nextOptional = false;
                } else {
                    break;
                }
            }
            clauses.add(matchClause(nextOptional));
            nextOptional = false;
            while (true) {
                if (take(Kind.FILTER)) {
                    clauses.add(new Stmt.Clause.Filter(expr()));
                } else if (take(Kind.LET)) {
                    String letName = ident();
                    expect(Kind.EQ, "LET 后面要写等号");
                    clauses.add(new Stmt.Clause.Let(letName, expr()));
                } else if (peek() == Kind.FOR && look(1) != Kind.EQ) {
                    next();
                    String forName = ident();
                    expect(Kind.IN, "FOR 后面要写 IN");
                    clauses.add(new Stmt.Clause.For(forName, expr()));
                } else {
                    break;
                }
            }
            if (peek() != Kind.MATCH && peek() != Kind.OPTIONAL) {
                break;
            }
        }
        if (take(Kind.INSERT)) {
            return new Stmt.InsertUsing(clauses, insertElements());
        }
        if (peek() == Kind.SET || peek() == Kind.DETACH || peek() == Kind.NODETACH || peek() == Kind.DELETE
                || peek() == Kind.REMOVE) {
            return mutation(clauses);
        }
        Stmt.Tail tail = tail();
        Expr aggregateFilter = null;
        if (take(Kind.FILTER)) {
            aggregateFilter = expr();
        }
        Stmt query = lower(clauses, tail);
        Stmt selected = query == null ? new Stmt.Select(clauses, tail) : query;
        return compose(aggregateFilter == null ? selected : new Stmt.AggregateFilter(selected, aggregateFilter));
    }

    private Stmt mutation(List<Stmt.Clause> clauses) {
        List<Stmt.Assign> assignments = List.of();
        List<Stmt.Assign> replacements = List.of();
        List<String> removals = List.of();
        if (take(Kind.SET)) {
            if (peek() == Kind.IDENT || isName(peek())) {
                String variable = ident();
                if (take(Kind.EQ)) {
                    replacements = List.of(new Stmt.Assign(variable, "", expr()));
                } else {
                    expect(Kind.DOT, "SET 要写成 变量.属性 = 值");
                    String field = ident();
                    expect(Kind.EQ, "SET 缺少等号");
                    List<Stmt.Assign> all = new ArrayList<>();
                    all.add(new Stmt.Assign(variable, field, expr()));
                    while (take(Kind.COMMA)) {
                        all.add(assignment());
                    }
                    assignments = all;
                }
            }
        }
        if (take(Kind.REMOVE)) {
            removals = variables();
        }
        boolean detach = false;
        List<String> deletes = List.of();
        if (take(Kind.DETACH)) {
            detach = true;
            expect(Kind.DELETE, "DETACH 后面要写 DELETE");
            deletes = variables();
        } else if (take(Kind.NODETACH)) {
            expect(Kind.DELETE, "NODETACH 后面要写 DELETE");
            deletes = variables();
        } else if (take(Kind.DELETE)) {
            deletes = variables();
        }
        Stmt legacy = lowerMutation(clauses, assignments, deletes, detach);
        if (legacy != null && replacements.isEmpty() && removals.isEmpty()) {
            return legacy;
        }
        return new Stmt.Mutate(clauses, assignments, replacements, removals, deletes, detach);
    }

    private Stmt.Clause.Match matchClause(boolean optional) {
        String pathVariable = null;
        if (isName(peek()) && look(1) == Kind.EQ) {
            pathVariable = name();
            expect(Kind.EQ, "路径变量后面要写等号");
        }
        String search = null;
        Integer searchCount = null;
        if (take(Kind.ANY)) {
            if (take(Kind.SHORTEST)) {
                search = "ANY_SHORTEST";
            } else {
                search = "ANY";
                searchCount = (int) integer();
            }
        } else if (take(Kind.ALL)) {
            expect(Kind.SHORTEST, "ALL 后面要写 SHORTEST");
            search = "ALL_SHORTEST";
        } else if (take(Kind.SHORTEST)) {
            search = "SHORTEST";
            searchCount = (int) integer();
        }
        String mode = null;
        if (peek() == Kind.WALK || peek() == Kind.TRAIL || peek() == Kind.SIMPLE || peek() == Kind.ACYCLIC) {
            mode = next().text.toUpperCase(Locale.ROOT);
        }
        List<Stmt.Pattern> patterns = new ArrayList<>();
        patterns.add(pattern());
        while (take(Kind.COMMA)) {
            patterns.add(pattern());
        }
        Expr where = take(Kind.WHERE) ? expr() : null;
        return new Stmt.Clause.Match(optional, pathVariable, search, searchCount, mode, patterns, where);
    }

    private Stmt.Pattern pattern() {
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(patternElement(true));
        while (peek() == Kind.MINUS || peek() == Kind.LARROW) {
            elements.add(new Stmt.Element.Rel(edge()));
            elements.add(patternElement(false));
        }
        return new Stmt.Pattern(elements);
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
        while (peek() == Kind.MINUS || peek() == Kind.LARROW) {
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
        return new Stmt.Element.Group(elements, where, range[0], max);
    }

    private Stmt.Tail tail() {
        expect(Kind.RETURN, "缺少 RETURN");
        boolean distinct = take(Kind.DISTINCT);
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
            groupBy = new ArrayList<>();
            groupBy.add(expr());
            while (take(Kind.COMMA)) {
                groupBy.add(expr());
            }
            groupBy = List.copyOf(groupBy);
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
        return new Stmt.Tail(distinct, star, items, groupBy, order, skip, limit, postFilter);
    }

    private Stmt compose(Stmt query) {
        if (peek() == Kind.UNION || peek() == Kind.EXCEPT || peek() == Kind.INTERSECT) {
            String op = next().text.toUpperCase(Locale.ROOT);
            boolean all = take(Kind.ALL);
            if (!all) {
                take(Kind.DISTINCT);
            }
            expect(Kind.MATCH, "集合运算后面要写查询");
            Stmt right = query(false);
            return new Stmt.Composite(op, all, query, right);
        }
        if (take(Kind.NEXT)) {
            if (peek() != Kind.MATCH && peek() != Kind.OPTIONAL && peek() != Kind.CALL) {
                return fail("NEXT 后面要写查询");
            }
            Stmt right = peek() == Kind.CALL ? call() : query(false);
            return new Stmt.Next(query, right);
        }
        return query;
    }

    private Stmt lower(List<Stmt.Clause> clauses, Stmt.Tail tail) {
        if (clauses.size() != 1 || !(clauses.get(0) instanceof Stmt.Clause.Match match)) {
            return null;
        }
        if (match.optional() || match.search() != null || match.mode() != null || match.patterns().size() != 1) {
            return null;
        }
        if (tail.star() || !tail.groupBy().isEmpty()) {
            return null;
        }
        List<Stmt.Element> elements = match.patterns().get(0).elements();
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
                tail.skip(), tail.limit(), tail.distinct(), match.pathVariable());
    }

    private Stmt lowerMutation(List<Stmt.Clause> clauses, List<Stmt.Assign> assignments, List<String> deletes, boolean detach) {
        if (clauses.size() != 1 || !(clauses.get(0) instanceof Stmt.Clause.Match match)) {
            return null;
        }
        if (match.optional() || match.search() != null || match.mode() != null || match.patterns().size() != 1) {
            return null;
        }
        List<Stmt.Element> elements = match.patterns().get(0).elements();
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
            if (rel.edge().minHop() != null || node.node().where() != null || complex(node.node().label())) {
                return null;
            }
            edge = rel.edge();
            target = node.node();
        }
        if (!deletes.isEmpty()) {
            return new Stmt.Delete(source.node(), edge, target, match.where(), deletes, detach);
        }
        if (!assignments.isEmpty()) {
            return new Stmt.Set(source.node(), edge, target, match.where(), assignments);
        }
        return null;
    }

    private static boolean complex(Stmt.Label label) {
        return label != null && !(label instanceof Stmt.Label.One);
    }

    private Stmt call() {
        expect(Kind.CALL, "缺少 CALL");
        String namespace = name();
        expect(Kind.DOT, "过程名要写成名字.名字");
        String procedure = name();
        expect(Kind.LP, "过程缺少左括号");
        List<Stmt.Arg> args = new ArrayList<>();
        if (take(Kind.LC)) {
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
        expect(Kind.YIELD, "过程调用要写 YIELD");
        List<String> columns = new ArrayList<>();
        columns.add(name());
        while (take(Kind.COMMA)) {
            columns.add(name());
        }
        Stmt.Call call = new Stmt.Call(namespace + "." + procedure, args, columns);
        if (peek() == Kind.FILTER || peek() == Kind.RETURN || peek() == Kind.WHERE || peek() == Kind.ORDER) {
            List<Stmt.Clause> clauses = new ArrayList<>();
            clauses.add(new Stmt.Clause.Call(call));
            while (take(Kind.FILTER)) {
                clauses.add(new Stmt.Clause.Filter(expr()));
            }
            if (peek() != Kind.RETURN) {
                return call;
            }
            return new Stmt.Select(clauses, tail());
        }
        return call;
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

    private List<Stmt.Assign> assignments() {
        List<Stmt.Assign> assignments = new ArrayList<>();
        assignments.add(assignment());
        while (take(Kind.COMMA)) {
            assignments.add(assignment());
        }
        return List.copyOf(assignments);
    }

    private Stmt.Assign assignment() {
        String variable = ident();
        expect(Kind.DOT, "SET 要写成 变量.属性 = 值");
        String field = ident();
        expect(Kind.EQ, "SET 缺少等号");
        return new Stmt.Assign(variable, field, expr());
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
        expect(Kind.REL, "这里要写 NODE 或 REL");
        return false;
    }

    private List<Stmt.Field> fields() {
        if (peek() == Kind.RP) {
            return List.of();
        }
        List<Stmt.Field> fields = new ArrayList<>();
        fields.add(field());
        while (take(Kind.COMMA)) {
            fields.add(field());
        }
        return List.copyOf(fields);
    }

    private Stmt.Field field() {
        String name = ident();
        String valueType = typeName();
        boolean nullable = take(Kind.NULL);
        return new Stmt.Field(name, valueType, nullable, comment());
    }

    private String comment() {
        if (!take(Kind.COMMENT)) return null;
        if (peek() != Kind.STRING) return fail("COMMENT 后面要写字符串");
        return next().text;
    }

    private String typeName() {
        Kind kind = peek();
        if (kind == Kind.BOOL || kind == Kind.BOOLEAN || kind == Kind.INT || kind == Kind.INTEGER_WORD
                || kind == Kind.INT64 || kind == Kind.DOUBLE || kind == Kind.STRING || kind == Kind.DATETIME) {
            return next().text;
        }
        return fail("无法识别的列类型");
    }

    private Stmt.NodePat node() {
        expect(Kind.LP, "节点缺少左括号");
        String variable = null;
        if (isName(peek()) && peek() != Kind.WHERE) {
            variable = next().text;
        }
        String typeName = null;
        Stmt.Label label = null;
        if (take(Kind.COLON)) {
            label = label();
            if (label instanceof Stmt.Label.One one) {
                typeName = one.name();
            }
        }
        List<Stmt.Prop> props = props();
        Expr where = take(Kind.WHERE) ? expr() : null;
        expect(Kind.RP, "节点缺少右括号");
        return new Stmt.NodePat(variable, typeName, props, where, label);
    }

    private Stmt.Label label() {
        Stmt.Label label = labelTerm();
        if (peek() == Kind.PIPE || peek() == Kind.AND) {
            Kind op = peek();
            List<Stmt.Label> options = new ArrayList<>();
            options.add(label);
            while (peek() == op) {
                next();
                options.add(labelTerm());
            }
            label = op == Kind.PIPE ? new Stmt.Label.Or(List.copyOf(options)) : new Stmt.Label.And(List.copyOf(options));
        }
        return label;
    }

    private Stmt.Label labelTerm() {
        if (take(Kind.PERCENT)) {
            return new Stmt.Label.Any();
        }
        if (take(Kind.BANG) || take(Kind.NOT)) {
            return new Stmt.Label.Not(labelTerm());
        }
        return new Stmt.Label.One(ident());
    }

    private List<Stmt.Element> insertElements() {
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(new Stmt.Element.Node(node()));
        while (peek() == Kind.COMMA || peek() == Kind.MINUS || peek() == Kind.LARROW) {
            if (take(Kind.COMMA)) {
                elements.add(new Stmt.Element.Node(node()));
                continue;
            }
            Stmt.EdgePat edge = edge();
            if (edge.minHop() != null) {
                return fail("插入边不能是多跳");
            }
            elements.add(new Stmt.Element.Rel(edge));
            elements.add(new Stmt.Element.Node(node()));
        }
        return elements;
    }

    private Stmt.EdgePat edge() {
        Stmt.EdgePat.Direction direction;
        if (take(Kind.LARROW)) {
            direction = Stmt.EdgePat.Direction.IN;
            expect(Kind.LB, "边缺少左方括号");
            Stmt.EdgePat edge = edgeBody(direction);
            expect(Kind.RB, "边缺少右方括号");
            expect(Kind.MINUS, "入边的箭头不完整");
            return hops(edge, quantifier());
        }
        expect(Kind.MINUS, "边缺少方向");
        expect(Kind.LB, "边缺少左方括号");
        Stmt.EdgePat undecided = edgeBody(Stmt.EdgePat.Direction.OUT);
        expect(Kind.RB, "边缺少右方括号");
        if (take(Kind.ARROW)) {
            direction = Stmt.EdgePat.Direction.OUT;
        } else if (take(Kind.MINUS)) {
            direction = Stmt.EdgePat.Direction.ANY;
        } else {
            return fail("边的方向不完整");
        }
        return hops(new Stmt.EdgePat(
                direction, undecided.variable(), undecided.typeNames(), undecided.rank(), undecided.properties(),
                null, null), quantifier());
    }

    private int[] quantifier() {
        if (peek() == Kind.STAR) {
            next();
            return new int[] {0, -1};
        }
        if (peek() == Kind.PLUS) {
            next();
            return new int[] {1, -1};
        }
        if (!take(Kind.LC)) {
            return null;
        }
        long min = integer();
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

    private static Stmt.EdgePat hops(Stmt.EdgePat edge, int[] range) {
        if (range == null) {
            return edge;
        }
        return new Stmt.EdgePat(
                edge.direction(), edge.variable(), edge.typeNames(), edge.rank(), edge.properties(),
                range[0], range[1]);
    }

    private Stmt.EdgePat edgeBody(Stmt.EdgePat.Direction direction) {
        String variable = null;
        if (isName(peek())) {
            variable = next().text;
        }
        List<String> typeNames = List.of();
        if (take(Kind.COLON)) {
            typeNames = typeNames();
        }
        Long rank = null;
        if (take(Kind.AT)) {
            rank = integer();
        }
        return new Stmt.EdgePat(direction, variable, typeNames, rank, props(), null, null);
    }

    /** {@code :a|:b} 或 {@code :a|b}。冒号可以写在每个类型前面。 */
    private List<String> typeNames() {
        java.util.ArrayList<String> names = new java.util.ArrayList<>();
        names.add(ident());
        while (take(Kind.PIPE)) {
            take(Kind.COLON);
            String name = ident();
            if (names.contains(name)) {
                return fail("边类型重复：" + name);
            }
            names.add(name);
        }
        return List.copyOf(names);
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
        return new Stmt.Sort(expr, descending);
    }

    private Expr expr() {
        return or();
    }

    private Expr or() {
        Expr left = xor(and());
        while (take(Kind.OR)) {
            left = new Expr.Binary("OR", left, xor(and()));
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

    private Expr xor(Expr left) {
        while (take(Kind.XOR)) {
            left = new Expr.Binary("XOR", left, not());
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
        if (kind == Kind.NOT && look(1) == Kind.IN) {
            next();
            return in(left, true);
        }
        if (peek() == Kind.IN) {
            return in(left, false);
        }
        if (take(Kind.IS)) {
            boolean negated = take(Kind.NOT);
            if (take(Kind.NULL)) {
                return new Expr.IsNull(left, negated);
            }
            if (take(Kind.TYPED)) {
                return new Expr.Typed(left, typeName(), negated);
            }
            if (take(Kind.LABELED)) {
                return new Expr.Labeled(left, label(), negated);
            }
            if (take(Kind.SOURCE)) {
                expect(Kind.OF, "SOURCE 后面要写 OF");
                return new Expr.EndpointOf(left, expr(), true, negated);
            }
            if (take(Kind.DESTINATION)) {
                expect(Kind.OF, "DESTINATION 后面要写 OF");
                return new Expr.EndpointOf(left, expr(), false, negated);
            }
            return fail("IS 后面无法识别");
        }
        return left;
    }

    private Expr in(Expr left, boolean negated) {
        expect(Kind.IN, "缺少 IN");
        if (take(Kind.LB)) {
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
        if (take(Kind.LP)) {
            List<Expr> options = new ArrayList<>();
            options.add(expr());
            while (take(Kind.COMMA)) {
                options.add(expr());
            }
            expect(Kind.RP, "IN 缺少右括号");
            return new Expr.In(left, List.copyOf(options), negated);
        }
        return new Expr.Member(left, expr(), negated);
    }

    private Expr add() {
        Expr left = mul();
        while (peek() == Kind.PLUS || peek() == Kind.MINUS || peek() == Kind.CONCAT) {
            String op = next().text;
            left = new Expr.Binary(op, left, mul());
        }
        return left;
    }

    private Expr mul() {
        Expr left = unary();
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
        return primary();
    }

    private Expr primary() {
        Expr expr;
        if (take(Kind.LP)) {
            Expr inner = expr();
            expect(Kind.RP, "表达式缺少右括号");
            expr = inner;
        } else if (take(Kind.DOLLAR)) {
            expr = new Expr.Param(ident());
        } else if (take(Kind.DATETIME)) {
            if (peek() != Kind.STRING) {
                return fail("DATETIME 后面要写字符串");
            }
            expr = new Expr.Literal(new DateTimeText(next().text));
        } else if (peek() == Kind.INTEGER) {
            expr = new Expr.Literal(integer());
        } else if (peek() == Kind.REAL) {
            expr = new Expr.Literal(Double.valueOf(next().text));
        } else if (peek() == Kind.STRING) {
            expr = new Expr.Literal(next().text);
        } else if (take(Kind.TRUE)) {
            expr = new Expr.Literal(true);
        } else if (take(Kind.FALSE)) {
            expr = new Expr.Literal(false);
        } else if (take(Kind.NULL)) {
            expr = new Expr.Literal(null);
        } else if (take(Kind.CASE)) {
            expr = caseExpr();
        } else if (take(Kind.CAST)) {
            expect(Kind.LP, "CAST 缺少左括号");
            Expr value = expr();
            expect(Kind.AS, "CAST 缺少 AS");
            String type = typeName();
            expect(Kind.RP, "CAST 缺少右括号");
            expr = new Expr.Cast(value, type);
        } else if (peek() == Kind.IDENT && "REDUCE".equalsIgnoreCase(at().text)
                && look(1) == Kind.LP) {
            next();
            expr = reduceExpr();
        } else if (take(Kind.EXISTS)) {
            if (take(Kind.LC)) {
                Stmt query = peek() == Kind.MATCH || peek() == Kind.OPTIONAL ? statement() : query(false);
                expect(Kind.RC, "EXISTS 缺少右花括号");
                expr = new Expr.Exists(query);
            } else {
                expr = callExpr("exists");
            }
        } else if (take(Kind.LB)) {
            expr = listLit();
        } else if (take(Kind.LC)) {
            expr = mapLit();
        } else if (isName(peek())) {
            String name = next().text;
            if (take(Kind.LP)) {
                expr = arguments(name);
            } else if (take(Kind.DOT)) {
                expr = new Expr.Prop(name, ident());
            } else {
                expr = new Expr.Variable(name);
            }
        } else {
            return fail("无法识别的表达式");
        }
        return postfix(expr);
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
        while (take(Kind.LB)) {
            Expr start = expr();
            Expr end = null;
            if (take(Kind.COLON)) {
                end = peek() == Kind.RB ? null : expr();
            }
            expect(Kind.RB, "下标缺少右方括号");
            expr = new Expr.Index(expr, start, end);
        }
        return expr;
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
        keys.add(ident());
        expect(Kind.COLON, "映射键后面要写冒号");
        values.add(expr());
        while (take(Kind.COMMA)) {
            keys.add(ident());
            expect(Kind.COLON, "映射键后面要写冒号");
            values.add(expr());
        }
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

    private String ident() {
        if (!isName(peek())) {
            return fail("这里要写名字");
        }
        return next().text;
    }

    private boolean ifNotExists() {
        if (!take(Kind.IF)) {
            return false;
        }
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
                || kind == Kind.TRUE || kind == Kind.FALSE || kind == Kind.NULL
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
            if (current == '/' && index + 1 < length && source.charAt(index + 1) == '/') {
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
            if (Character.isLetter(current) && current < 128) {
                int start = index;
                int startColumn = column;
                index++;
                column++;
                while (index < length) {
                    char next = source.charAt(index);
                    if (Character.isLetterOrDigit(next) && next < 128 || next == '_') {
                        index++;
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
                tokens.add(new Tok(Kind.STRING, body.toString(), bodyStart, index, line, startColumn));
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

    private static Tok symbol(String source, int index, int line, int column) {
        char current = source.charAt(index);
        char next = index + 1 < source.length() ? source.charAt(index + 1) : '\0';
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
            default -> null;
        };
        if (kind == null) {
            return null;
        }
        return new Tok(kind, String.valueOf(current), index, index + 1, line, column);
    }

    private enum Kind {
        IDENT, INTEGER, REAL, STRING,
        BOOL, BOOLEAN, INT, INT64, INTEGER_WORD, DOUBLE, DATETIME, ID,
        CREATE, GRAPH, GRAPHS, PARTS, USE, ALIAS, ALIASES, FOR, DROP, SHOW, NODE, REL, TYPES, TYPE,
        DESCRIBE, ALTER, ADD, NULL, INSERT, MATCH, WHERE, RETURN, ORDER, BY, LIMIT, SKIP,
        SET, DELETE, DISTINCT, CALL, YIELD, INDEX, INDEXES, UNIQUE, ON, COMMENT, EXPLAIN,
        AND, OR, XOR, NOT, IS, IN, AS, ASC, DESC, TRUE, FALSE,
        IF, EXISTS, OPTIONAL, FILTER, LET, GROUP, UNION, EXCEPT, INTERSECT, ALL, ANY, NEXT, OFFSET,
        DETACH, NODETACH, REMOVE, RENAME, TO, CAST, CASE, WHEN, THEN, ELSE, END,
        WALK, TRAIL, SIMPLE, ACYCLIC, SHORTEST, TYPED, LABELED, SOURCE, DESTINATION, OF, SAME,
        ARROW, LARROW, LE, GE, NEQ, EQ, LT, GT, PLUS, MINUS, STAR, SLASH, CONCAT, BANG, PERCENT,
        LP, RP, LB, RB, LC, RC, COMMA, COLON, SEMI, DOT, AT, DOLLAR, PIPE, EOF
    }

    private record Tok(Kind kind, String text, int start, int end, int line, int column) {
    }
}
