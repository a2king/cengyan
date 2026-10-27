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
            Map.entry("datetime", Kind.DATETIME),
            Map.entry("create", Kind.CREATE),
            Map.entry("graph", Kind.GRAPH),
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
            Map.entry("false", Kind.FALSE));

    private final String source;
    private final List<Tok> tokens;
    private int cursor;

    private GqlReader(String source) {
        this.source = source;
        this.tokens = tokenize(source);
    }

    static Stmt parse(String source) {
        if (source == null || source.isBlank()) {
            throw new ParseException("查询不能为空");
        }
        return new GqlReader(source).script();
    }

    private Stmt script() {
        Stmt statement = statement();
        take(Kind.SEMI);
        expect(Kind.EOF, "语句后面还有多余内容");
        return statement;
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
            case MATCH -> match();
            case CALL -> call();
            default -> fail("无法识别的语句");
        };
    }

    private Stmt create() {
        expect(Kind.CREATE, "缺少 CREATE");
        if (take(Kind.GRAPH)) {
            return new Stmt.CreateGraph(ident());
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
        expect(Kind.LP, "类型定义缺少左括号");
        List<Stmt.Field> fields = fields();
        expect(Kind.RP, "类型定义缺少右括号");
        return new Stmt.CreateType(node, name, fields);
    }

    private Stmt createIndex(boolean unique) {
        expect(Kind.INDEX, "缺少 INDEX");
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
        return new Stmt.CreateIndex(unique, name, node, typeName, properties);
    }

    private Stmt drop() {
        expect(Kind.DROP, "缺少 DROP");
        if (take(Kind.ALIAS)) {
            return new Stmt.DropAlias(ident());
        }
        if (take(Kind.INDEX)) {
            return new Stmt.DropIndex(ident());
        }
        expect(Kind.GRAPH, "只能 DROP GRAPH、DROP ALIAS 或 DROP INDEX");
        return new Stmt.DropGraph(ident());
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
        expect(Kind.ADD, "只能在末尾增加列");
        String property = ident();
        String valueType = typeName();
        expect(Kind.NULL, "只能在末尾增加可空列");
        return new Stmt.AlterType(node, typeName, property, valueType);
    }

    private Stmt insert() {
        expect(Kind.INSERT, "缺少 INSERT");
        Stmt.NodePat source = node();
        if (peek() == Kind.MINUS || peek() == Kind.LARROW) {
            return new Stmt.Insert(source, edge(), node());
        }
        return new Stmt.Insert(source, null, null);
    }

    private Stmt match() {
        expect(Kind.MATCH, "缺少 MATCH");
        Stmt.NodePat source = node();
        Stmt.EdgePat edge = null;
        Stmt.NodePat target = null;
        if (peek() == Kind.MINUS || peek() == Kind.LARROW) {
            edge = edge();
            target = node();
        }
        Expr where = take(Kind.WHERE) ? expr() : null;
        if (take(Kind.SET)) {
            return new Stmt.Set(source, edge, target, where, assignments());
        }
        if (take(Kind.DELETE)) {
            return new Stmt.Delete(source, edge, target, where, variables());
        }
        if (edge == null) {
            return fail("这里要写边、SET 或 DELETE");
        }
        expect(Kind.RETURN, "缺少 RETURN");
        boolean distinct = take(Kind.DISTINCT);
        List<Stmt.Item> items = items();
        List<Stmt.Sort> order = List.of();
        if (take(Kind.ORDER)) {
            expect(Kind.BY, "ORDER 后面要写 BY");
            order = sorts();
        }
        Long skip = take(Kind.SKIP) ? integer() : null;
        Long limit = take(Kind.LIMIT) ? integer() : null;
        return new Stmt.Match(source, edge, target, where, items, order, skip, limit, distinct);
    }

    private Stmt call() {
        expect(Kind.CALL, "缺少 CALL");
        String namespace = name();
        expect(Kind.DOT, "过程名要写成名字.名字");
        String procedure = name();
        expect(Kind.LP, "过程缺少左括号");
        expect(Kind.LC, "过程参数要写在花括号里");
        List<Stmt.Arg> args = new ArrayList<>();
        if (!take(Kind.RC)) {
            args.add(arg(args));
            while (take(Kind.COMMA)) {
                args.add(arg(args));
            }
            expect(Kind.RC, "过程参数缺少右花括号");
        }
        expect(Kind.RP, "过程缺少右括号");
        expect(Kind.YIELD, "过程调用要写 YIELD");
        List<String> columns = new ArrayList<>();
        columns.add(name());
        while (take(Kind.COMMA)) {
            columns.add(name());
        }
        return new Stmt.Call(namespace + "." + procedure, args, columns);
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
        return new Stmt.Field(name, valueType, take(Kind.NULL));
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
        if (peek() == Kind.IDENT) {
            variable = next().text;
        }
        String typeName = null;
        if (take(Kind.COLON)) {
            typeName = ident();
        }
        List<Stmt.Prop> props = props();
        expect(Kind.RP, "节点缺少右括号");
        return new Stmt.NodePat(variable, typeName, props);
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
                direction, undecided.variable(), undecided.typeName(), undecided.rank(), undecided.properties(),
                null, null), quantifier());
    }

    private int[] quantifier() {
        if (peek() == Kind.STAR) {
            next();
            return fail("不支持所有路径，请用最短路或给跳数加上边界");
        }
        if (!take(Kind.LC)) {
            return null;
        }
        long min = integer();
        Long max = null;
        if (take(Kind.COMMA)) {
            max = integer();
        }
        expect(Kind.RC, "跳数缺少右花括号");
        if (min > Integer.MAX_VALUE || (max != null && max > Integer.MAX_VALUE) || min < 0) {
            return fail("跳数超出范围");
        }
        int upper = max == null ? (int) min : max.intValue();
        return new int[] {(int) min, upper};
    }

    private static Stmt.EdgePat hops(Stmt.EdgePat edge, int[] range) {
        if (range == null) {
            return edge;
        }
        return new Stmt.EdgePat(
                edge.direction(), edge.variable(), edge.typeName(), edge.rank(), edge.properties(),
                range[0], range[1]);
    }

    private Stmt.EdgePat edgeBody(Stmt.EdgePat.Direction direction) {
        String variable = null;
        if (peek() == Kind.IDENT) {
            variable = next().text;
        }
        expect(Kind.COLON, "边缺少类型");
        String typeName = ident();
        Long rank = null;
        if (take(Kind.AT)) {
            rank = integer();
        }
        return new Stmt.EdgePat(direction, variable, typeName, rank, props(), null, null);
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
        Expr left = and();
        while (take(Kind.OR)) {
            left = new Expr.Binary("OR", left, and());
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
        if (kind == Kind.NOT && look(1) == Kind.IN) {
            next();
            return in(left, true);
        }
        if (peek() == Kind.IN) {
            return in(left, false);
        }
        if (take(Kind.IS)) {
            boolean negated = take(Kind.NOT);
            expect(Kind.NULL, "IS 后面要写 NULL");
            return new Expr.IsNull(left, negated);
        }
        return left;
    }

    private Expr in(Expr left, boolean negated) {
        expect(Kind.IN, "缺少 IN");
        expect(Kind.LP, "IN 缺少左括号");
        List<Expr> options = new ArrayList<>();
        options.add(expr());
        while (take(Kind.COMMA)) {
            options.add(expr());
        }
        expect(Kind.RP, "IN 缺少右括号");
        return new Expr.In(left, List.copyOf(options), negated);
    }

    private Expr add() {
        Expr left = mul();
        while (peek() == Kind.PLUS || peek() == Kind.MINUS) {
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
        if (take(Kind.LP)) {
            Expr inner = expr();
            expect(Kind.RP, "表达式缺少右括号");
            return inner;
        }
        if (take(Kind.DOLLAR)) {
            return new Expr.Param(ident());
        }
        if (take(Kind.DATETIME)) {
            if (peek() != Kind.STRING) {
                return fail("DATETIME 后面要写字符串");
            }
            return new Expr.Literal(new DateTimeText(next().text));
        }
        if (peek() == Kind.INTEGER) {
            return new Expr.Literal(integer());
        }
        if (peek() == Kind.REAL) {
            return new Expr.Literal(Double.valueOf(next().text));
        }
        if (peek() == Kind.STRING) {
            return new Expr.Literal(next().text);
        }
        if (take(Kind.TRUE)) {
            return new Expr.Literal(true);
        }
        if (take(Kind.FALSE)) {
            return new Expr.Literal(false);
        }
        if (take(Kind.NULL)) {
            return new Expr.Literal(null);
        }
        if (peek() == Kind.IDENT) {
            String name = next().text;
            if (take(Kind.LP)) {
                List<Expr> args = new ArrayList<>();
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
            if (take(Kind.DOT)) {
                return new Expr.Prop(name, ident());
            }
            return new Expr.Variable(name);
        }
        return fail("无法识别的表达式");
    }

    private String ident() {
        if (peek() != Kind.IDENT) {
            return fail("这里要写名字");
        }
        return next().text;
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
                int start = index;
                int startColumn = column;
                boolean real = current == '.';
                index++;
                column++;
                while (index < length && Character.isDigit(source.charAt(index))) {
                    index++;
                    column++;
                }
                if (!real && index < length && source.charAt(index) == '.') {
                    real = true;
                    index++;
                    column++;
                    while (index < length && Character.isDigit(source.charAt(index))) {
                        index++;
                        column++;
                    }
                }
                tokens.add(new Tok(real ? Kind.REAL : Kind.INTEGER, source.substring(start, index), start, index, line, startColumn));
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

    private static Tok symbol(String source, int index, int line, int column) {
        char current = source.charAt(index);
        char next = index + 1 < source.length() ? source.charAt(index + 1) : '\0';
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
            default -> null;
        };
        if (kind == null) {
            return null;
        }
        return new Tok(kind, String.valueOf(current), index, index + 1, line, column);
    }

    private enum Kind {
        IDENT, INTEGER, REAL, STRING,
        BOOL, BOOLEAN, INT, INT64, INTEGER_WORD, DOUBLE, DATETIME,
        CREATE, GRAPH, GRAPHS, USE, ALIAS, ALIASES, FOR, DROP, SHOW, NODE, REL, TYPES, TYPE,
        DESCRIBE, ALTER, ADD, NULL, INSERT, MATCH, WHERE, RETURN, ORDER, BY, LIMIT, SKIP,
        SET, DELETE, DISTINCT, CALL, YIELD, INDEX, INDEXES, UNIQUE, ON,
        AND, OR, NOT, IS, IN, AS, ASC, DESC, TRUE, FALSE,
        ARROW, LARROW, LE, GE, NEQ, EQ, LT, GT, PLUS, MINUS, STAR, SLASH,
        LP, RP, LB, RB, LC, RC, COMMA, COLON, SEMI, DOT, AT, DOLLAR, EOF
    }

    private record Tok(Kind kind, String text, int start, int end, int line, int column) {
    }
}
