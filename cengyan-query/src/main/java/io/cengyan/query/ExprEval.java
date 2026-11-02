package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.parser.DateTimeText;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Stmt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** 表达式求值。空值参与比较的结果是未知，WHERE 只保留真。 */
final class ExprEval {
    private final Map<String, Object> params;
    private final boolean literalsOnly;
    private Map<String, Slot> slots = Map.of();
    private Subquery subquery;

    void subquery(Subquery subquery) {
        this.subquery = subquery;
    }

    ExprEval(Map<String, Object> params, boolean literalsOnly) {
        this.params = params == null ? Map.of() : params;
        this.literalsOnly = literalsOnly;
    }

    void slots(Map<String, Slot> slots) {
        this.slots = slots == null ? Map.of() : slots;
    }

    Object eval(Expr expr) {
        return switch (expr) {
            case Expr.Literal literal -> literal(literal.value());
            case Expr.Param param -> param(param.name());
            case Expr.Variable variable -> {
                forbidReference();
                Slot slot = slot(variable.name());
                if (!slot.edge && slot.typeName == null && slot.whole != null && slot.whole.containsKey("\u0000")) {
                    yield slot.whole.get("\u0000");
                }
                yield slot.whole();
            }
            case Expr.Prop prop -> {
                forbidReference();
                yield field(prop.variable(), prop.field());
            }
            case Expr.Call call -> {
                forbidReference();
                yield call(call);
            }
            case Expr.Unary unary -> unary(unary.op(), eval(unary.inner()));
            case Expr.Binary binary -> binary(binary.op(), eval(binary.left()), eval(binary.right()));
            case Expr.In in -> in(eval(in.value()), in.options(), in.negated());
            case Expr.IsNull isNull -> {
                boolean missing = eval(isNull.value()) == null;
                yield isNull.negated() ? !missing : missing;
            }
            case Expr.Member member -> member(eval(member.value()), eval(member.collection()), member.negated());
            case Expr.ListLit list -> {
                List<Object> items = new ArrayList<>();
                for (Expr item : list.items()) {
                    items.add(eval(item));
                }
                yield List.copyOf(items);
            }
            case Expr.MapLit map -> {
                Map<String, Object> values = new LinkedHashMap<>();
                for (int i = 0; i < map.keys().size(); i++) {
                    values.put(map.keys().get(i), eval(map.values().get(i)));
                }
                yield values;
            }
            case Expr.Index index -> index(eval(index.value()), eval(index.start()), index.end() == null ? null : eval(index.end()));
            case Expr.Cast cast -> cast(eval(cast.value()), cast.type());
            case Expr.CaseExpr item -> caseOf(item);
            case Expr.Exists exists -> exists(exists.query());
            case Expr.Typed typed -> typed(eval(typed.value()), typed.type(), typed.negated());
            case Expr.Labeled labeled -> labeled(eval(labeled.value()), labeled.label(), labeled.negated());
            case Expr.EndpointOf endpoint -> endpoint(eval(endpoint.node()), eval(endpoint.edge()), endpoint.source(), endpoint.negated());
            case Expr.Same same -> same(same.values(), same.different());
            case Expr.PropExists prop -> propertyExists(eval(prop.value()), prop.property());
        };
    }

    boolean isTrue(Expr expr) {
        return Boolean.TRUE.equals(eval(expr));
    }

    boolean sameValue(Object left, Object right) {
        return Boolean.TRUE.equals(compare("=", left, right));
    }

    private Object literal(Object value) {
        if (value instanceof DateTimeText time) {
            return new UtcTime(DateTimes.parse(time.text()));
        }
        return value;
    }

    private Object param(String name) {
        if (!params.containsKey(name)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "缺少参数 " + name);
        }
        return params.get(name);
    }

    private void forbidReference() {
        if (literalsOnly) {
            throw new CengyanException(ErrorCode.SYNTAX, "插入值不能引用变量");
        }
    }

    private Object field(String variable, String name) {
        Slot slot = slot(variable);
        if (!slot.edge && slot.typeName == null && slot.whole != null && slot.whole.containsKey("\u0000")
                && slot.whole.get("\u0000") == null) {
            return null;
        }
        if (!slot.edge && "id".equals(name)) {
            return slot.externalId;
        }
        if (slot.edge && "rank".equals(name)) {
            return slot.rank;
        }
        if (slot.props == null || !slot.props.containsKey(name)) {
            return null;
        }
        return slot.props.get(name);
    }

    private Slot slot(String name) {
        Slot slot = slots.get(name);
        if (slot == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + name);
        }
        return slot;
    }

    private Object call(Expr.Call call) {
        String name = call.name().toLowerCase(java.util.Locale.ROOT);
        List<Expr> args = call.args();
        return switch (name) {
            case "id" -> {
                requireArgs(call, 1);
                Slot slot = slotOf(args.get(0));
                if (slot.edge) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "边没有 id");
                }
                yield slot.externalId;
            }
            case "labels" -> {
                requireArgs(call, 1);
                String typeName = slotOf(args.get(0)).typeName;
                yield typeName == null || typeName.isBlank() ? List.of() : List.of(typeName);
            }
            case "type" -> {
                requireArgs(call, 1);
                yield slotOf(args.get(0)).typeName;
            }
            case "properties" -> {
                requireArgs(call, 1);
                Slot slot = slotOf(args.get(0));
                yield slot.props == null ? Map.of() : slot.props;
            }
            case "startnode" -> nodeEnd(call, true);
            case "endnode" -> nodeEnd(call, false);
            case "coalesce" -> {
                if (args.isEmpty()) {
                    throw new CengyanException(ErrorCode.SYNTAX, "coalesce 至少要一个参数");
                }
                Object chosen = null;
                for (Expr arg : args) {
                    chosen = eval(arg);
                    if (chosen != null) {
                        break;
                    }
                }
                yield chosen;
            }
            case "size", "length" -> {
                requireArgs(call, 1);
                Object value = eval(args.get(0));
                if (value instanceof List<?> list) {
                    yield (long) list.size();
                }
                if (value instanceof Map<?, ?> map) {
                    yield (long) map.size();
                }
                if (value instanceof CharSequence text) {
                    String string = text.toString();
                    yield (long) string.codePointCount(0, string.length());
                }
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, name + " 需要列表、映射或字符串");
            }
            case "count", "sum", "avg", "min", "max", "collect_list", "stddev_samp", "stddev_pop",
                    "percentile_cont", "percentile_disc" ->
                    throw new CengyanException(ErrorCode.SYNTAX, "聚合只能写在 RETURN");
            default -> scalar(name, args);
        };
    }

    private Slot slotOf(Expr expr) {
        if (expr instanceof Expr.Variable variable) {
            return slot(variable.name());
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写变量");
    }

    private static void requireArgs(Expr.Call call, int count) {
        if (call.args().size() != count) {
            throw new CengyanException(ErrorCode.SYNTAX, call.name() + " 的参数个数不对");
        }
    }

    private static void requireEdge(Slot slot) {
        if (!slot.edge) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写边");
        }
    }

    private Object unary(String op, Object value) {
        if ("NOT".equals(op)) {
            if (value == null) {
                return null;
            }
            if (!(value instanceof Boolean bool)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "NOT 需要布尔值");
            }
            return !bool;
        }
        if (value == null) {
            return null;
        }
        if (value instanceof Long number) {
            try {
                return Math.negateExact(number);
            } catch (ArithmeticException ex) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围");
            }
        }
        if (value instanceof Double number) {
            return -number;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "负号需要数字");
    }

    private Object binary(String op, Object left, Object right) {
        if ("AND".equals(op) || "OR".equals(op) || "XOR".equals(op)) {
            return logic(op, left, right);
        }
        if ("||".equals(op)) {
            if (left == null || right == null) {
                return null;
            }
            return String.valueOf(left) + right;
        }
        if ("+".equals(op) || "-".equals(op) || "*".equals(op) || "/".equals(op) || "%".equals(op)) {
            return arithmetic(op, left, right);
        }
        return compare(op, left, right);
    }

    private Object logic(String op, Object left, Object right) {
        Boolean a = bool(left);
        Boolean b = bool(right);
        if ("AND".equals(op)) {
            if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
                return false;
            }
            if (a == null || b == null) {
                return null;
            }
            return true;
        }
        if ("XOR".equals(op)) {
            if (a == null || b == null) {
                return null;
            }
            return a ^ b;
        }
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
            return true;
        }
        if (a == null || b == null) {
            return null;
        }
        return false;
    }

    private Boolean bool(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "逻辑运算需要布尔值");
    }

    private Object arithmetic(String op, Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }
        boolean integral = isIntegral(left) && isIntegral(right);
        if (!integral && !(isNumber(left) && isNumber(right))) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "算术运算需要数字");
        }
        if ("/".equals(op)) {
            if (integral) {
                long divisor = toLong(right);
                long dividend = toLong(left);
                if (divisor == 0L) {
                    throw new CengyanException(ErrorCode.DIVIDE_BY_ZERO, "除数不能为 0");
                }
                if (dividend == Long.MIN_VALUE && divisor == -1L) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围");
                }
                return dividend / divisor;
            }
            double divisor = toDouble(right);
            if (divisor == 0.0d) {
                throw new CengyanException(ErrorCode.DIVIDE_BY_ZERO, "除数不能为 0");
            }
            return toDouble(left) / divisor;
        }
        if (integral) {
            long a = toLong(left);
            long b = toLong(right);
            try {
                return switch (op) {
                    case "+" -> Math.addExact(a, b);
                    case "-" -> Math.subtractExact(a, b);
                    case "*" -> Math.multiplyExact(a, b);
                    case "%" -> {
                        if (b == 0L) {
                            throw new CengyanException(ErrorCode.DIVIDE_BY_ZERO, "除数不能为 0");
                        }
                        yield Math.floorMod(a, b);
                    }
                    default -> throw new CengyanException(ErrorCode.SYNTAX, "未知运算符 " + op);
                };
            } catch (ArithmeticException ex) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围");
            }
        }
        double a = toDouble(left);
        double b = toDouble(right);
        return switch (op) {
            case "+" -> a + b;
            case "-" -> a - b;
            case "*" -> a * b;
            case "%" -> {
                if (b == 0.0d) {
                    throw new CengyanException(ErrorCode.DIVIDE_BY_ZERO, "除数不能为 0");
                }
                yield a % b;
            }
            default -> throw new CengyanException(ErrorCode.SYNTAX, "未知运算符 " + op);
        };
    }

    private Object in(Object value, List<Expr> options, boolean negated) {
        boolean unknown = false;
        for (Expr option : options) {
            Object compared = compare("=", value, eval(option));
            if (Boolean.TRUE.equals(compared)) {
                return !negated;
            }
            if (compared == null) {
                unknown = true;
            }
        }
        if (unknown) {
            return null;
        }
        return negated;
    }

    private Object compare(String op, Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }
        int order;
        if (left instanceof UtcTime || right instanceof UtcTime) {
            order = Long.compare(time(left), time(right));
        } else if (isNumber(left) && isNumber(right)) {
            if (left instanceof Double || right instanceof Double) {
                order = Double.compare(toDouble(left), toDouble(right));
            } else {
                order = Long.compare(toLong(left), toLong(right));
            }
        } else if (left instanceof String text && right instanceof String other) {
            order = Arrays.compareUnsigned(text.getBytes(StandardCharsets.UTF_8), other.getBytes(StandardCharsets.UTF_8));
        } else if (left instanceof Boolean && right instanceof Boolean && ("=".equals(op) || "<>".equals(op))) {
            order = Boolean.compare((Boolean) left, (Boolean) right);
        } else {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这两边的类型不能比较");
        }
        return switch (op) {
            case "=" -> order == 0;
            case "<>" -> order != 0;
            case "<" -> order < 0;
            case "<=" -> order <= 0;
            case ">" -> order > 0;
            case ">=" -> order >= 0;
            default -> throw new CengyanException(ErrorCode.SYNTAX, "未知运算符 " + op);
        };
    }

    private static long time(Object value) {
        if (value instanceof UtcTime time) {
            return time.millis();
        }
        if (value instanceof Long number) {
            return number;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "日期时间不能和这个值比较");
    }

    private static boolean isNumber(Object value) {
        return value instanceof Long || value instanceof Double || value instanceof Integer;
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Long || value instanceof Integer;
    }

    private static long toLong(Object value) {
        return ((Number) value).longValue();
    }

    private static double toDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    private Object nodeEnd(Expr.Call call, boolean source) {
        requireArgs(call, 1);
        Slot slot = slotOf(call.args().get(0));
        requireEdge(slot);
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", source ? slot.externalSrc : slot.externalDst);
        Object type = slot.whole == null ? null : slot.whole.get(source ? "srcType" : "dstType");
        node.put("type", type);
        node.put("properties", Map.of());
        return node;
    }

    private Object scalar(String name, List<Expr> args) {
        List<Object> values = new ArrayList<>();
        for (Expr arg : args) {
            values.add(eval(arg));
        }
        return switch (name) {
            case "char_length", "size", "length" -> lengthOf(values);
            case "upper" -> text(values, 1).toUpperCase(Locale.ROOT);
            case "lower" -> text(values, 1).toLowerCase(Locale.ROOT);
            case "trim", "btrim" -> text(values, 1).trim();
            case "ltrim" -> strip(text(values, 1), true);
            case "rtrim" -> strip(text(values, 1), false);
            case "left" -> side(values, true);
            case "right" -> side(values, false);
            case "abs" -> abs(number(values));
            case "floor" -> Math.floor(toDouble(number(values)));
            case "ceil" -> Math.ceil(toDouble(number(values)));
            case "sqrt" -> Math.sqrt(toDouble(number(values)));
            case "exp" -> Math.exp(toDouble(number(values)));
            case "ln", "log" -> Math.log(toDouble(number(values)));
            case "log10" -> Math.log10(toDouble(number(values)));
            case "power" -> Math.pow(toDouble(values.get(0)), toDouble(values.get(1)));
            case "mod" -> arithmetic("%", values.get(0), values.get(1));
            case "startswith", "starts_with" -> text(values, 2).startsWith(String.valueOf(values.get(1)));
            case "endswith", "ends_with" -> text(values, 2).endsWith(String.valueOf(values.get(1)));
            case "contains" -> text(values, 2).contains(String.valueOf(values.get(1)));
            case "regex_match" -> Pattern.compile(String.valueOf(values.get(1))).matcher(text(values, 2)).find();
            case "nodes" -> pathPart(values, "nodes");
            case "relationships" -> pathPart(values, "edges");
            case "path_length" -> pathPart(values, "length");
            case "element_id" -> elementId(values);
            case "coalesce" -> values.stream().filter(value -> value != null).findFirst().orElse(null);
            default -> throw new CengyanException(ErrorCode.SYNTAX, "未知函数 " + name);
        };
    }

    private Object lengthOf(List<Object> values) {
        if (values.size() != 1) {
            throw new CengyanException(ErrorCode.SYNTAX, "参数个数不对");
        }
        Object value = values.get(0);
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            return (long) list.size();
        }
        if (value instanceof Map<?, ?> map) {
            return (long) map.size();
        }
        if (value instanceof CharSequence text) {
            String string = text.toString();
            return (long) string.codePointCount(0, string.length());
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "size 需要列表、映射或字符串");
    }

    private static String text(List<Object> values, int count) {
        if (values.size() != count || !(values.get(0) instanceof String text)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写字符串");
        }
        return text;
    }

    private static String strip(String text, boolean left) {
        int start = 0;
        int end = text.length();
        if (left) {
            while (start < end && Character.isWhitespace(text.charAt(start))) {
                start++;
            }
        } else {
            while (end > start && Character.isWhitespace(text.charAt(end - 1))) {
                end--;
            }
        }
        return text.substring(start, end);
    }

    private static String side(List<Object> values, boolean left) {
        String text = text(values, 2);
        int count = (int) toLong(values.get(1));
        if (count < 0) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "长度不能为负");
        }
        if (count >= text.length()) {
            return text;
        }
        return left ? text.substring(0, count) : text.substring(text.length() - count);
    }

    private static Object abs(Object value) {
        if (value instanceof Long number) {
            return number == Long.MIN_VALUE
                    ? throwRange()
                    : Math.abs(number);
        }
        return Math.abs(toDouble(value));
    }

    private static Object throwRange() {
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "数字超出范围");
    }

    private static Object number(List<Object> values) {
        if (values.size() != 1 || values.get(0) == null || !isNumber(values.get(0))) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写数字");
        }
        return values.get(0);
    }

    private static Object pathPart(List<Object> values, String key) {
        if (values.size() != 1 || !(values.get(0) instanceof Map<?, ?> path)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写路径");
        }
        return path.get(key);
    }

    private static Object elementId(List<Object> values) {
        if (values.size() != 1 || !(values.get(0) instanceof Map<?, ?> body)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写点或边");
        }
        if (body.containsKey("id")) {
            return body.get("id");
        }
        return body.get("type") + ":" + body.get("src") + ":" + body.get("dst") + ":" + body.get("rank");
    }

    private Object member(Object value, Object collection, boolean negated) {
        if (value == null || collection == null) {
            return null;
        }
        if (!(collection instanceof List<?> list)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "IN 的右边要是列表");
        }
        boolean found = false;
        for (Object item : list) {
            if (Boolean.TRUE.equals(compare("=", value, item))) {
                found = true;
                break;
            }
        }
        return negated ? !found : found;
    }

    private Object index(Object value, Object start, Object end) {
        if (value == null || start == null) {
            return null;
        }
        int from = (int) toLong(start);
        if (value instanceof List<?> list) {
            if (end == null) {
                return from < 0 || from >= list.size() ? null : list.get(from);
            }
            int to = (int) toLong(end);
            int lo = Math.max(0, from);
            int hi = Math.min(list.size(), to);
            return lo >= hi ? List.of() : List.copyOf(list.subList(lo, hi));
        }
        if (value instanceof String text) {
            if (end == null) {
                return from < 0 || from >= text.length() ? null : text.substring(from, from + 1);
            }
            int to = (int) toLong(end);
            int lo = Math.max(0, from);
            int hi = Math.min(text.length(), to);
            return lo >= hi ? "" : text.substring(lo, hi);
        }
        if (value instanceof Map<?, ?> map && end == null) {
            return map.get(String.valueOf(start));
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这个值不能取下标");
    }

    private Object cast(Object value, String type) {
        if (value == null) {
            return null;
        }
        String kind = type.toUpperCase(Locale.ROOT);
        try {
            return switch (kind) {
                case "INT", "INT64", "INTEGER" -> value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
                case "DOUBLE" -> value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value));
                case "STRING" -> String.valueOf(value instanceof UtcTime time ? DateTimes.plain(time) : value);
                case "BOOL", "BOOLEAN" -> value instanceof Boolean bool ? bool : Boolean.parseBoolean(String.valueOf(value));
                case "DATETIME" -> value instanceof UtcTime time ? time : new UtcTime(DateTimes.parse(String.valueOf(value)));
                default -> throw new CengyanException(ErrorCode.TYPE_MISMATCH, "不能转换成 " + type);
            };
        } catch (RuntimeException ex) {
            if (ex instanceof CengyanException cengyan) {
                throw cengyan;
            }
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "不能转换成 " + type);
        }
    }

    private Object caseOf(Expr.CaseExpr item) {
        if (item.scrutinee() == null) {
            for (int i = 0; i < item.whens().size(); i++) {
                if (Boolean.TRUE.equals(eval(item.whens().get(i)))) {
                    return eval(item.thens().get(i));
                }
            }
            return item.otherwise() == null ? null : eval(item.otherwise());
        }
        Object value = eval(item.scrutinee());
        for (int i = 0; i < item.whens().size(); i++) {
            if (Boolean.TRUE.equals(compare("=", value, eval(item.whens().get(i))))) {
                return eval(item.thens().get(i));
            }
        }
        return item.otherwise() == null ? null : eval(item.otherwise());
    }

    private Object exists(Stmt query) {
        if (subquery == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "这里还不能写子查询");
        }
        return subquery.exists(query, slots);
    }

    private Object typed(Object value, String type, boolean negated) {
        if (value == null) {
            return null;
        }
        boolean hit = switch (type.toUpperCase(Locale.ROOT)) {
            case "INT", "INT64", "INTEGER" -> isIntegral(value);
            case "DOUBLE" -> value instanceof Double;
            case "STRING" -> value instanceof String;
            case "BOOL", "BOOLEAN" -> value instanceof Boolean;
            case "DATETIME" -> value instanceof UtcTime;
            default -> false;
        };
        return negated ? !hit : hit;
    }

    private Object labeled(Object value, Stmt.Label label, boolean negated) {
        String type = value instanceof Map<?, ?> map && map.get("type") instanceof String text ? text : null;
        boolean hit = type != null && labelHit(label, type);
        return negated ? !hit : hit;
    }

    private static boolean labelHit(Stmt.Label label, String type) {
        return switch (label) {
            case Stmt.Label.One one -> one.name().equals(type);
            case Stmt.Label.Any ignored -> true;
            case Stmt.Label.Not not -> !labelHit(not.inner(), type);
            case Stmt.Label.Or or -> or.options().stream().anyMatch(option -> labelHit(option, type));
            case Stmt.Label.And and -> and.options().stream().allMatch(option -> labelHit(option, type));
        };
    }

    private Object endpoint(Object node, Object edge, boolean source, boolean negated) {
        if (!(node instanceof Map<?, ?> left) || !(edge instanceof Map<?, ?> right)) {
            throw new CengyanException(ErrorCode.TYPE_MISMATCH, "这里要写点和边");
        }
        Object id = left.get("id");
        Object end = right.get(source ? "src" : "dst");
        boolean hit = id != null && id.equals(end);
        return negated ? !hit : hit;
    }

    private Object same(List<Expr> values, boolean different) {
        List<Object> resolved = new ArrayList<>();
        for (Expr value : values) {
            resolved.add(eval(value));
        }
        for (int i = 0; i < resolved.size(); i++) {
            for (int j = i + 1; j < resolved.size(); j++) {
                boolean equal = Boolean.TRUE.equals(compare("=", resolved.get(i), resolved.get(j)));
                if (different && equal) {
                    return false;
                }
                if (!different && !equal) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Object propertyExists(Object value, String property) {
        if (!(value instanceof Map<?, ?> map)) {
            return false;
        }
        Object properties = map.get("properties");
        if (properties instanceof Map<?, ?> nested) {
            return nested.containsKey(property);
        }
        return map.containsKey(property);
    }

    @FunctionalInterface
    interface Subquery {
        boolean exists(Stmt query, Map<String, Slot> outer);
    }

    static final class Slot {
        final boolean edge;
        final long vid;
        final Object externalId;
        final String typeName;
        final Long rank;
        final Long src;
        final Long dst;
        final Object externalSrc;
        final Object externalDst;
        final Map<String, Object> props;
        final Map<String, Object> whole;

        private Slot(
                boolean edge,
                long vid,
                Object externalId,
                String typeName,
                Long rank,
                Long src,
                Long dst,
                Object externalSrc,
                Object externalDst,
                Map<String, Object> props,
                Map<String, Object> whole) {
            this.edge = edge;
            this.vid = vid;
            this.externalId = externalId;
            this.typeName = typeName;
            this.rank = rank;
            this.src = src;
            this.dst = dst;
            this.externalSrc = externalSrc;
            this.externalDst = externalDst;
            this.props = props;
            this.whole = whole;
        }

        static Slot node(long vid, String typeName, Map<String, Object> props) {
            return node(vid, vid, typeName, props);
        }

        static Slot node(long vid, Object externalId, String typeName, Map<String, Object> props) {
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("id", externalId);
            body.put("type", typeName);
            body.put("properties", props == null ? Map.of() : props);
            return new Slot(false, vid, externalId, typeName, null, null, null, null, null, props, body);
        }

        /** 整条路径。{@code body} 含 length、nodes、edges。 */
        static Slot scalar(Object value) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("\u0000", value);
            return new Slot(false, 0L, value, null, null, null, null, null, null, Map.of(), body);
        }

        static Slot path(Map<String, Object> body) {
            return new Slot(false, 0L, null, null, null, null, null, null, null, null, body);
        }

        static Slot edge(String typeName, long rank, long src, long dst, Map<String, Object> props) {
            return edge(typeName, rank, src, dst, props, null, null);
        }

        static Slot edge(
                String typeName,
                long rank,
                long src,
                long dst,
                Map<String, Object> props,
                String srcType,
                String dstType) {
            return edge(typeName, rank, src, dst, src, dst, props, srcType, dstType);
        }

        static Slot edge(
                String typeName,
                long rank,
                long src,
                long dst,
                Object externalSrc,
                Object externalDst,
                Map<String, Object> props,
                String srcType,
                String dstType) {
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("type", typeName);
            body.put("rank", rank);
            body.put("src", externalSrc);
            body.put("dst", externalDst);
            if (srcType != null && !srcType.isBlank()) {
                body.put("srcType", srcType);
            }
            if (dstType != null && !dstType.isBlank()) {
                body.put("dstType", dstType);
            }
            body.put("properties", props == null ? Map.of() : props);
            return new Slot(true, 0L, null, typeName, rank, src, dst, externalSrc, externalDst, props, body);
        }

        Object whole() {
            return whole;
        }
    }
}
