package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.parser.DateTimeText;
import io.cengyan.parser.Expr;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** 表达式求值。空值参与比较的结果是未知，WHERE 只保留真。 */
final class ExprEval {
    private final Map<String, Object> params;
    private final boolean literalsOnly;
    private Map<String, Slot> slots = Map.of();

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
                yield slot(variable.name()).whole();
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
            case "labels", "type" -> {
                requireArgs(call, 1);
                yield slotOf(args.get(0)).typeName;
            }
            case "properties" -> {
                requireArgs(call, 1);
                Slot slot = slotOf(args.get(0));
                yield slot.props == null ? Map.of() : slot.props;
            }
            case "startnode" -> {
                requireArgs(call, 1);
                Slot slot = slotOf(args.get(0));
                requireEdge(slot);
                yield slot.externalSrc;
            }
            case "endnode" -> {
                requireArgs(call, 1);
                Slot slot = slotOf(args.get(0));
                requireEdge(slot);
                yield slot.externalDst;
            }
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
            case "count", "sum" -> throw new CengyanException(ErrorCode.SYNTAX, "聚合只能写在 RETURN");
            default -> throw new CengyanException(ErrorCode.SYNTAX, "未知函数 " + call.name());
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
        if ("AND".equals(op) || "OR".equals(op)) {
            return logic(op, left, right);
        }
        if ("+".equals(op) || "-".equals(op) || "*".equals(op) || "/".equals(op)) {
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
