package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.IndexCodec;
import io.cengyan.catalog.IndexDef;
import io.cengyan.catalog.PropDef;
import io.cengyan.catalog.ValueType;
import io.cengyan.parser.Expr;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单跳查询里，能覆盖前缀的就绪索引改走范围扫描。
 * 用不上时返回 null，调用方继续原来的扫描。
 */
final class IndexLookup {
    private IndexLookup() {
    }

    static List<Long> vertexIds(
            List<IndexDef> indexes,
            ReadSession session,
            int graphId,
            int typeId,
            List<PropDef> properties,
            String variable,
            Expr where,
            ExprEval eval,
            CancelToken cancel) {
        Plan plan = plan(indexes, true, typeId, properties, variable, where, eval);
        if (plan == null) {
            return null;
        }
        if (plan.empty()) {
            return List.of();
        }
        List<Long> ids = new ArrayList<>();
        int seen = 0;
        for (byte[] key : session.scanIndex(plan.start(), plan.end(), -1, cancel)) {
            if ((seen++ & 63) == 0 && cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            if (key.length != plan.identityOffset() + KeyCodec.VERTEX_IDENTITY_LENGTH) {
                continue;
            }
            if (graphAt(key, plan.identityOffset()) != graphId) {
                continue;
            }
            ids.add(KeyCodec.identityVid(key, plan.identityOffset()));
        }
        return ids;
    }

    static List<EdgeRef> edges(
            List<IndexDef> indexes,
            ReadSession session,
            int graphId,
            int typeId,
            List<PropDef> properties,
            String variable,
            Expr where,
            ExprEval eval,
            CancelToken cancel) {
        Plan plan = plan(indexes, false, typeId, properties, variable, where, eval);
        if (plan == null) {
            return null;
        }
        if (plan.empty()) {
            return List.of();
        }
        List<EdgeRef> edges = new ArrayList<>();
        int seen = 0;
        for (byte[] key : session.scanIndex(plan.start(), plan.end(), -1, cancel)) {
            if ((seen++ & 63) == 0 && cancel != null && cancel.cancelled()) {
                throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
            }
            if (key.length != plan.identityOffset() + KeyCodec.EDGE_IDENTITY_LENGTH) {
                continue;
            }
            if (graphAt(key, plan.identityOffset()) != graphId) {
                continue;
            }
            edges.add(KeyCodec.identityEdge(key, plan.identityOffset()));
        }
        return edges;
    }

    private static Plan plan(
            List<IndexDef> indexes,
            boolean node,
            int typeId,
            List<PropDef> properties,
            String variable,
            Expr where,
            ExprEval eval) {
        if (indexes == null || indexes.isEmpty() || properties == null || variable == null || where == null) {
            return null;
        }
        Map<String, Bound> bounds = new HashMap<>();
        collect(where, variable, eval, bounds);
        if (bounds.isEmpty()) {
            return null;
        }
        Plan best = null;
        for (IndexDef index : indexes) {
            if (!index.ready() || index.node() != node || index.typeId() != typeId) {
                continue;
            }
            Plan plan = cover(index, properties, bounds);
            if (plan == null) {
                continue;
            }
            if (best == null || plan.used() > best.used() || (plan.used() == best.used() && index.id() < best.indexId())) {
                best = plan;
            }
        }
        return best;
    }

    private static void collect(Expr expr, String variable, ExprEval eval, Map<String, Bound> bounds) {
        if (expr instanceof Expr.Binary binary && "AND".equals(binary.op())) {
            collect(binary.left(), variable, eval, bounds);
            collect(binary.right(), variable, eval, bounds);
            return;
        }
        if (!(expr instanceof Expr.Binary binary) || !comparison(binary.op())) {
            return;
        }
        boolean propOnLeft;
        Expr.Prop prop;
        Expr other;
        if (binary.left() instanceof Expr.Prop left && constant(binary.right())) {
            prop = left;
            other = binary.right();
            propOnLeft = true;
        } else if (binary.right() instanceof Expr.Prop right && constant(binary.left())) {
            prop = right;
            other = binary.left();
            propOnLeft = false;
        } else {
            return;
        }
        if (!variable.equals(prop.variable())) {
            return;
        }
        Object value = eval.eval(other);
        if (value instanceof UtcTime time) {
            value = time.millis();
        }
        if (value == null) {
            return;
        }
        add(bounds, prop.field(), propOnLeft ? binary.op() : flip(binary.op()), value);
    }

    private static void add(Map<String, Bound> bounds, String field, String op, Object value) {
        Bound bound = bounds.computeIfAbsent(field, name -> new Bound());
        if (bound.complex) {
            return;
        }
        switch (op) {
            case "=" -> {
                if (bound.eq != null || bound.lower != null || bound.upper != null) {
                    bound.complex = true;
                } else {
                    bound.eq = value;
                }
            }
            case "<", "<=" -> {
                if (bound.eq != null || bound.upper != null) {
                    bound.complex = true;
                } else {
                    bound.upper = value;
                    bound.upperInclusive = "<=".equals(op);
                }
            }
            case ">", ">=" -> {
                if (bound.eq != null || bound.lower != null) {
                    bound.complex = true;
                } else {
                    bound.lower = value;
                    bound.lowerInclusive = ">=".equals(op);
                }
            }
            default -> bound.complex = true;
        }
    }

    private static Plan cover(IndexDef index, List<PropDef> definitions, Map<String, Bound> bounds) {
        byte[] equality = new byte[0];
        int used = 0;
        for (int position : index.propertyIndexes()) {
            if (position < 0 || position >= definitions.size()) {
                return null;
            }
            PropDef property = definitions.get(position);
            Bound bound = bounds.get(property.name());
            if (bound == null || bound.complex || bound.eq == null) {
                break;
            }
            Object value = coerce(property.type(), bound.eq);
            if (value == null) {
                return null;
            }
            equality = concat(equality, IndexCodec.encode(property.type(), value));
            used++;
        }
        ValueType rangeType = null;
        Object lower = null;
        Object upper = null;
        boolean lowerInclusive = false;
        boolean upperInclusive = false;
        if (used < index.propertyIndexes().size()) {
            int position = index.propertyIndexes().get(used);
            if (position >= 0 && position < definitions.size()) {
                PropDef property = definitions.get(position);
                Bound bound = bounds.get(property.name());
                if (bound != null && !bound.complex && bound.eq == null
                        && (bound.lower != null || bound.upper != null)
                        && IndexCodec.rangeable(property.type())) {
                    Object low = bound.lower == null ? null : coerce(property.type(), bound.lower);
                    Object high = bound.upper == null ? null : coerce(property.type(), bound.upper);
                    boolean lowOk = bound.lower == null || low != null;
                    boolean highOk = bound.upper == null || high != null;
                    if (lowOk && highOk) {
                        rangeType = property.type();
                        lower = low;
                        upper = high;
                        lowerInclusive = bound.lowerInclusive;
                        upperInclusive = bound.upperInclusive;
                        used++;
                    }
                }
            }
        }
        if (used == 0) {
            return null;
        }
        int rangeWidth = rangeType == null ? 0 : IndexCodec.encodedWidth(rangeType);
        int identityOffset = 4 + equality.length + rangeWidth + 2;
        byte[] start;
        byte[] end;
        if (rangeType == null) {
            start = KeyCodec.indexSeek(index.id(), equality);
            end = KeyCodec.upperBound(start);
        } else if (lower != null) {
            byte[] lowKey = KeyCodec.indexSeek(index.id(), concat(equality, IndexCodec.encode(rangeType, lower)));
            start = lowerInclusive ? lowKey : KeyCodec.upperBound(lowKey);
            if (start == null) {
                return Plan.empty(index.id(), used);
            }
            end = upperEnd(index.id(), equality, rangeType, upper, upperInclusive);
        } else {
            start = KeyCodec.indexSeek(index.id(), equality);
            end = upperEnd(index.id(), equality, rangeType, upper, upperInclusive);
        }
        if (end != null && Arrays.compareUnsigned(start, end) >= 0) {
            return Plan.empty(index.id(), used);
        }
        return new Plan(index.id(), used, start, end, identityOffset, false);
    }

    private static byte[] upperEnd(
            int indexId, byte[] equality, ValueType rangeType, Object upper, boolean upperInclusive) {
        if (upper == null) {
            return KeyCodec.upperBound(KeyCodec.indexSeek(indexId, equality));
        }
        byte[] highKey = KeyCodec.indexSeek(indexId, concat(equality, IndexCodec.encode(rangeType, upper)));
        return upperInclusive ? KeyCodec.upperBound(highKey) : highKey;
    }

    private static Object coerce(ValueType type, Object value) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case INT64 -> value instanceof Long number ? number : null;
            case DOUBLE -> {
                if (value instanceof Double number) {
                    yield Double.isFinite(number) ? number : null;
                }
                if (value instanceof Long number) {
                    yield number.doubleValue();
                }
                yield null;
            }
            case STRING -> value instanceof String text ? text : null;
            case BOOL -> value instanceof Boolean bit ? bit : null;
            case DATETIME -> value instanceof Long number ? number : null;
        };
    }

    private static boolean constant(Expr expr) {
        return switch (expr) {
            case Expr.Literal ignored -> true;
            case Expr.Param ignored -> true;
            case Expr.Unary unary -> constant(unary.inner());
            case Expr.Binary binary -> constant(binary.left()) && constant(binary.right());
            default -> false;
        };
    }

    private static boolean comparison(String op) {
        return "=".equals(op) || "<>".equals(op) || "<".equals(op) || ">".equals(op)
                || "<=".equals(op) || ">=".equals(op);
    }

    private static String flip(String op) {
        return switch (op) {
            case "<" -> ">";
            case "<=" -> ">=";
            case ">" -> "<";
            case ">=" -> "<=";
            default -> op;
        };
    }

    private static byte[] concat(byte[] left, byte[] right) {
        byte[] joined = Arrays.copyOf(left, left.length + right.length);
        System.arraycopy(right, 0, joined, left.length, right.length);
        return joined;
    }

    private static int graphAt(byte[] key, int identityOffset) {
        int at = identityOffset - 2;
        return ((key[at] & 0xFF) << 8) | (key[at + 1] & 0xFF);
    }

    private static final class Bound {
        private Object eq;
        private Object lower;
        private boolean lowerInclusive;
        private Object upper;
        private boolean upperInclusive;
        private boolean complex;
    }

    private record Plan(int indexId, int used, byte[] start, byte[] end, int identityOffset, boolean empty) {
        private static Plan empty(int indexId, int used) {
            return new Plan(indexId, used, null, null, 0, true);
        }
    }
}
