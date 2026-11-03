package io.cengyan.query;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.Mutation;
import io.cengyan.catalog.NodeType;
import io.cengyan.catalog.RelType;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Stmt;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphStore;
import io.cengyan.store.ReadSession;
import io.cengyan.store.VertexId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 多段模式、OPTIONAL、标签表达式、线性查询和集合运算。
 * 排序、聚合和集合运算交给 DuckDB。
 */
final class GraphQuery {
    private static final int SPILL_ROWS = 100_000;

    private final Catalog catalog;
    private final GraphStore store;
    private final DuckSettings duck;
    private final ProcedureExecutor procedures;

    GraphQuery(Catalog catalog, GraphStore store, DuckSettings duck, ProcedureExecutor procedures) {
        this.catalog = catalog;
        this.store = store;
        this.duck = duck;
        this.procedures = procedures;
    }

    QueryResult explain(Stmt statement) {
        List<String> lines = new ArrayList<>();
        describe(statement, lines, 0);
        List<List<Object>> rows = new ArrayList<>();
        for (String line : lines) {
            rows.add(List.of(line));
        }
        return new QueryResult(List.of("plan"), rows, 0);
    }

    QueryResult runMatch(Stmt.Match match, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        List<Stmt.Element> elements = new ArrayList<>();
        elements.add(new Stmt.Element.Node(match.source()));
        if (match.edge() != null) {
            elements.add(new Stmt.Element.Rel(match.edge()));
            elements.add(new Stmt.Element.Node(match.target()));
        }
        Stmt.Clause.Match clause = new Stmt.Clause.Match(
                false, match.pathVariable(), null, null, null, List.of(new Stmt.Pattern(elements)), match.where());
        Stmt.Tail tail = new Stmt.Tail(
                match.distinct(), false, match.items(), List.of(), match.orderBy(), match.skip(), match.limit());
        return select(new Stmt.Select(List.of(clause), tail), graphId, params, limits, cancel);
    }

    QueryResult select(Stmt.Select select, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        List<Row> rows = runClauses(select.clauses(), List.of(Row.empty()), graphId, params, limits, cancel);
        QueryMemory.current().observeRows(rows.size(), select.tail().items().size());
        return finish(select.tail(), rows, graphId, params, cancel);
    }

    QueryResult composite(Stmt.Composite composite, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        QueryResult left = run(composite.left(), graphId, params, limits, cancel);
        QueryResult right = run(composite.right(), graphId, params, limits, cancel);
        if (!left.columns().equals(right.columns())) {
            throw new CengyanException(ErrorCode.SYNTAX, "集合运算两边的列不一致");
        }
        List<List<Object>> rows = new ArrayList<>(left.rows());
        rows.addAll(right.rows());
        QueryMemory.current().observeRows(rows);
        if (composite.all() && "UNION".equals(composite.op())) {
            return new QueryResult(left.columns(), rows, 0);
        }
        List<List<Object>> distinct = DuckResult.arrange(rows, List.of(), List.of(), true, 0L, null, duck, cancel);
        if ("UNION".equals(composite.op())) {
            return new QueryResult(left.columns(), distinct, 0);
        }
        Set<List<Object>> rightSet = new LinkedHashSet<>(right.rows());
        Set<List<Object>> leftSet = new LinkedHashSet<>(left.rows());
        List<List<Object>> kept = new ArrayList<>();
        if ("EXCEPT".equals(composite.op())) {
            for (List<Object> row : distinct) {
                if (!rightSet.contains(row)) {
                    kept.add(row);
                }
            }
        } else {
            for (List<Object> row : distinct) {
                if (rightSet.contains(row) && leftSet.contains(row)) {
                    kept.add(row);
                }
            }
        }
        return new QueryResult(left.columns(), kept, 0);
    }

    QueryResult next(Stmt.Next next, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        QueryResult left = run(next.left(), graphId, params, limits, cancel);
        List<Row> incoming = new ArrayList<>();
        for (List<Object> row : left.rows()) {
            Row bound = Row.empty();
            for (int i = 0; i < left.columns().size(); i++) {
                bound.values.put(left.columns().get(i), row.get(i));
            }
            incoming.add(bound);
        }
        if (incoming.size() > SPILL_ROWS) {
            incoming = List.copyOf(incoming.subList(0, incoming.size()));
        }
        if (next.right() instanceof Stmt.Select select) {
            List<Row> rows = runClauses(select.clauses(), incoming, graphId, params, limits, cancel);
            return finish(select.tail(), rows, graphId, params, cancel);
        }
        List<List<Object>> rows = new ArrayList<>();
        List<String> columns = List.of();
        for (Row row : incoming) {
            Map<String, Object> scoped = new LinkedHashMap<>(params);
            scoped.putAll(row.values);
            QueryResult part = run(next.right(), graphId, scoped, limits, cancel);
            columns = part.columns();
            rows.addAll(part.rows());
        }
        return new QueryResult(columns, rows, 0);
    }


    int insertGraph(List<Stmt.Element> elements, int graphId, Map<String, Object> params, Map<String, Long> bound) {
        return writePattern(elements, graphId, params, bound);
    }

    int insertUsing(Stmt.InsertUsing insert, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        List<Row> rows = runClauses(insert.clauses(), List.of(Row.empty()), graphId, params, limits, cancel);
        int written = 0;
        for (Row row : rows) {
            written += writePattern(insert.elements(), graphId, params, row.vids);
        }
        return written;
    }

    int mutate(Stmt.Mutate mutate, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        List<Row> rows = runClauses(mutate.clauses(), List.of(Row.empty()), graphId, params, limits, cancel);
        List<Mutation> mutations = new ArrayList<>();
        ExprEval eval = new ExprEval(params, false);
        for (Row row : rows) {
            eval.slots(slots(graphId, row));
            for (Stmt.Assign assignment : mutate.assignments()) {
                mutations.add(assignment(graphId, row, assignment, eval));
            }
            for (Stmt.Assign replacement : mutate.replacements()) {
                Object value = eval.eval(replacement.value());
                if (!(value instanceof Map<?, ?> map)) {
                    throw new CengyanException(ErrorCode.TYPE_MISMATCH, "整体赋值的右边要是映射");
                }
                Map<String, Object> values = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    values.put(String.valueOf(entry.getKey()), entry.getValue());
                }
                mutations.add(setAll(graphId, row, replacement.variable(), values));
            }
            for (String removal : mutate.removals()) {
                String[] parts = removal.split("\\.", 2);
                if (parts.length != 2) {
                    throw new CengyanException(ErrorCode.SYNTAX, "REMOVE 要写成 变量.属性");
                }
                mutations.add(assignment(graphId, row, new Stmt.Assign(parts[0], parts[1], new Expr.Literal(null)), eval));
            }
            for (String variable : mutate.deletes()) {
                if (row.edgeRefs.containsKey(variable)) {
                    EdgeRef edge = row.edgeRefs.get(variable);
                    RelType type = relById(graphId, edge.edgeTypeId());
                    mutations.add(new Mutation.DeleteEdge(type.name(), edge.rank(), edge.outSrc(), edge.outDst()));
                } else if (row.vids.containsKey(variable)) {
                    mutations.add(new Mutation.DeleteVertex(row.vids.get(variable), mutate.detach()));
                } else {
                    throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
                }
            }
        }
        return mutations.isEmpty() ? 0 : catalog.apply(graphId, mutations, limits.maxEdgesPerVertex());
    }

    void dropType(int graphId, Stmt.DropType drop) {
        catalog.dropType(graphId, drop.node(), drop.name(), drop.ifExists());
    }

    void dropProperty(int graphId, boolean node, String typeName, String property) {
        catalog.dropProperty(graphId, node, typeName, property);
    }

    void renameProperty(int graphId, boolean node, String typeName, String from, String to) {
        catalog.renameProperty(graphId, node, typeName, from, to);
    }

    private QueryResult run(Stmt statement, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        return switch (statement) {
            case Stmt.Select select -> select(select, graphId, params, limits, cancel);
            case Stmt.Composite composite -> composite(composite, graphId, params, limits, cancel);
            case Stmt.Next next -> next(next, graphId, params, limits, cancel);
            case Stmt.Match match -> new MatchExecutor(catalog, store, duck).run(match, graphId, params, limits, cancel);
            case Stmt.Call call -> procedures.call(call, graphId, params, limits, cancel);
            default -> throw new CengyanException(ErrorCode.SYNTAX, "这里不能写这种语句");
        };
    }

    private List<Row> runClauses(
            List<Stmt.Clause> clauses,
            List<Row> incoming,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        List<Row> rows = incoming.isEmpty() ? List.of(Row.empty()) : incoming;
        try (ReadSession session = store.read()) {
            for (Stmt.Clause clause : clauses) {
                cancelled(cancel);
                rows = switch (clause) {
                    case Stmt.Clause.Match match -> matchClause(match, rows, session, graphId, params, limits, cancel);
                    case Stmt.Clause.Filter filter -> filter(filter.expr(), rows, graphId, params);
                    case Stmt.Clause.Let let -> let(let, rows, graphId, params);
                    case Stmt.Clause.For each -> unwind(each, rows, graphId, params);
                    case Stmt.Clause.Call call -> call(call.call(), rows, graphId, params, limits, cancel);
                };
                QueryMemory.current().observeRows(rows.size(), rows.isEmpty() ? 1 : rows.get(0).values.size());
                if (rows.size() > SPILL_ROWS * 10) {
                    throw new CengyanException(ErrorCode.QUERY_MEMORY_EXCEEDED, "中间结果超过 " + (SPILL_ROWS * 10) + " 行");
                }
            }
        }
        return rows;
    }

    private List<Row> matchClause(
            Stmt.Clause.Match match,
            List<Row> rows,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        List<Row> matched = new ArrayList<>();
        for (Row row : rows) {
            List<Row> acc = List.of(row);
            for (Stmt.Pattern pattern : match.patterns()) {
                List<Row> next = new ArrayList<>();
                for (Row left : acc) {
                    next.addAll(matchPattern(pattern, match, left, session, graphId, params, limits, cancel));
                }
                acc = next;
            }
            if (match.where() != null) {
                acc = filter(match.where(), acc, graphId, params);
            }
            if (acc.isEmpty() && match.optional()) {
                Row copy = row.copy();
                for (String name : introduced(match)) {
                    copy.values.putIfAbsent(name, null);
                }
                matched.add(copy);
            } else {
                matched.addAll(acc);
            }
        }
        return matched;
    }

    private static List<String> introduced(Stmt.Clause.Match match) {
        List<String> names = new ArrayList<>();
        for (Stmt.Pattern pattern : match.patterns()) {
            for (Stmt.Element element : pattern.elements()) {
                if (element instanceof Stmt.Element.Node node && node.node().variable() != null) {
                    names.add(node.node().variable());
                }
                if (element instanceof Stmt.Element.Rel rel && rel.edge().variable() != null) {
                    names.add(rel.edge().variable());
                }
            }
        }
        return names;
    }

    private List<Row> matchPattern(
            Stmt.Pattern pattern,
            Stmt.Clause.Match match,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        Row start = row.copy();
        start.candidates = sourceIdCandidates(match, graphId, params);
        List<Row> current = List.of(start);
        String previous = null;
        for (Stmt.Element element : pattern.elements()) {
            List<Row> next = new ArrayList<>();
            for (Row left : current) {
                switch (element) {
                    case Stmt.Element.Node node -> next.addAll(bindNode(node.node(), left, session, graphId, params, limits, cancel));
                    case Stmt.Element.Rel rel -> next.addAll(bindEdge(rel.edge(), match, previous, left, session, graphId, params, limits, cancel));
                    case Stmt.Element.Group group -> next.addAll(bindGroup(group, match, left, session, graphId, params, limits, cancel));
                }
            }
            current = next;
            if (element instanceof Stmt.Element.Node node && node.node().variable() != null) {
                previous = node.node().variable();
            }
        }
        if (match.pathVariable() != null) {
            for (Row bound : current) {
                bound.values.put(match.pathVariable(), pathOf(bound, pattern));
            }
        }
        return current;
    }

    /**
     * 在通用 MATCH 管道中也下推起点 ID 谓词。
     * 带 LET/GROUP BY 的查询不会走 MatchExecutor 的轻量 lower 路径，
     * 因此这里必须在 bindNode 前处理 WHERE id(start)=...。
     */
    private List<Long> sourceIdCandidates(
            Stmt.Clause.Match match,
            int graphId,
            Map<String, Object> params) {
        if (match.patterns().isEmpty() || match.where() == null) {
            return null;
        }
        Stmt.Pattern pattern = match.patterns().get(0);
        if (pattern.elements().isEmpty() || !(pattern.elements().get(0) instanceof Stmt.Element.Node source)) {
            return null;
        }
        String variable = source.node().variable();
        if (variable == null) {
            return null;
        }
        NodeType type = source.node().typeName() == null
                ? null
                : catalog.nodeType(graphId, source.node().typeName());
        ExprEval eval = new ExprEval(params, false);
        return sourceIdCandidates(match.where(), variable, type, graphId, eval);
    }

    private List<Long> sourceIdCandidates(
            Expr where,
            String variable,
            NodeType type,
            int graphId,
            ExprEval eval) {
        if (where instanceof Expr.Binary binary && "AND".equals(binary.op())) {
            List<Long> left = sourceIdCandidates(binary.left(), variable, type, graphId, eval);
            return left != null ? left : sourceIdCandidates(binary.right(), variable, type, graphId, eval);
        }
        if (where instanceof Expr.Binary binary && "=".equals(binary.op())) {
            Expr constant;
            if (isIdExpr(binary.left(), variable)) {
                constant = binary.right();
            } else if (isIdExpr(binary.right(), variable)) {
                constant = binary.left();
            } else {
                return null;
            }
            Object value = constantValue(constant, eval);
            if (value == null) {
                return List.of();
            }
            long id = catalog.findVertexId(graphId, type, value);
            return id == io.cengyan.catalog.Catalog.MISSING_VERTEX_ID ? List.of() : List.of(id);
        }
        if (where instanceof Expr.In in && !in.negated() && isIdExpr(in.value(), variable)) {
            List<Long> ids = new ArrayList<>();
            for (Expr option : in.options()) {
                Object value = constantValue(option, eval);
                if (value == null) {
                    return List.of();
                }
                long id = catalog.findVertexId(graphId, type, value);
                if (id != io.cengyan.catalog.Catalog.MISSING_VERTEX_ID) {
                    ids.add(id);
                }
            }
            return ids;
        }
        return null;
    }

    private static boolean isIdExpr(Expr expr, String variable) {
        if (expr instanceof Expr.Prop prop) {
            return variable.equals(prop.variable()) && "id".equals(prop.field());
        }
        return expr instanceof Expr.Call call
                && "id".equalsIgnoreCase(call.name())
                && call.args().size() == 1
                && call.args().get(0) instanceof Expr.Variable ref
                && variable.equals(ref.name());
    }

    private static Object constantValue(Expr expr, ExprEval eval) {
        return expr instanceof Expr.Literal || expr instanceof Expr.Param ? eval.eval(expr) : null;
    }

    private List<Row> bindNode(
            Stmt.NodePat node,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        if (node.variable() != null && row.vids.containsKey(node.variable())) {
            return keepNode(node, row, graphId, params) ? List.of(row) : List.of();
        }
        List<Long> ids = row.candidates != null
                ? row.candidates
                : nodeIds(node, session, graphId, params, limits, cancel);
        List<Row> rows = new ArrayList<>();
        for (long vid : ids) {
            NodeType type = typeOf(graphId, vid, node);
            if (type == null || !labelOk(node, type.name())) {
                continue;
            }
            Map<String, Object> props = properties(graphId, type, vid);
            if (props == null) {
                continue;
            }
            Row copy = row.copy();
            copy.candidates = null;
            if (node.variable() != null) {
                copy.vids.put(node.variable(), vid);
                copy.values.put(node.variable(), nodeMap(graphId, type, vid, props));
            }
            if (!propsOk(node.properties(), props, params) || !inline(node.where(), copy, graphId, params)) {
                continue;
            }
            rows.add(copy);
        }
        return rows;
    }

    private List<Row> bindEdge(
            Stmt.EdgePat edge,
            Stmt.Clause.Match match,
            String previous,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        if (previous == null || !row.vids.containsKey(previous)) {
            throw new CengyanException(ErrorCode.SYNTAX, "边前面要有已经绑定的点");
        }
        long start = row.vids.get(previous);
        if (edge.minHop() == null) {
            return oneHop(edge, start, row, session, graphId, params, limits, cancel);
        }
        if (match.search() != null) {
            return shortest(edge, match, start, row, session, graphId, params, limits, cancel);
        }
        return walks(edge, match.mode(), start, row, session, graphId, params, limits, cancel);
    }

    private List<Row> oneHop(
            Stmt.EdgePat edge,
            long start,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        List<Row> rows = new ArrayList<>();
        for (Found found : scan(edge, start, session, graphId, params, limits, cancel)) {
            Row copy = row.copy();
            bindFound(copy, edge, List.of(found), graphId);
            rows.add(copy);
        }
        return rows;
    }

    private List<Row> walks(
            Stmt.EdgePat edge,
            String mode,
            long start,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        int min = Math.max(0, edge.minHop());
        int max = edge.maxHop() != null && edge.maxHop() < 0 ? budgetHops(limits) : edge.maxHop();
        List<List<Found>> walks = new ArrayList<>();
        if (min == 0) {
            walks.add(List.of());
        }
        walk(edge, mode == null ? "WALK" : mode, start, List.of(), new LinkedHashSet<>(List.of(start)),
                new LinkedHashSet<>(), 1, max, walks, session, graphId, params, limits, cancel);
        List<Row> rows = new ArrayList<>();
        for (List<Found> walk : walks) {
            if (walk.size() < min || walk.size() > max) {
                continue;
            }
            Row copy = row.copy();
            bindFound(copy, edge, walk, graphId);
            rows.add(copy);
            if (rows.size() > limits.maxPaths()) {
                throw new CengyanException(ErrorCode.PATH_LIMIT_EXCEEDED, "路径条数超过上限 " + limits.maxPaths());
            }
        }
        return rows;
    }

    private void walk(
            Stmt.EdgePat edge,
            String mode,
            long vid,
            List<Found> path,
            Set<Long> nodes,
            Set<EdgeRef> used,
            int depth,
            int max,
            List<List<Found>> walks,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        if (depth > max) {
            return;
        }
        cancelled(cancel);
        List<Found> next = scan(edge, vid, session, graphId, params, limits, cancel);
        if (next.size() > limits.maxFrontierPerHop()) {
            throw new CengyanException(ErrorCode.FRONTIER_EXCEEDED,
                    "第 " + depth + " 跳起点 " + vid + " 的新点超过上限 " + limits.maxFrontierPerHop());
        }
        for (Found found : next) {
            if ("TRAIL".equals(mode) && used.contains(found.edge)) {
                continue;
            }
            if (("SIMPLE".equals(mode) || "ACYCLIC".equals(mode)) && nodes.contains(found.other)) {
                continue;
            }
            List<Found> longer = new ArrayList<>(path);
            longer.add(found);
            walks.add(longer);
            Set<Long> seenNodes = new LinkedHashSet<>(nodes);
            seenNodes.add(found.other);
            Set<EdgeRef> seenEdges = new LinkedHashSet<>(used);
            seenEdges.add(found.edge);
            walk(edge, mode, found.other, longer, seenNodes, seenEdges, depth + 1, max, walks, session, graphId, params, limits, cancel);
        }
    }

    private List<Row> shortest(
            Stmt.EdgePat edge,
            Stmt.Clause.Match match,
            long start,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        int max = edge.maxHop() != null && edge.maxHop() < 0 ? budgetHops(limits) : edge.maxHop();
        List<List<Found>> layer = List.of(List.of());
        List<List<Found>> found = new ArrayList<>();
        Set<Long> seen = new LinkedHashSet<>();
        seen.add(start);
        for (int hop = 1; hop <= max && found.isEmpty(); hop++) {
            List<List<Found>> next = new ArrayList<>();
            for (List<Found> path : layer) {
                long vid = path.isEmpty() ? start : path.get(path.size() - 1).other;
                for (Found step : scan(edge, vid, session, graphId, params, limits, cancel)) {
                    if (!seen.add(step.other)) {
                        continue;
                    }
                    List<Found> longer = new ArrayList<>(path);
                    longer.add(step);
                    next.add(longer);
                    if (edge.minHop() == null || hop >= edge.minHop()) {
                        found.add(longer);
                    }
                }
            }
            layer = next;
        }
        int keep = switch (match.search()) {
            case "ANY_SHORTEST", "ANY" -> match.searchCount() == null ? 1 : match.searchCount();
            case "SHORTEST" -> match.searchCount() == null ? 1 : match.searchCount();
            default -> found.size();
        };
        List<Row> rows = new ArrayList<>();
        for (List<Found> path : found) {
            if (rows.size() >= keep) {
                break;
            }
            Row copy = row.copy();
            bindFound(copy, edge, path, graphId);
            rows.add(copy);
        }
        return rows;
    }

    private List<Row> bindGroup(
            Stmt.Element.Group group,
            Stmt.Clause.Match match,
            Row row,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        List<Row> current = List.of(row);
        List<Row> produced = new ArrayList<>();
        if (group.min() == 0) {
            produced.add(row);
        }
        for (int i = 1; i <= group.max() && i < 64; i++) {
            List<Row> next = new ArrayList<>();
            for (Row left : current) {
                next.addAll(matchPattern(new Stmt.Pattern(group.elements()), match, left, session, graphId, params, limits, cancel));
            }
            if (group.where() != null) {
                next = filter(group.where(), next, graphId, params);
            }
            current = next;
            if (i >= group.min()) {
                produced.addAll(current);
            }
        }
        return produced;
    }

    private void bindFound(Row row, Stmt.EdgePat edge, List<Found> found, int graphId) {
        if (found.isEmpty()) {
            return;
        }
        Found last = found.get(found.size() - 1);
        for (Found step : found) {
            row.pathEdges.add(edgeMap(graphId, step));
        }
        if (edge.variable() != null) {
            if (edge.minHop() == null || found.size() == 1) {
                row.edgeRefs.put(edge.variable(), last.edge);
                row.values.put(edge.variable(), edgeMap(graphId, last));
            } else {
                List<Object> edges = new ArrayList<>();
                for (Found step : found) {
                    edges.add(edgeMap(graphId, step));
                }
                boolean all = true;
                row.edgeRefs.put(edge.variable(), last.edge);
                row.values.put(edge.variable(), edges);
                if (!all) {
                    row.values.remove(edge.variable());
                }
            }
        }
        String target = null;
        row.candidates = List.of(last.other);
        row.vids.put("#end", last.other);
        row.values.put("#end", nodeMap(graphId, typeOf(graphId, last.other, null), last.other,
                properties(graphId, typeOf(graphId, last.other, null), last.other)));
        if (target != null) {
            row.vids.put(target, last.other);
        }
    }

    private List<Found> scan(
            Stmt.EdgePat edge,
            long start,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        List<RelType> rels = edge.typeNames().isEmpty()
                ? catalog.relTypes(graphId)
                : named(graphId, edge.typeNames());
        Set<Integer> typeIds = edge.typeNames().isEmpty() ? null : ids(rels);
        List<Found> found = new ArrayList<>();
        try (EdgeCursor cursor = session.scanEdges(
                graphId, start, typeIds, directions(edge.direction()), -1, limits.maxEdgesPerVertex(),
                ref -> edge.rank() == null || ref.rank() == edge.rank(), cancel)) {
            while (cursor.hasNext()) {
                EdgeRef ref = cursor.next();
                long other = ref.outSrc() == start ? ref.outDst() : ref.outSrc();
                RelType type = relById(rels, ref.edgeTypeId());
                Map<String, Object> props = catalog.readRel(graphId, type.name(), ref.rank(), ref.outSrc(), ref.outDst());
                if (!propsOk(edge.properties(), props == null ? Map.of() : props, params)) {
                    continue;
                }
                found.add(new Found(ref, other, type, props == null ? Map.of() : props));
            }
        }
        return found;
    }

    private List<Long> nodeIds(
            Stmt.NodePat node,
            ReadSession session,
            int graphId,
            Map<String, Object> params,
            QueryLimits limits,
            CancelToken cancel) {
        ExprEval eval = new ExprEval(params, false);
        for (Stmt.Prop prop : node.properties()) {
            if ("id".equals(prop.name())) {
                NodeType type = node.typeName() == null ? null : catalog.nodeType(graphId, node.typeName());
                long vid = catalog.findVertexId(graphId, type, eval.eval(prop.value()));
                return vid == Catalog.MISSING_VERTEX_ID ? List.of() : List.of(vid);
            }
        }
        List<NodeType> types = candidateTypes(graphId, node);
        if (types.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "起点需要 id 或节点类型");
        }
        List<Long> ids = new ArrayList<>();
        for (NodeType type : types) {
            for (VertexId vertex : session.scanVertices(graphId, type.id(), -1, limits.maxEdgesPerVertex(), cancel)) {
                ids.add(vertex.vid());
            }
        }
        return ids;
    }

    private QueryResult finish(Stmt.Tail tail, List<Row> rows, int graphId, Map<String, Object> params, CancelToken cancel) {
        ExprEval eval = new ExprEval(params, false);
        eval.subquery((query, outer) -> !run(asMatch(query), graphId, params, new QueryLimits(0), cancel).rows().isEmpty());
        List<String> columns = new ArrayList<>();
        List<List<Object>> table = new ArrayList<>();
        if (tail.star()) {
            for (Row row : rows) {
                List<Object> values = new ArrayList<>();
                if (columns.isEmpty()) {
                    columns.addAll(row.values.keySet());
                }
                for (String column : columns) {
                    values.add(row.values.get(column));
                }
                table.add(values);
            }
        } else {
            String[] kinds = aggregates(tail);
            for (Stmt.Item item : tail.items()) {
                columns.add(item.alias() == null ? item.text() : item.alias());
            }
            for (Row row : rows) {
                eval.slots(slots(graphId, row));
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < tail.items().size(); i++) {
                    Expr expr = tail.items().get(i).expr();
                    if (kinds != null && kinds[i] != null && !"group".equals(kinds[i])) {
                        if (expr instanceof Expr.Call call && !call.args().isEmpty()) {
                            Expr arg = call.args().get(0);
                            if (arg instanceof Expr.Unary unary && "DISTINCT".equals(unary.op())) {
                                arg = unary.inner();
                            }
                            if (arg instanceof Expr.Literal literal && "*".equals(literal.value())) {
                                values.add(1L);
                            } else {
                                values.add(DateTimes.plain(eval.eval(arg)));
                            }
                        }
                    } else {
                        values.add(DateTimes.plain(eval.eval(expr)));
                    }
                }
                table.add(values);
            }
            QueryMemory.current().observeRows(table);
            if (kinds != null) {
                table = DuckResult.aggregate(table, kinds, duck, cancel);
            }
        }
        if (tail.postFilter() != null) {
            List<List<Object>> filtered = new ArrayList<>();
            for (List<Object> row : table) {
                Map<String, ExprEval.Slot> slots = new LinkedHashMap<>();
                for (int i = 0; i < columns.size() && i < row.size(); i++) {
                    slots.put(columns.get(i), ExprEval.Slot.scalar(row.get(i)));
                }
                ExprEval post = new ExprEval(params, false);
                post.slots(slots);
                if (post.isTrue(tail.postFilter())) {
                    filtered.add(row);
                }
            }
            table = filtered;
        }
        Long skip = count(tail.skip(), params);
        Long limit = count(tail.limit(), params);
        if (!tail.orderBy().isEmpty() || tail.distinct() || skip != null || limit != null) {
            List<List<Object>> keys = new ArrayList<>();
            for (List<Object> ignored : table) {
                keys.add(List.of());
            }
            if (!tail.orderBy().isEmpty() && !table.isEmpty()) {
                keys = new ArrayList<>();
                for (List<Object> row : table) {
                    List<Object> key = new ArrayList<>();
                    for (Stmt.Sort sort : tail.orderBy()) {
                        key.add(sortKey(sort.expr(), tail.items(), row));
                    }
                    keys.add(key);
                }
            }
            table = DuckResult.arrange(table, keys, tail.orderBy(), tail.distinct(), skip, limit, duck, cancel);
        }
        return new QueryResult(columns, table, 0);
    }

    private static Object sortKey(Expr expr, List<Stmt.Item> items, List<Object> row) {
        if (expr instanceof Expr.Variable variable) {
            for (int i = 0; i < items.size(); i++) {
                if (variable.name().equals(items.get(i).alias())) {
                    return row.get(i);
                }
            }
        }
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).expr().equals(expr)) {
                return row.get(i);
            }
        }
        return null;
    }

    private static String[] aggregates(Stmt.Tail tail) {
        String[] kinds = new String[tail.items().size()];
        boolean aggregate = false;
        boolean plain = false;
        for (int i = 0; i < tail.items().size(); i++) {
            if (tail.items().get(i).expr() instanceof Expr.Call call && aggregate(call.name())) {
                aggregate = true;
                boolean star = call.args().size() == 1 && call.args().get(0) instanceof Expr.Literal literal && "*".equals(literal.value());
                kinds[i] = star ? "star" : call.name().toLowerCase(Locale.ROOT);
                if ("count".equals(kinds[i]) && !star && call.args().get(0) instanceof Expr.Unary unary
                        && "DISTINCT".equals(unary.op())) {
                    kinds[i] = "count_distinct";
                }
            } else {
                plain = true;
            }
        }
        if (!aggregate) {
            return null;
        }
        if (plain || !tail.groupBy().isEmpty()) {
            for (int i = 0; i < kinds.length; i++) {
                if (kinds[i] == null) {
                    kinds[i] = "group";
                }
            }
        }
        return kinds;
    }

    private static boolean aggregate(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "count", "sum", "avg", "min", "max", "collect_list", "stddev_samp", "stddev_pop",
                    "percentile_cont", "percentile_disc" -> true;
            default -> false;
        };
    }

    private int writePattern(List<Stmt.Element> elements, int graphId, Map<String, Object> params, Map<String, Long> bound) {
        ExprEval eval = new ExprEval(params, true);
        Map<String, Long> vids = new LinkedHashMap<>(bound);
        int written = 0;
        String previous = null;
        Stmt.EdgePat pending = null;
        for (Stmt.Element element : elements) {
            switch (element) {
                case Stmt.Element.Node node -> {
                    String key = node.node().variable() == null ? "#n" + written : node.node().variable();
                    long vid = vids.containsKey(key)
                            ? vids.get(key)
                            : ensureVertex(node.node(), graphId, vids, eval);
                    vids.put(key, vid);
                    if (pending != null) {
                        if (previous == null) {
                            throw new CengyanException(ErrorCode.SYNTAX, "插入边缺少起点");
                        }
                        long src = vids.get(previous);
                        long dst = vid;
                        if (pending.direction() == Stmt.EdgePat.Direction.IN) {
                            long swap = src;
                            src = dst;
                            dst = swap;
                        }
                        if (pending.direction() == Stmt.EdgePat.Direction.ANY || pending.typeNames().size() != 1) {
                            throw new CengyanException(ErrorCode.SYNTAX, "插入边必须有方向和一个类型");
                        }
                        catalog.insertRel(graphId, pending.typeNames().get(0),
                                pending.rank() == null ? 0L : pending.rank(),
                                io.cengyan.catalog.VertexWrite.onlyId(src),
                                io.cengyan.catalog.VertexWrite.onlyId(dst),
                                props(pending.properties(), eval), false);
                        written++;
                        pending = null;
                    }
                    previous = key;
                }
                case Stmt.Element.Rel rel -> pending = rel.edge();
                case Stmt.Element.Group ignored -> throw new CengyanException(ErrorCode.SYNTAX, "插入不能是多跳");
            }
        }
        return written;
    }

    private long ensureVertex(Stmt.NodePat node, int graphId, Map<String, Long> vids, ExprEval eval) {
        if (node.typeName() == null) {
            return catalog.findVertexId(graphId, null, idOf(node, vids, eval));
        }
        NodeType type = catalog.nodeType(graphId, node.typeName());
        long vid = catalog.resolveVertexId(graphId, type, idOf(node, vids, eval));
        catalog.insertVertex(graphId, type.name(), vid, props(node.properties(), eval), false);
        return vid;
    }

    private List<Row> filter(Expr expr, List<Row> rows, int graphId, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, false);
        List<Row> kept = new ArrayList<>();
        for (Row row : rows) {
            eval.slots(slots(graphId, row));
            if (eval.isTrue(expr)) {
                kept.add(row);
            }
        }
        return kept;
    }

    private List<Row> let(Stmt.Clause.Let let, List<Row> rows, int graphId, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, false);
        for (Row row : rows) {
            eval.slots(slots(graphId, row));
            row.values.put(let.name(), DateTimes.plain(eval.eval(let.value())));
        }
        return rows;
    }

    private List<Row> unwind(Stmt.Clause.For each, List<Row> rows, int graphId, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, false);
        List<Row> unwound = new ArrayList<>();
        for (Row row : rows) {
            eval.slots(slots(graphId, row));
            Object value = eval.eval(each.list());
            if (!(value instanceof List<?> list)) {
                throw new CengyanException(ErrorCode.TYPE_MISMATCH, "FOR 的右边要是列表");
            }
            for (Object item : list) {
                Row copy = row.copy();
                copy.values.put(each.name(), item);
                unwound.add(copy);
            }
        }
        return unwound;
    }

    private List<Row> call(Stmt.Call call, List<Row> rows, int graphId, Map<String, Object> params, QueryLimits limits, CancelToken cancel) {
        List<Row> result = new ArrayList<>();
        for (Row row : rows) {
            Map<String, Object> scoped = new LinkedHashMap<>(params);
            scoped.putAll(row.values);
            QueryResult called = procedures.call(call, graphId, scoped, limits, cancel);
            for (List<Object> line : called.rows()) {
                Row copy = row.copy();
                for (int i = 0; i < called.columns().size(); i++) {
                    copy.values.put(called.columns().get(i), line.get(i));
                }
                result.add(copy);
            }
        }
        return result;
    }

    private Map<String, ExprEval.Slot> slots(int graphId, Row row) {
        Map<String, ExprEval.Slot> slots = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.values.entrySet()) {
            if (row.vids.containsKey(entry.getKey()) && entry.getValue() instanceof Map<?, ?> map) {
                Object id = map.get("id");
                Object type = map.get("type");
                Object properties = map.get("properties");
                slots.put(entry.getKey(), ExprEval.Slot.node(
                        row.vids.get(entry.getKey()), id, type == null ? "" : String.valueOf(type),
                        properties instanceof Map<?, ?> props ? cast(props) : Map.of()));
            } else if (row.edgeRefs.containsKey(entry.getKey())) {
                EdgeRef edge = row.edgeRefs.get(entry.getKey());
                RelType type = relById(graphId, edge.edgeTypeId());
                Map<String, Object> props = catalog.readRel(graphId, type.name(), edge.rank(), edge.outSrc(), edge.outDst());
                slots.put(entry.getKey(), ExprEval.Slot.edge(
                        type.name(), edge.rank(), edge.outSrc(), edge.outDst(),
                        catalog.externalVertexId(graphId, null, edge.outSrc()),
                        catalog.externalVertexId(graphId, null, edge.outDst()),
                        props == null ? Map.of() : props, null, null));
            } else {
                slots.put(entry.getKey(), ExprEval.Slot.scalar(entry.getValue()));
            }
        }
        return slots;
    }

    private boolean keepNode(Stmt.NodePat node, Row row, int graphId, Map<String, Object> params) {
        long vid = row.vids.get(node.variable());
        NodeType type = typeOf(graphId, vid, node);
        return type != null && labelOk(node, type.name())
                && propsOk(node.properties(), properties(graphId, type, vid), params)
                && inline(node.where(), row, graphId, params);
    }

    private boolean inline(Expr where, Row row, int graphId, Map<String, Object> params) {
        if (where == null) {
            return true;
        }
        ExprEval eval = new ExprEval(params, false);
        eval.slots(slots(graphId, row));
        return eval.isTrue(where);
    }

    private boolean propsOk(List<Stmt.Prop> expected, Map<String, Object> actual, Map<String, Object> params) {
        ExprEval eval = new ExprEval(params, false);
        for (Stmt.Prop prop : expected) {
            if ("id".equals(prop.name())) {
                continue;
            }
            if (!eval.sameValue(eval.eval(prop.value()), actual.get(prop.name()))) {
                return false;
            }
        }
        return true;
    }

    private boolean labelOk(Stmt.NodePat node, String type) {
        if (node.label() == null) {
            return node.typeName() == null || node.typeName().equals(type);
        }
        return labelHit(node.label(), type);
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

    private List<NodeType> candidateTypes(int graphId, Stmt.NodePat node) {
        if (node.typeName() != null) {
            return List.of(catalog.nodeType(graphId, node.typeName()));
        }
        List<NodeType> types = new ArrayList<>();
        for (NodeType type : catalog.nodeTypes(graphId)) {
            if (labelOk(node, type.name())) {
                types.add(type);
            }
        }
        return types;
    }

    private NodeType typeOf(int graphId, long vid, Stmt.NodePat hint) {
        if (hint != null && hint.typeName() != null) {
            NodeType type = catalog.nodeType(graphId, hint.typeName());
            return catalog.readVertex(graphId, type.name(), vid) == null ? null : type;
        }
        for (NodeType type : catalog.nodeTypes(graphId)) {
            if (catalog.readVertex(graphId, type.name(), vid) != null) {
                return type;
            }
        }
        return null;
    }

    private Map<String, Object> properties(int graphId, NodeType type, long vid) {
        if (type == null) {
            return null;
        }
        return catalog.readVertex(graphId, type.name(), vid);
    }

    private Map<String, Object> nodeMap(int graphId, NodeType type, long vid, Map<String, Object> props) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", catalog.externalVertexId(graphId, type, vid));
        body.put("type", type == null ? null : type.name());
        body.put("properties", props == null ? Map.of() : props);
        return body;
    }

    private Map<String, Object> edgeMap(int graphId, Found found) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", found.type.name());
        body.put("rank", found.edge.rank());
        body.put("src", catalog.externalVertexId(graphId, null, found.edge.outSrc()));
        body.put("dst", catalog.externalVertexId(graphId, null, found.edge.outDst()));
        body.put("properties", found.props);
        return body;
    }

    private Map<String, Object> pathOf(Row row, Stmt.Pattern pattern) {
        Map<String, Object> body = new LinkedHashMap<>();
        List<Object> nodes = new ArrayList<>();
        for (Stmt.Element element : pattern.elements()) {
            if (element instanceof Stmt.Element.Node node && node.node().variable() != null) {
                nodes.add(row.values.get(node.node().variable()));
            }
        }
        body.put("length", Math.max(0, nodes.size() - 1));
        body.put("nodes", nodes);
        body.put("edges", List.copyOf(row.pathEdges));
        return body;
    }

    private Mutation assignment(int graphId, Row row, Stmt.Assign assignment, ExprEval eval) {
        Object value = eval.eval(assignment.value());
        if (value instanceof UtcTime time) {
            value = time.millis();
        }
        if (row.edgeRefs.containsKey(assignment.variable())) {
            EdgeRef edge = row.edgeRefs.get(assignment.variable());
            RelType type = relById(graphId, edge.edgeTypeId());
            return new Mutation.SetEdge(type.name(), edge.rank(), edge.outSrc(), edge.outDst(), Map.of(assignment.field(), value));
        }
        Long vid = row.vids.get(assignment.variable());
        if (vid == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + assignment.variable());
        }
        NodeType type = typeOf(graphId, vid, null);
        return new Mutation.SetVertex(type.name(), vid, Map.of(assignment.field(), value));
    }

    private Mutation setAll(int graphId, Row row, String variable, Map<String, Object> values) {
        Long vid = row.vids.get(variable);
        if (vid == null) {
            throw new CengyanException(ErrorCode.SYNTAX, "未知变量 " + variable);
        }
        NodeType type = typeOf(graphId, vid, null);
        return new Mutation.SetVertex(type.name(), vid, values);
    }

    private Object idOf(Stmt.NodePat node, Map<String, Long> vids, ExprEval eval) {
        for (Stmt.Prop prop : node.properties()) {
            if ("id".equals(prop.name())) {
                return eval.eval(prop.value());
            }
        }
        if (node.variable() != null && vids.containsKey(node.variable())) {
            return vids.get(node.variable());
        }
        throw new CengyanException(ErrorCode.SYNTAX, "插入需要 id");
    }

    private static Map<String, Object> props(List<Stmt.Prop> props, ExprEval eval) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Stmt.Prop prop : props) {
            if (!"id".equals(prop.name())) {
                Object value = eval.eval(prop.value());
                values.put(prop.name(), value instanceof UtcTime time ? time.millis() : value);
            }
        }
        return values;
    }

    private List<RelType> named(int graphId, List<String> names) {
        List<RelType> types = new ArrayList<>();
        for (String name : names) {
            types.add(catalog.relType(graphId, name));
        }
        return types;
    }

    private static Set<Integer> ids(List<RelType> rels) {
        Set<Integer> ids = new LinkedHashSet<>();
        for (RelType rel : rels) {
            ids.add(rel.id());
        }
        return ids;
    }

    private RelType relById(int graphId, int id) {
        return relById(catalog.relTypes(graphId), id);
    }

    private static RelType relById(List<RelType> rels, int id) {
        for (RelType rel : rels) {
            if (rel.id() == id) {
                return rel;
            }
        }
        throw new CengyanException(ErrorCode.INTERNAL, "边类型丢失");
    }

    private static List<Direction> directions(Stmt.EdgePat.Direction direction) {
        return switch (direction) {
            case OUT -> List.of(Direction.OUT);
            case IN -> List.of(Direction.IN);
            case ANY -> List.of(Direction.OUT, Direction.IN);
        };
    }

    private static int budgetHops(QueryLimits limits) {
        if (limits.maxHops() == Integer.MAX_VALUE) {
            throw new CengyanException(ErrorCode.SYNTAX, "请写上界或配置 max_hops");
        }
        return limits.maxHops();
    }

    private static Long count(Expr expr, Map<String, Object> params) {
        if (expr == null) {
            return null;
        }
        Object value = new ExprEval(params, false).eval(expr);
        if (value instanceof Long number && number >= 0) {
            return number;
        }
        throw new CengyanException(ErrorCode.TYPE_MISMATCH, "LIMIT 或 SKIP 必须是整数");
    }

    private static Map<String, Object> cast(Map<?, ?> props) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : props.entrySet()) {
            values.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return values;
    }

    private static Stmt asMatch(Stmt query) {
        return query;
    }

    private static void cancelled(CancelToken cancel) {
        if (cancel != null && cancel.cancelled()) {
            throw new CengyanException(ErrorCode.QUERY_CANCELED, "查询已取消");
        }
    }

    private void describe(Stmt statement, List<String> lines, int indent) {
        String pad = "  ".repeat(indent);
        switch (statement) {
            case Stmt.Select select -> {
                for (Stmt.Clause clause : select.clauses()) {
                    lines.add(pad + clause.getClass().getSimpleName());
                }
                lines.add(pad + "Project");
                if (!select.tail().orderBy().isEmpty()) {
                    lines.add(pad + "DuckSort");
                }
            }
            case Stmt.Composite composite -> {
                lines.add(pad + composite.op());
                describe(composite.left(), lines, indent + 1);
                describe(composite.right(), lines, indent + 1);
            }
            case Stmt.Next next -> {
                lines.add(pad + "Next");
                describe(next.left(), lines, indent + 1);
                describe(next.right(), lines, indent + 1);
            }
            default -> lines.add(pad + statement.getClass().getSimpleName());
        }
    }

    private record Found(EdgeRef edge, long other, RelType type, Map<String, Object> props) {
    }

    private static final class Row {
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, Long> vids = new LinkedHashMap<>();
        private final Map<String, EdgeRef> edgeRefs = new LinkedHashMap<>();
        private final List<Object> pathEdges = new ArrayList<>();
        private List<Long> candidates;

        private static Row empty() {
            return new Row();
        }

        private Row copy() {
            Row copy = new Row();
            copy.values.putAll(values);
            copy.vids.putAll(vids);
            copy.edgeRefs.putAll(edgeRefs);
            copy.pathEdges.addAll(pathEdges);
            copy.candidates = candidates;
            return copy;
        }
    }
}
