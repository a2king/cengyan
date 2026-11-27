package io.cengyan.query;

import com.google.protobuf.ByteString;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.EntityCodec;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Stmt;
import io.cengyan.protocol.proto.ClusterProtocol.*;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

/** 有界 WALK 查询的路径计数及带持股比例的路径聚合。 */
final class SpillPathQuery {
    private static final int BATCH = 128;
    private static final int PATH_LIMIT = 100_000;

    record Plan(Stmt.NodePat start, Stmt.NodePat end, Stmt.EdgePat edge, Expr id, int min, int max,
                String weight, Stmt.Tail tail) {
        boolean countOnly() { return weight == null; }
    }

    static Plan plan(Stmt.Select select) {
        var tail = select.tail();
        if (tail == null || tail.distinct() || tail.star() || tail.finish()
                || select.clauses().isEmpty() || !(select.clauses().getFirst() instanceof Stmt.Clause.Match match)
                || match.optional() || "DIFFERENT_EDGES".equals(match.matchMode()) || match.patterns().size() != 1) return null;
        var pattern = match.patterns().getFirst();
        if (pattern.search() != null || !pattern.union().isEmpty()
                || pattern.mode() != null && !pattern.mode().equalsIgnoreCase("WALK") || pattern.elements().size() != 3
                || !(pattern.elements().get(0) instanceof Stmt.Element.Node from)
                || !(pattern.elements().get(1) instanceof Stmt.Element.Rel link)
                || !(pattern.elements().get(2) instanceof Stmt.Element.Node to)) return null;
        var start = from.node();
        var end = to.node();
        var edge = link.edge();
        if (start.variable() == null || end.variable() == null || start.variable().equals(end.variable())
                || !simpleNode(start) || !simpleNode(end) || edge.typeName() == null
                || edge.direction() == Stmt.EdgePat.Direction.ANY || edge.where() != null
                || !edge.properties().isEmpty() || edge.rank() != null
                || edge.variable() != null && (edge.variable().equals(start.variable()) || edge.variable().equals(end.variable()))
                || pattern.pathVariable() != null && (pattern.pathVariable().equals(start.variable())
                    || pattern.pathVariable().equals(end.variable()) || pattern.pathVariable().equals(edge.variable()))) return null;
        Expr id = fixedId(match.where(), start.variable());
        int min = edge.minHop() == null ? 1 : edge.minHop();
        int max = edge.maxHop() == null ? 1 : edge.maxHop();
        if (id == null || min < 0 || max < min || max > 32) return null;
        var items = tail.items();
        if (select.clauses().size() == 1 && items.size() == 1 && countStar(items.getFirst().expr())
                && tail.groupBy().isEmpty() && tail.postFilter() == null && tail.orderBy().isEmpty()
                && tail.skip() == null && tail.limit() == null) {
            return new Plan(start, end, edge, id, min, max, null, tail);
        }
        if (select.clauses().size() != 2 || !(select.clauses().get(1) instanceof Stmt.Clause.Let let)
                || pattern.pathVariable() == null || items.size() != 5
                || let.name().equals(start.variable()) || let.name().equals(end.variable()) || let.name().equals(pattern.pathVariable())
                || !variable(items.get(0).expr(), start.variable()) || !variable(items.get(1).expr(), end.variable())
                || !collect(items.get(2).expr(), "nodes", pattern.pathVariable())
                || !collect(items.get(3).expr(), "relationships", pattern.pathVariable())
                || !call(items.get(4).expr(), "sum", let.name())
                || items.get(4).alias() == null || tail.orderBy().size() > 1
                || !tail.orderBy().isEmpty() && !variable(tail.orderBy().getFirst().expr(), items.get(4).alias())
                || tail.limit() != null && !constant(tail.limit()) || tail.skip() != null && !constant(tail.skip())) return null;
        if (tail.postFilter() != null && !scoreFilter(tail.postFilter(), items.get(4).alias())) return null;
        String weight = weight(let.value(), pattern.pathVariable());
        return weight == null ? null : new Plan(start, end, edge, id, min, max, weight, tail);
    }

    private static boolean simpleNode(Stmt.NodePat node) {
        return node.where() == null && node.properties().isEmpty()
                && (node.label() == null || node.label() instanceof Stmt.Label.One);
    }

    private static Expr fixedId(Expr where, String name) {
        if (!(where instanceof Expr.Binary pair) || !pair.op().equals("=")) return null;
        if (call(pair.left(), "id", name) && constant(pair.right())) return pair.right();
        if (call(pair.right(), "id", name) && constant(pair.left())) return pair.left();
        return null;
    }

    private static boolean constant(Expr expr) { return expr instanceof Expr.Literal || expr instanceof Expr.Param; }
    private static boolean variable(Expr expr, String name) { return expr instanceof Expr.Variable v && v.name().equals(name); }
    private static boolean countStar(Expr expr) {
        if (!(expr instanceof Expr.Call fn) || !fn.name().equalsIgnoreCase("count") || fn.args().size() != 1) return false;
        var argument = fn.args().getFirst();
        return variable(argument, "*") || argument instanceof Expr.Literal literal && "*".equals(literal.value());
    }
    private static boolean call(Expr expr, String name, String var) {
        return expr instanceof Expr.Call fn && fn.name().equalsIgnoreCase(name) && fn.args().size() == 1
                && variable(fn.args().getFirst(), var);
    }
    private static boolean collect(Expr expr, String name, String path) {
        return expr instanceof Expr.Call fn && fn.name().equalsIgnoreCase("collect_list") && fn.args().size() == 1
                && call(fn.args().getFirst(), name, path);
    }
    private static boolean scoreFilter(Expr expr, String alias) {
        return expr instanceof Expr.Binary compare && Set.of("=", "<", ">", "<=", ">=", "<>", "!=").contains(compare.op())
                && (variable(compare.left(), alias) && constant(compare.right())
                    || variable(compare.right(), alias) && constant(compare.left()));
    }
    private static String weight(Expr expr, String path) {
        if (!(expr instanceof Expr.Reduce reduce) || !call(reduce.collection(), "relationships", path)
                || !(reduce.initial() instanceof Expr.Literal start) || !(start.value() instanceof Double first) || first != 1
                || !(reduce.body() instanceof Expr.Binary product) || !product.op().equals("*")
                || !variable(product.left(), reduce.accumulator()) || !(product.right() instanceof Expr.Call coalesce)
                || !coalesce.name().equalsIgnoreCase("coalesce") || coalesce.args().size() != 2
                || !(coalesce.args().getFirst() instanceof Expr.Prop property) || !property.variable().equals(reduce.item())
                || !(coalesce.args().getLast() instanceof Expr.Literal fallback) || !(fallback.value() instanceof Double zero)
                || zero != 0) return null;
        return property.field();
    }

    private record State(long[] chain, double score) {}
    private record Match(ByteString key, long end, long sequence, double score, long path) {}
    private record Group(long end, long first, long sequence, double score, int count) {}

    private static final SpillSorter.Codec<Match> MATCH = new SpillSorter.Codec<>() {
        @Override public Match read(DataInputStream in) throws IOException {
            var key = ByteString.copyFrom(readBytes(in));
            return new Match(key, in.readLong(), in.readLong(), in.readDouble(), in.readLong());
        }
        @Override public void write(DataOutputStream out, Match value) throws IOException {
            out.writeInt(value.key().size());
            value.key().writeTo(out);
            out.writeLong(value.end());
            out.writeLong(value.sequence());
            out.writeDouble(value.score());
            out.writeLong(value.path());
        }
    };
    private static final SpillSorter.Codec<Group> GROUP = new SpillSorter.Codec<>() {
        @Override public Group read(DataInputStream in) throws IOException {
            return new Group(in.readLong(), in.readLong(), in.readLong(), in.readDouble(), in.readInt());
        }
        @Override public void write(DataOutputStream out, Group value) throws IOException {
            out.writeLong(value.end());
            out.writeLong(value.first());
            out.writeLong(value.sequence());
            out.writeDouble(value.score());
            out.writeInt(value.count());
        }
    };

    private final GraphDefinition graph;
    private final RemoteQueryEngine.Reads reads;
    private final TransactionEngine.QueryScope scope;
    private final int threads;
    private final long memory;
    private final Runnable check;
    private final Function<Entity, Map<String, Object>> body;

    SpillPathQuery(GraphDefinition graph, RemoteQueryEngine.Reads reads, TransactionEngine.QueryScope scope, int threads,
                   long memory, Runnable check, Function<Entity, Map<String, Object>> body) {
        this.graph = graph;
        this.reads = reads;
        this.scope = scope;
        this.threads = threads;
        this.memory = memory;
        this.check = check;
        this.body = body;
    }

    QueryResult execute(Plan plan, ExprEval eval, Path directory, long disk) {
        var columns = plan.tail().items().stream().map(item -> item.alias() == null ? item.text() : item.alias()).toList();
        Value id = EntityCodec.value(eval.eval(plan.id()));
        var start = reads.node(graph, type(plan.start()), id, scope);
        if (start == null) return new QueryResult(columns, plan.countOnly() ? List.of(List.of(0L)) : List.of(), 0);
        if (plan.countOnly()) return new QueryResult(columns, List.of(List.of(count(plan, start))), 0);
        try (var workspace = new SpillWorkspace(directory, disk);
             var entities = new Store(workspace);
             var details = new Store(workspace);
             var offsets = new Store(workspace)) {
            var sorter = new SpillSorter<>(workspace, MATCH,
                    Comparator.comparing(Match::key, ByteString.unsignedLexicographicalComparator())
                            .thenComparingLong(Match::sequence), memory / 8, check);
            long root = entities.add(start.toByteArray());
            enumerate(plan, start, root, workspace, entities, details, sorter);
            return new QueryResult(columns, results(plan, eval, workspace, entities, details, offsets, sorter, root), 0);
        } catch (IOException ex) {
            throw new ClusterException("QUERY_TEMP_IO", "查询落盘读写失败：" + ex.getMessage(), ex);
        }
    }

    private long count(Plan p, Entity start) {
        var cache = new Lookup(p);
        cache.nodes.put(start.getId(), start, size(start));
        Map<Value, Long> frontier = Map.of(start.getId(), 1L);
        long frontierBytes = countEntryBytes(start.getId());
        long count = p.min() == 0 && accepts(p, start) ? 1 : 0;
        for (int hop = 1; hop <= p.max() && !frontier.isEmpty(); hop++) {
            var next = new LinkedHashMap<Value, Long>();
            long nextBytes = 0;
            for (var batch : batches(frontier.keySet())) {
                var adjacency = cache.edges(batch);
                for (Value vertex : batch) {
                    check.run();
                    for (Entity edge : adjacency.get(vertex)) {
                        Value target = endpoint(edge, vertex);
                        Long previous = next.get(target);
                        if (previous == null) {
                            nextBytes += countEntryBytes(target);
                            guard(nextBytes + frontierBytes);
                        }
                        long paths = frontier.get(vertex);
                        next.put(target, previous == null ? paths : Math.addExact(previous, paths));
                    }
                }
            }
            for (var batch : batches(next.keySet())) {
                var found = cache.nodes(batch);
                for (Value id : batch) {
                    var node = found.get(id);
                    if (node == null) {
                        next.remove(id);
                        nextBytes -= countEntryBytes(id);
                    } else if (hop >= p.min() && accepts(p, node)) count = Math.addExact(count, next.get(id));
                }
            }
            frontier = next;
            frontierBytes = nextBytes;
        }
        return count;
    }

    private static long countEntryBytes(Value id) { return 160L + id.getSerializedSize() * 3L; }

    private void enumerate(Plan p, Entity start, long root, SpillWorkspace workspace, Store entities, Store details,
                           SpillSorter<Match> sorter) throws IOException {
        var lookup = new Lookup(p);
        var refs = new Cache<Entity, Long>(memory / 12);
        var decoded = new Cache<Long, Entity>(memory / 12);
        lookup.nodes.put(start.getId(), start, size(start));
        refs.put(start, root, size(start));
        decoded.put(root, start, size(start));
        Path frontier = workspace.file();
        try (var out = workspace.output(frontier)) { writeState(out, new State(new long[]{root}, 1)); }
        long sequence = 0;
        for (int depth = 0; depth <= p.max(); depth++) {
            Path next = depth < p.max() ? workspace.file() : null;
            long continued = 0;
            try (var input = workspace.input(frontier); var output = next == null ? null : workspace.output(next)) {
                while (input.available() > 0) {
                    check.run();
                    var states = new ArrayList<State>(BATCH);
                    var vertices = new ArrayList<Entity>(BATCH);
                    var keys = new LinkedHashSet<Value>();
                    while (states.size() < BATCH && input.available() > 0) {
                        var state = readState(input);
                        long ref = state.chain()[state.chain().length - 1];
                        var end = decoded.get(ref);
                        if (end == null) {
                            end = Entity.parseFrom(entities.get(ref));
                            decoded.put(ref, end, size(end));
                        }
                        states.add(state);
                        vertices.add(end);
                        keys.add(end.getId());
                    }
                    guard(vertices.stream().mapToLong(SpillPathQuery::size).sum());
                    var adjacency = output == null ? Map.<Value,List<Entity>>of() : lookup.edges(keys);
                    var targetIds = new LinkedHashSet<Value>();
                    adjacency.forEach((vertex, edges) -> {
                        for (Entity edge : edges) targetIds.add(endpoint(edge, vertex));
                    });
                    var found = lookup.nodes(targetIds);
                    for (int i = 0; i < states.size(); i++) {
                        var state = states.get(i);
                        var end = vertices.get(i);
                        long endRef = state.chain()[state.chain().length - 1];
                        if (depth >= p.min() && accepts(p, end)) {
                            if (sequence == PATH_LIMIT) throw new ClusterException("PATH_LIMIT_EXCEEDED", "路径数量超过安全上限");
                            long detail = details.add(encodeState(state));
                            var key = end.getId().toByteString();
                            sorter.add(new Match(key, endRef, sequence++, state.score(), detail), 128L + key.size());
                        }
                        if (output == null) continue;
                        for (Entity edge : adjacency.get(end.getId())) {
                            Entity target = found.get(endpoint(edge, end.getId()));
                            if (target == null) continue;
                            long[] chain = Arrays.copyOf(state.chain(), state.chain().length + 2);
                            chain[chain.length - 2] = ref(edge, refs, entities);
                            chain[chain.length - 1] = ref(target, refs, entities);
                            double score = state.score() * weight(edge, p.weight());
                            writeState(output, new State(chain, score));
                            if (++continued > 1_000_000) throw new ClusterException("PATH_LIMIT_EXCEEDED", "路径展开中间状态超过安全上限");
                        }
                    }
                }
            }
            workspace.delete(frontier);
            if (next == null) break;
            if (continued == 0) { workspace.delete(next); break; }
            frontier = next;
        }
    }

    private static long ref(Entity entity, Cache<Entity, Long> refs, Store entities) throws IOException {
        Long found = refs.get(entity);
        if (found != null) return found;
        long offset = entities.add(entity.toByteArray());
        refs.put(entity, offset, size(entity) + 32);
        return offset;
    }

    private List<List<Object>> results(Plan p, ExprEval eval, SpillWorkspace workspace, Store entities, Store details,
                                       Store offsets, SpillSorter<Match> input, long root) throws IOException {
        var order = Comparator.comparingDouble(Group::score);
        if (!p.tail().orderBy().isEmpty() && p.tail().orderBy().getFirst().descending()) order = order.reversed();
        Comparator<Group> rankOrder = p.tail().orderBy().isEmpty() ? Comparator.comparingLong(Group::sequence)
                : order.thenComparingLong(Group::sequence);
        var rank = new SpillSorter<>(workspace, GROUP, rankOrder, memory / 8, check);
        String alias = p.tail().items().get(4).alias();
        final class Aggregator {
            Match first;
            long offset;
            int count;
            double score;
            void flush() throws IOException {
                if (first == null) return;
                eval.slots(Map.of(alias, ExprEval.Slot.scalar(score)));
                if (p.tail().postFilter() == null || eval.isTrue(p.tail().postFilter())) {
                    rank.add(new Group(first.end(), offset, first.sequence(), score, count), 96);
                }
            }
            void add(Match row) throws IOException {
                if (first == null || !row.key().equals(first.key())) {
                    flush();
                    first = row;
                    offset = offsets.position();
                    count = 0;
                    score = 0;
                }
                count++;
                score += row.score();
                offsets.addLong(row.path());
            }
        }
        var aggregate = new Aggregator();
        input.visit(aggregate::add);
        aggregate.flush();
        eval.slots(Map.of());
        long skip = p.tail().skip() == null ? 0 : ((Number) eval.eval(p.tail().skip())).longValue();
        long limit = p.tail().limit() == null ? Long.MAX_VALUE : ((Number) eval.eval(p.tail().limit())).longValue();
        if (skip < 0 || limit < 0) throw new ClusterException("SYNTAX", "LIMIT 或 SKIP 超出范围");
        var start = body.apply(Entity.parseFrom(entities.get(root)));
        List<List<Object>> output = new ArrayList<>();
        long[] seen = {0};
        long[] retained = {0};
        rank.visit(group -> {
            if (seen[0]++ < skip || output.size() >= limit) return;
            var end = body.apply(Entity.parseFrom(entities.get(group.end())));
            var nodes = new ArrayList<List<Object>>();
            var edges = new ArrayList<List<Object>>();
            for (int i = 0; i < group.count(); i++) {
                check.run();
                var state = decodeState(details.get(offsets.getLong(group.first() + i * 8L)));
                var pathNodes = new ArrayList<Object>();
                var pathEdges = new ArrayList<Object>();
                for (int j = 0; j < state.chain().length; j++) {
                    var entity = Entity.parseFrom(entities.get(state.chain()[j]));
                    retained[0] += size(entity) * 2;
                    if (retained[0] > memory / 2) {
                        throw new ClusterException("QUERY_MEMORY_EXCEEDED", "路径明细结果超过查询内存预算");
                    }
                    (j % 2 == 0 ? pathNodes : pathEdges).add(body.apply(entity));
                }
                nodes.add(pathNodes);
                edges.add(pathEdges);
            }
            output.add(List.of(start, end, nodes, edges, group.score()));
        });
        return output;
    }

    private static State readState(DataInputStream input) throws IOException {
        int size = input.readUnsignedByte();
        if (size == 0 || size > 65) throw new IOException("路径临时文件记录长度异常");
        var chain = new long[size];
        for (int i = 0; i < size; i++) chain[i] = input.readLong();
        return new State(chain, input.readDouble());
    }

    private static void writeState(DataOutputStream output, State state) throws IOException {
        output.writeByte(state.chain().length);
        for (long ref : state.chain()) output.writeLong(ref);
        output.writeDouble(state.score());
    }

    private static byte[] encodeState(State state) throws IOException {
        var bytes = new ByteArrayOutputStream(9 + state.chain().length * 8);
        try (var output = new DataOutputStream(bytes)) { writeState(output, state); }
        return bytes.toByteArray();
    }

    private static State decodeState(byte[] bytes) throws IOException {
        try (var input = new DataInputStream(new ByteArrayInputStream(bytes))) { return readState(input); }
    }

    private static byte[] readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) throw new IOException("临时排序记录长度异常");
        return input.readNBytes(length);
    }

    private static String type(Stmt.NodePat node) {
        if (node.typeName() != null) return node.typeName();
        return node.label() instanceof Stmt.Label.One name ? name.name() : "";
    }
    private static boolean accepts(Plan plan, Entity entity) { return type(plan.end()).isEmpty() || type(plan.end()).equals(entity.getType()); }
    private static Value endpoint(Entity edge, Value vertex) {
        return edge.getSource().equals(vertex) ? edge.getTarget() : edge.getSource();
    }
    private static long size(Entity entity) { return entity == null ? 96 : 128L + entity.getSerializedSize() * 3L; }
    private static double weight(Entity edge, String name) {
        Object value = EntityCodec.object(edge.getPropertiesOrDefault(name, Value.getDefaultInstance()));
        if (value == null) return 0;
        if (!(value instanceof Number number)) throw new ClusterException("TYPE_MISMATCH", "路径累乘要求数字属性");
        return number.doubleValue();
    }

    private static List<List<Value>> batches(Collection<Value> values) {
        var batches = new ArrayList<List<Value>>();
        List<Value> batch = new ArrayList<>();
        for (Value value : values) {
            if (batch.size() == BATCH) { batches.add(batch); batch = new ArrayList<>(); }
            batch.add(value);
        }
        if (!batch.isEmpty()) batches.add(batch);
        return batches;
    }

    private void guard(long bytes) {
        if (bytes > memory / 8) throw new ClusterException("QUERY_MEMORY_EXCEEDED", "路径执行缓冲超过查询内存预算");
    }

    private final class Lookup {
        final Plan plan;
        final Cache<Value, Entity> nodes = new Cache<>(memory / 10);
        final Cache<Value, List<Entity>> adjacent = new Cache<>(memory / 10);
        Lookup(Plan plan) { this.plan = plan; }

        Map<Value, Entity> nodes(Collection<Value> ids) {
            var result = new LinkedHashMap<Value, Entity>();
            var fresh = new LinkedHashSet<Value>();
            for (Value id : ids) {
                if (nodes.contains(id)) result.put(id, nodes.get(id));
                else fresh.add(id);
            }
            long retained = result.values().stream().mapToLong(SpillPathQuery::size).sum();
            for (var batch : batches(fresh)) {
                var fetched = reads.nodes(graph, batch, scope);
                for (Value id : batch) {
                    var entity = fetched.get(id);
                    retained += size(entity) + id.getSerializedSize();
                    guard(retained);
                    nodes.put(id, entity, size(entity) + id.getSerializedSize());
                    result.put(id, entity);
                }
            }
            return result;
        }

        Map<Value, List<Entity>> edges(Collection<Value> ids) {
            var result = new LinkedHashMap<Value, List<Entity>>();
            var fresh = new ArrayList<Value>();
            for (Value id : ids) {
                var cached = adjacent.get(id);
                if (cached == null) fresh.add(id);
                else result.put(id, cached);
            }
            if (!fresh.isEmpty()) {
                var fetched = reads.neighborsMany(graph, fresh, plan.edge().direction() == Stmt.EdgePat.Direction.OUT,
                        plan.edge().typeNames().getFirst(), scope, threads);
                for (Value id : fresh) {
                    var unique = new LinkedHashMap<Value, Entity>();
                    for (Entity edge : fetched.getOrDefault(id, List.of())) unique.putIfAbsent(edge.getId(), edge);
                    var edges = List.copyOf(unique.values());
                    adjacent.put(id, edges, edges.stream().mapToLong(SpillPathQuery::size).sum() + 96);
                    result.put(id, edges);
                }
            }
            guard(result.values().stream().flatMap(Collection::stream).mapToLong(SpillPathQuery::size).sum());
            return result;
        }
    }

    private static final class Cache<K,V> {
        private record Entry<V>(V value, long size) {}
        final long limit;
        long bytes;
        final LinkedHashMap<K, Entry<V>> entries = new LinkedHashMap<>(16, 0.75f, true);
        Cache(long limit) { this.limit = limit; }
        boolean contains(K key) { return entries.containsKey(key); }
        V get(K key) { var entry = entries.get(key); return entry == null ? null : entry.value(); }
        void put(K key, V value, long size) {
            if (size > limit) return;
            var before = entries.put(key, new Entry<>(value, size));
            bytes += size - (before == null ? 0 : before.size());
            while (bytes > limit) bytes -= entries.pollFirstEntry().getValue().size();
        }
    }

    private static final class Store implements AutoCloseable {
        final RandomAccessFile file;
        final DataOutputStream output;
        long position;
        Store(SpillWorkspace workspace) throws IOException {
            Path path = workspace.file();
            output = workspace.output(path);
            try { file = new RandomAccessFile(path.toFile(), "r"); }
            catch (IOException ex) {
                try { output.close(); } catch (IOException failure) { ex.addSuppressed(failure); }
                throw ex;
            }
        }
        long position() { return position; }
        long add(byte[] item) throws IOException {
            long offset = position;
            output.writeInt(item.length);
            output.write(item);
            position += item.length + 4;
            return offset;
        }
        byte[] get(long offset) throws IOException {
            output.flush();
            file.seek(offset);
            int length = file.readInt();
            if (length < 0 || length > position - offset - 4) throw new IOException("临时记录长度异常");
            var item = new byte[length];
            file.readFully(item);
            return item;
        }
        void addLong(long item) throws IOException {
            output.writeLong(item);
            position += 8;
        }
        long getLong(long offset) throws IOException { output.flush(); file.seek(offset); return file.readLong(); }
        @Override public void close() throws IOException {
            try { output.close(); } finally { file.close(); }
        }
    }
}
