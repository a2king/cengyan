package io.cengyan.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.protocol.EntityId;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.IdType;
import io.cengyan.protocol.proto.ClusterProtocol.PropertyDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.TypeDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import io.cengyan.distributed.EntityCodec;
import io.cengyan.raft.ImportSst;
import org.rocksdb.RocksDB;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 批量导入客户端。一次命令只导入一种点或一种边。 */
public final class CengyanClient {
    static {
        // ORC 1.9 的 protobuf 代码较旧，运行时会打印拒绝服务警告。默认关掉，仍可用启动参数改回去。
        String silence = "com.google.protobuf.use_unsafe_pre22_gencode";
        if (System.getProperty(silence) == null) System.setProperty(silence, "true");
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int CHUNK = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !"import".equals(args[0])) {
            Console.run(args);
            return;
        }
        int code = 2;
        try {
            code = new CengyanClient().run(args);
        } catch (Exception ex) {
            System.err.println(ex.getMessage() == null ? ex.toString() : ex.getMessage());
            code = 1;
        }
        System.exit(code);
    }

    int run(String[] args) throws Exception {
        Options options = Options.parse(args);
        List<Path> files = ImportFiles.expand(options.files);
        RocksDB.loadLibrary();
        long startedAt = System.currentTimeMillis();
        LocalDateTime started = LocalDateTime.now();
        AtomicLong completed = new AtomicLong();
        AtomicLong skipped = new AtomicLong();
        AtomicLong networkRetries = new AtomicLong();
        long[] progressSample = { startedAt, 0 };
        String failure = null;
        AtomicReference<Throwable> broken = new AtomicReference<>();
        String session = UUID.randomUUID().toString();
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "import-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        ScheduledExecutorService progress = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "import-progress");
            thread.setDaemon(true);
            return thread;
        });
        try (LineClient client = LineClient.connect(options.nodes)) {
            client.login(options.username, options.password);
            heartbeat.scheduleAtFixedRate(() -> {
                if (broken.get() != null) return;
                try { client.call(JSON.createObjectNode().put("op", "importHeartbeat").put("sessionId", session), 30_000); }
                catch (Exception ignored) { /* 下一次心跳再试。超过 30 分钟没有成功心跳时，副本会删除暂存文件。 */ }
            }, 0, 20, TimeUnit.SECONDS);
            progress.scheduleAtFixedRate(() -> printProgress(startedAt, progressSample, completed, skipped, networkRetries), 60, 60, TimeUnit.SECONDS);
            JsonNode plan = client.call(JSON.createObjectNode().put("op", "importPlan").put("graph", options.graph)
                    .put("node", options.node).put("type", options.typeName), 60_000);
            GraphDefinition graph = graph(plan, options);
            Path work = Files.createTempDirectory(Path.of("").toAbsolutePath(), ".cengyan-import-");
            ImportIds ids = null;
            ExecutorService pool = null;
            List<LineClient> uploads = new ArrayList<>();
            List<Future<Void>> tasks = new ArrayList<>();
            try {
                ids = new ImportIds(work.resolve("ids"));
                pool = Executors.newFixedThreadPool(options.threads);
                BlockingQueue<LineClient> idle = new ArrayBlockingQueue<>(options.threads);
                for (int i = 0; i < options.threads; i++) {
                    LineClient extra = LineClient.connect(options.nodes);
                    uploads.add(extra);
                    extra.login(options.username, options.password);
                    idle.add(extra);
                }
                ImportBatches batches = new ImportBatches(options.batchBytes, options.batchBytes * 4);
                ExecutorService workers = pool;
                for (Path file : files) {
                    if (broken.get() != null) break;
                    ImportIds seen = ids;
                    ImportFiles.read(options.fileType, file, options.delimiter, record -> {
                        if (broken.get() != null) throw new IllegalStateException(text(broken.get()));
                        String id = required(record, options.idColumn, file);
                        try {
                            if (!seen.add(id)) {
                                skipped.incrementAndGet();
                                return;
                            }
                            List<ImportBatches.Ready> ready = new ArrayList<>();
                            place(graph, options, record, id, batches, ready);
                            for (ImportBatches.Ready batch : ready) submit(workers, idle, tasks, broken, completed, skipped, networkRetries, options, plan, session, work, batch);
                        } catch (RuntimeException ex) { throw ex; }
                        catch (Exception ex) { throw new IllegalStateException(ex.getMessage(), ex); }
                    });
                }
                if (broken.get() == null) {
                    for (ImportBatches.Ready batch : batches.finish()) {
                        submit(workers, idle, tasks, broken, completed, skipped, networkRetries, options, plan, session, work, batch);
                    }
                }
                awaitUploads(tasks, broken);
            } catch (Exception ex) {
                broken.compareAndSet(null, ex);
            } finally {
                if (broken.get() != null) {
                    heartbeat.shutdownNow();
                    abortUploads(pool, tasks, uploads);
                } else {
                    for (LineClient extra : uploads) closeQuietly(extra);
                    if (pool != null) pool.shutdown();
                }
                if (ids != null) ids.close();
                deleteTree(work);
                if (plan != null && broken.get() == null) {
                    try {
                        client.call(JSON.createObjectNode().put("op", "importFinish").put("graph", options.graph).put("sessionId", session), 1_800_000);
                    } catch (Exception ex) {
                        broken.compareAndSet(null, ex);
                    }
                }
            }
        } catch (Exception ex) {
            broken.compareAndSet(null, ex);
        } finally {
            heartbeat.shutdownNow();
            progress.shutdownNow();
        }
        if (broken.get() != null && failure == null) failure = text(broken.get());
        printReport(started, LocalDateTime.now(), System.currentTimeMillis() - startedAt, completed.get(), skipped.get(), session, failure);
        if (failure != null) throw new IllegalStateException(failure);
        return 0;
    }

    /** 成功时等全部批次结束。失败时不等，避免卡住后仍去结束会话。 */
    private static void awaitUploads(List<Future<Void>> tasks, AtomicReference<Throwable> broken) throws Exception {
        for (Future<Void> task : tasks) {
            if (broken.get() != null) return;
            try { task.get(); }
            catch (Exception ex) {
                Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
                broken.compareAndSet(null, cause);
                return;
            }
        }
    }

    /** 关掉正在上传的连接，让卡住的请求马上失败。已经写入的数据保留，未提交的暂存文件等心跳超时后由存储删除。 */
    private static void abortUploads(ExecutorService pool, List<Future<Void>> tasks, List<LineClient> uploads) {
        for (Future<Void> task : tasks) task.cancel(true);
        if (pool != null) pool.shutdownNow();
        for (LineClient extra : uploads) closeQuietly(extra);
        if (pool != null) {
            try { pool.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        }
    }

    private static void closeQuietly(LineClient client) {
        try { client.close(); }
        catch (Exception ignored) { /* 连接关闭失败不影响已经返回的导入结果。 */ }
    }

    private static void submit(ExecutorService pool, BlockingQueue<LineClient> idle, List<Future<Void>> tasks, AtomicReference<Throwable> broken,
                               AtomicLong completed, AtomicLong skipped, AtomicLong networkRetries, Options options, JsonNode plan, String session, Path work, ImportBatches.Ready batch) throws InterruptedException {
        if (broken.get() != null || batch.rows.isEmpty()) return;
        LineClient worker = idle.take();
        LineClient[] holder = { worker };
        try {
            tasks.add(pool.submit(() -> {
                try {
                    long[] counts = upload(holder, options, plan, session, work, batch.part, batch.rows, batch.primaries, networkRetries, broken);
                    completed.addAndGet(counts[0]);
                    skipped.addAndGet(counts[1]);
                } catch (Exception ex) {
                    broken.compareAndSet(null, ex);
                } finally {
                    idle.put(holder[0]);
                }
                return null;
            }));
        } catch (RuntimeException ex) {
            idle.put(worker);
            throw ex;
        }
    }

    private static long[] upload(LineClient[] client, Options options, JsonNode plan, String session, Path work, int part, List<ImportSst.Row> rows, int primaries, AtomicLong networkRetries, AtomicReference<Throwable> broken) throws Exception {
        Path file = Files.createTempFile(work, "part-" + part + "-", ".sst");
        try {
            ImportSst.write(file, rows);
            byte[] checksum = sha256(file);
            String hex = hex(checksum);
            try (InputStream input = Files.newInputStream(file)) {
                byte[] buffer = new byte[CHUNK];
                byte[] pending = null;
                long offset = 0;
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (pending != null) {
                        stage(client, options, plan, session, part, hex, offset, pending, false, networkRetries, broken);
                        offset += pending.length;
                    }
                    pending = java.util.Arrays.copyOf(buffer, read);
                }
                if (pending != null) stage(client, options, plan, session, part, hex, offset, pending, true, networkRetries, broken);
            }
            ObjectNode commit = JSON.createObjectNode().put("op", "importCommit").put("graphId", plan.path("graphId").asLong())
                    .put("part", part).put("sessionId", session).put("checksum", hex).put("conflict", options.conflict)
                    .put("generation", plan.path("generation").asLong());
            JsonNode result = callBatch(client, options, commit, 1_800_000, part, "提交", networkRetries, broken);
            long affected = result.path("affected").asLong();
            if ("IMPORT_REPLACE".equals(options.conflict) || "replace".equals(options.conflict)) return new long[] {primaries, 0};
            return new long[] {affected, Math.max(0, primaries - affected)};
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static void stage(LineClient[] client, Options options, JsonNode plan, String session, int part, String checksum, long offset, byte[] data, boolean last, AtomicLong networkRetries, AtomicReference<Throwable> broken) throws Exception {
        ObjectNode request = JSON.createObjectNode().put("op", "importStage").put("graphId", plan.path("graphId").asLong())
                .put("part", part).put("sessionId", session).put("checksum", checksum).put("offset", offset)
                .put("last", last).put("data", Base64.getEncoder().encodeToString(data));
        callBatch(client, options, request, 180_000, part, "分块 " + offset, networkRetries, broken);
    }

    /** 只重试这一批的这一次请求。同一校验和的提交由服务端按请求号去重。导入已经失败时不再重试。 */
    private static JsonNode callBatch(LineClient[] client, Options options, ObjectNode request, int timeoutMs, int part, String step, AtomicLong networkRetries, AtomicReference<Throwable> broken) throws Exception {
        for (int attempt = 0; ; attempt++) {
            if (broken.get() != null) throw new IllegalStateException(text(broken.get()));
            try {
                return client[0].call(request, timeoutMs);
            } catch (Exception ex) {
                String message = ex.getMessage() == null ? "" : ex.getMessage();
                if (attempt > 0 && message.contains("导入分块偏移不连续")) return JSON.createObjectNode().put("code", "OK");
                if (broken.get() != null || attempt >= options.retries || !retryable(message)) throw ex;
                networkRetries.incrementAndGet();
                System.out.println(LocalDateTime.now().format(CLOCK) + " INFO  CengyanClient - 分片 " + part + " " + step
                        + " 失败，" + options.retryIntervalSeconds + " 秒后重试，剩余 " + (options.retries - attempt) + " 次：" + message);
                if (options.retryIntervalSeconds > 0) Thread.sleep(options.retryIntervalSeconds * 1000L);
                if (message.contains("关闭了连接")) client[0] = reconnect(client[0], options);
            }
        }
    }

    static boolean retryable(String message) {
        if (message == null || message.isBlank()) return true;
        if (message.startsWith("FAILED")) return true;
        return message.contains("超时") || message.contains("连接") || message.contains("RPC") || message.contains("中断");
    }

    private static LineClient reconnect(LineClient dead, Options options) throws Exception {
        try { dead.close(); }
        catch (Exception ignored) { /* 旧连接已经不可用。 */ }
        LineClient client = LineClient.connect(options.nodes);
        client.login(options.username, options.password);
        return client;
    }

    private static byte[] sha256(Path file) throws Exception {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[CHUNK];
            int read;
            while ((read = input.read(buffer)) >= 0) digest.update(buffer, 0, read);
        }
        return digest.digest();
    }

    private static void place(GraphDefinition graph, Options options, Map<String, String> record, String id,
                              ImportBatches batches, List<ImportBatches.Ready> ready) throws Exception {
        boolean node = options.node;
        Value identity = typed(id, node ? graph.getVertexIdType() : graph.getEdgeIdType());
        Entity.Builder entity = Entity.newBuilder().setNode(node).setType(options.typeName).setId(identity).setState("ACTIVE");
        if (!node) {
            entity.setSource(typed(required(record, options.sourceColumn, null), graph.getVertexIdType()));
            entity.setTarget(typed(required(record, options.targetColumn, null), graph.getVertexIdType()));
        }
        for (String property : options.properties) {
            PropertyDefinition field = graph.getTypes(0).getPropertiesList().stream().filter(item -> item.getName().equals(property))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("属性未定义：" + property));
            entity.putProperties(property, propertyValue(record.getOrDefault(property, ""), field));
        }
        Entity encoded = EntityCodec.validate(entity.build(), graph);
        byte[] key = EntityCodec.key(node, encoded.getId(), graph).toByteArray();
        int part = EntityCodec.part(node, encoded.getId(), graph);
        ready.addAll(batches.add(part, new ImportSst.Row(key, encoded.toByteArray()), true));
        if (!node) {
            byte[] edge = canonical(identity);
            byte[] source = canonical(encoded.getSource());
            byte[] target = canonical(encoded.getTarget());
            long generation = graph.getGeneration();
            ready.addAll(addAdjacency(graph, batches, true, source, edge, generation, encoded));
            ready.addAll(addAdjacency(graph, batches, false, target, edge, generation, encoded));
        }
    }

    private static List<ImportBatches.Ready> addAdjacency(GraphDefinition graph, ImportBatches batches, boolean outgoing,
                                                          byte[] vertex, byte[] edge, long generation, Entity encoded) {
        Value endpoint = outgoing ? encoded.getSource() : encoded.getTarget();
        int part = EntityCodec.part(true, endpoint, graph);
        byte[] key = ImportSst.adjacency(outgoing, generation, vertex, edge);
        Entity hint = encoded.toBuilder().clearProperties().build();
        return batches.add(part, new ImportSst.Row(key, hint.toByteArray()), false);
    }

    private static GraphDefinition graph(JsonNode plan, Options options) {
        TypeDefinition.Builder type = TypeDefinition.newBuilder().setName(options.typeName).setNode(options.node)
                .setIdType(IdType.valueOf(plan.path(options.node ? "vertexIdType" : "edgeIdType").asText("STRING")));
        for (JsonNode property : plan.path("properties")) {
            type.addProperties(PropertyDefinition.newBuilder().setName(property.path("name").asText())
                    .setType(property.path("type").asText()).setNullable(property.path("nullable").asBoolean()));
        }
        return GraphDefinition.newBuilder().setId(plan.path("graphId").asLong()).setName(options.graph)
                .setPartitions(plan.path("partitions").asInt()).setGeneration(plan.path("generation").asLong())
                .setVertexIdType(IdType.valueOf(plan.path("vertexIdType").asText()))
                .setEdgeIdType(IdType.valueOf(plan.path("edgeIdType").asText()))
                .addTypes(type).build();
    }

    private static Value typed(String text, IdType type) {
        if (type == IdType.INT64) return Value.newBuilder().setIntValue(Long.parseLong(text.trim())).build();
        return Value.newBuilder().setStringValue(text).build();
    }

    private static Value propertyValue(String text, PropertyDefinition field) {
        if (text == null || text.isEmpty()) return Value.newBuilder().setNullValue(true).build();
        return switch (field.getType().toUpperCase(java.util.Locale.ROOT)) {
            case "INT", "INT64" -> Value.newBuilder().setIntValue(Long.parseLong(text.trim())).build();
            case "DOUBLE" -> Value.newBuilder().setDoubleValue(Double.parseDouble(text.trim())).build();
            case "BOOLEAN", "BOOL" -> Value.newBuilder().setBoolValue(Boolean.parseBoolean(text.trim())).build();
            default -> Value.newBuilder().setStringValue(text).build();
        };
    }

    private static byte[] canonical(Value value) {
        return value.getKindCase() == Value.KindCase.INT_VALUE
                ? new EntityId.Int64(value.getIntValue()).canonicalBytes()
                : new EntityId.Text(value.getStringValue()).canonicalBytes();
    }

    private static String required(Map<String, String> record, String column, Path file) {
        String value = record.get(column);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("缺少列 " + column + (file == null ? "" : "：" + file));
        return value;
    }

    private static void printProgress(long startedAt, long[] sample, AtomicLong completed, AtomicLong skipped, AtomicLong networkRetries) {
        long now = System.currentTimeMillis();
        long done = completed.get();
        long recent = recentPerSecond(done, sample[1], now - sample[0]);
        sample[0] = now;
        sample[1] = done;
        System.out.println(progressLine(LocalDateTime.now(), Math.max(1, (now - startedAt) / 1000), done, skipped.get(), recent, networkRetries.get()));
    }

    /** 最近一个进度窗口内的速度。窗口按进度间隔约 60 秒。 */
    static long recentPerSecond(long completed, long previousCompleted, long windowMillis) {
        long seconds = Math.max(1, windowMillis / 1000);
        return Math.max(0, completed - previousCompleted) / seconds;
    }

    static String progressLine(LocalDateTime now, long elapsedSeconds, long completed, long skipped, long recentPerSecond, long networkRetries) {
        long seconds = Math.max(1, elapsedSeconds);
        return now.format(CLOCK) + " INFO  CengyanClient - 完成总条数 " + completed + " 条 | 总跳过 " + skipped
                + " 条 | 平均导入速度 " + (completed / seconds) + " 条/s | 最新导入速度 " + recentPerSecond
                + " 条/s | 网络异常重试次数 " + networkRetries + " 次";
    }

    private static void printReport(LocalDateTime started, LocalDateTime ended, long elapsedMs, long completed, long skipped, String session, String failure) {
        System.out.println(ended.format(CLOCK) + " INFO  CengyanClient -");
        for (String line : reportLines(started, ended, elapsedMs, completed, skipped, session, failure)) System.out.println(line);
    }

    static List<String> reportLines(LocalDateTime started, LocalDateTime ended, long elapsedMs, long completed, long skipped, String session, String failure) {
        long seconds = Math.max(1, elapsedMs / 1000);
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[] {"起始时间", started.format(CLOCK)});
        rows.add(new String[] {"结束时间", ended.format(CLOCK)});
        rows.add(new String[] {"总耗时", seconds + " s"});
        rows.add(new String[] {"完成条数", completed + " 条"});
        rows.add(new String[] {"跳过条数", skipped + " 条"});
        rows.add(new String[] {"平均速度", (completed / seconds) + " 条/s"});
        rows.add(new String[] {"唯一索引构建时间", "0 s"});
        rows.add(new String[] {"快照分发时间", "0 s"});
        rows.add(new String[] {"会话号", session});
        int width = 0;
        for (String[] row : rows) width = Math.max(width, displayWidth(row[0]));
        if (failure != null) width = Math.max(width, displayWidth("导数发生错误"));
        List<String> lines = new ArrayList<>();
        for (String[] row : rows) lines.add(align(row[0], row[1], width));
        if (failure != null) {
            lines.add("");
            lines.add(align("导数发生错误", failure, width));
        }
        return lines;
    }

    private static String align(String label, String value, int width) {
        StringBuilder text = new StringBuilder(label);
        for (int pad = width - displayWidth(label); pad > 0; pad--) text.append(' ');
        return text.append(" : ").append(value).toString();
    }

    static int displayWidth(String text) {
        int width = 0;
        for (int i = 0; i < text.length(); ) {
            int code = text.codePointAt(i);
            width += code >= 0x1100 && (code <= 0x115F
                    || (code >= 0x2E80 && code <= 0xA4CF && code != 0x303F)
                    || (code >= 0xAC00 && code <= 0xD7A3)
                    || (code >= 0xF900 && code <= 0xFAFF)
                    || (code >= 0xFE10 && code <= 0xFE19)
                    || (code >= 0xFE30 && code <= 0xFE6F)
                    || (code >= 0xFF00 && code <= 0xFF60)
                    || (code >= 0xFFE0 && code <= 0xFFE6)
                    || (code >= 0x20000 && code <= 0x3FFFD)) ? 2 : 1;
            i += Character.charCount(code);
        }
        return width;
    }

    private static String text(Throwable failure) {
        return failure.getMessage() == null ? failure.toString() : failure.getMessage();
    }

    private static void deleteTree(Path root) {
        if (root == null || !Files.exists(root)) return;
        try (var walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        } catch (Exception ignored) { /* 工作目录清不掉时留在当前目录，不掩盖导入结果。 */ }
    }

    private static String hex(byte[] bytes) {
        StringBuilder text = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            text.append(Character.forDigit((value >> 4) & 0xf, 16)).append(Character.forDigit(value & 0xf, 16));
        }
        return text.toString();
    }

    static final class Options {
        final List<InetSocketAddress> nodes;
        final String username;
        final String password;
        final String graph;
        final String fileType;
        final String files;
        final char delimiter;
        final String conflict;
        final int threads;
        final long batchBytes;
        final int retries;
        final int retryIntervalSeconds;
        final boolean node;
        final String typeName;
        final String idColumn;
        final String sourceColumn;
        final String targetColumn;
        final List<String> properties;

        private Options(List<InetSocketAddress> nodes, String username, String password, String graph, String fileType, String files,
                        char delimiter, String conflict, int threads, long batchBytes, int retries, int retryIntervalSeconds, boolean node, String typeName, String idColumn,
                        String sourceColumn, String targetColumn, List<String> properties) {
            this.nodes = nodes;
            this.username = username;
            this.password = password;
            this.graph = graph;
            this.fileType = fileType;
            this.files = files;
            this.delimiter = delimiter;
            this.conflict = conflict;
            this.threads = threads;
            this.batchBytes = batchBytes;
            this.retries = retries;
            this.retryIntervalSeconds = retryIntervalSeconds;
            this.node = node;
            this.typeName = typeName;
            this.idColumn = idColumn;
            this.sourceColumn = sourceColumn;
            this.targetColumn = targetColumn;
            this.properties = properties;
        }

        static Options parse(String[] args) {
            if (args.length == 0 || !"import".equals(args[0])) throw new IllegalArgumentException("用法：cengyan-client import [参数]");
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 1; i < args.length; i++) {
                if (!args[i].startsWith("--")) throw new IllegalArgumentException("无法识别的参数：" + args[i]);
                if (i + 1 >= args.length) throw new IllegalArgumentException("参数缺少取值：" + args[i]);
                values.put(args[i], args[++i]);
            }
            String nodes = required(values, "--compute-nodes");
            List<InetSocketAddress> addresses = new ArrayList<>();
            for (String item : nodes.split(",")) {
                String[] hostPort = item.trim().split(":");
                if (hostPort.length != 2) throw new IllegalArgumentException("计算节点必须是 host:port：" + item);
                addresses.add(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
            }
            boolean node = values.containsKey("--node");
            if (node == values.containsKey("--edge")) throw new IllegalArgumentException("一次导入只能指定 --node 或 --edge");
            String type = values.get(node ? "--node" : "--edge");
            String fileType = required(values, "--file-type");
            if (!fileType.equals("tsv") && !fileType.equals("parquet") && !fileType.equals("orc")) {
                throw new IllegalArgumentException("--file-type 只能是 parquet、orc 或 tsv");
            }
            String conflict = values.getOrDefault("--unq", "ignore");
            if (!conflict.equals("ignore") && !conflict.equals("replace") && !conflict.equals("error")) {
                throw new IllegalArgumentException("--unq 只能是 ignore、replace 或 error");
            }
            int threads = Integer.parseInt(values.getOrDefault("--thread", "4"));
            if (threads < 1) throw new IllegalArgumentException("--thread 必须是正整数");
            int batchMb = Integer.parseInt(values.getOrDefault("--batch-mb", "64"));
            if (batchMb < 1) throw new IllegalArgumentException("--batch-mb 必须是正整数");
            int retries = Integer.parseInt(values.getOrDefault("--retry", "3"));
            if (retries < 0) throw new IllegalArgumentException("--retry 不能是负数");
            int retryInterval = Integer.parseInt(values.getOrDefault("--retry-interval", "5"));
            if (retryInterval < 0) throw new IllegalArgumentException("--retry-interval 不能是负数");
            String delimiterText = values.getOrDefault("--delimiter", "\t");
            if ("\\t".equals(delimiterText)) delimiterText = "\t";
            if (delimiterText.length() != 1) throw new IllegalArgumentException("--delimiter 只能是一个字符");
            List<String> properties = new ArrayList<>();
            if (values.containsKey("--parameter") && !values.get("--parameter").isBlank()) {
                for (String name : values.get("--parameter").split(",")) if (!name.isBlank()) properties.add(name.trim());
            }
            if (!node && (!values.containsKey("--src-id") || !values.containsKey("--dst-id"))) {
                throw new IllegalArgumentException("边导入必须提供 --src-id 和 --dst-id");
            }
            return new Options(addresses, required(values, "--username"), required(values, "--password"), required(values, "--graph-space"),
                    fileType, required(values, "--files"), delimiterText.charAt(0), conflict.equals("ignore") ? "IMPORT_IGNORE" : conflict.equals("replace") ? "IMPORT_REPLACE" : "IMPORT_ERROR",
                    threads, batchMb * 1024L * 1024L, retries, retryInterval, node, type, required(values, "--id"), values.get("--src-id"), values.get("--dst-id"), properties);
        }

        private static String required(Map<String, String> values, String name) {
            String value = values.get(name);
            if (value == null || value.isBlank()) throw new IllegalArgumentException("缺少参数 " + name);
            return value;
        }
    }

    static final class LineClient implements AutoCloseable {
        private final Socket socket;
        private final BufferedWriter writer;
        private final BufferedReader reader;
        private String token = "";

        private LineClient(Socket socket) throws Exception {
            this.socket = socket;
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        }

        static LineClient connect(List<InetSocketAddress> nodes) throws Exception {
            Exception last = null;
            for (InetSocketAddress node : nodes) {
                try {
                    Socket socket = new Socket();
                    socket.connect(node, 3000);
                    socket.setTcpNoDelay(true);
                    return new LineClient(socket);
                } catch (Exception ex) { last = ex; }
            }
            throw new IllegalStateException("无法连接计算节点", last);
        }

        void login(String user, String password) throws Exception {
            JsonNode response = call(JSON.createObjectNode().put("op", "login").put("user", user).put("password", password), 30_000);
            token = response.path("token").asText(response.path("session").asText());
            if (token.isBlank()) throw new IllegalStateException("登录没有返回令牌");
        }

        synchronized JsonNode call(ObjectNode request, int timeoutMs) throws Exception {
            if (!token.isBlank()) request.put("token", token);
            request.put("requestId", UUID.randomUUID().toString());
            request.put("timeoutMs", timeoutMs);
            writer.write(JSON.writeValueAsString(request));
            writer.write('\n');
            writer.flush();
            String line = reader.readLine();
            if (line == null) throw new IllegalStateException("计算节点关闭了连接");
            JsonNode response = JSON.readTree(line);
            if (!"OK".equals(response.path("code").asText())) {
                throw new IllegalStateException(response.path("code").asText() + " " + response.path("message").asText());
            }
            return response;
        }

        @Override public void close() throws Exception { socket.close(); }
    }
}
