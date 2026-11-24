package io.cengyan.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 批量导入客户端。一次命令只导入一种点或一种边，分三步：
 * <ol>
 *   <li>上传：读文件、去重、按分片攒成有序的上传段，传给分片的每个副本。上传段只进副本的暂存区，查询看不到；</li>
 *   <li>归并：所有分片上传完以后，各副本把上传段归并成可摄入的文件。任何一个分片归并失败，整次导入都不生效；</li>
 *   <li>发布：每个分片一条 Raft 指令，各副本一次性摄入。</li>
 * </ol>
 */
public final class CengyanClient {
    static {
        // ORC 1.9 的 protobuf 代码较旧，运行时会打印拒绝服务警告。默认关掉，仍可用启动参数改回去。
        String silence = "com.google.protobuf.use_unsafe_pre22_gencode";
        if (System.getProperty(silence) == null) System.setProperty(silence, "true");
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /** 4 MiB 原始数据，Base64 后约 5.6 MiB，低于 Compute 8 MiB 的单行上限。 */
    private static final int CHUNK = 4 * 1024 * 1024;

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

    /** 导入过程中的计数，进度线程和工作线程共用。 */
    private static final class Progress {
        final long startedAt = System.currentTimeMillis();
        final AtomicLong uploaded = new AtomicLong();
        final AtomicLong published = new AtomicLong();
        final AtomicLong skipped = new AtomicLong();
        final AtomicLong replaced = new AtomicLong();
        final AtomicLong retries = new AtomicLong();
        final AtomicInteger partsDone = new AtomicInteger();
        volatile int partsTotal;
        volatile String phase = "上传";
        final long[] sample = { startedAt, 0 };
    }

    int run(String[] args) throws Exception {
        Options options = Options.parse(args);
        List<Path> files = ImportFiles.expand(options.files);
        RocksDB.loadLibrary();
        LocalDateTime started = LocalDateTime.now();
        Progress progress = new Progress();
        long uploadMs = 0, publishMs = 0;
        String failure = null;
        AtomicReference<Throwable> broken = new AtomicReference<>();
        String session = UUID.randomUUID().toString();
        options.session = session;
        ScheduledExecutorService heartbeat = daemonScheduler("import-heartbeat");
        ScheduledExecutorService reporter = daemonScheduler("import-progress");
        try (LineClient client = LineClient.connect(options.nodes)) {
            client.login(options.username, options.password);
            JsonNode plan = client.call(JSON.createObjectNode().put("op", "importPlan").put("graph", options.graph)
                    .put("node", options.node).put("type", options.typeName), 60_000);
            // 心跳一直发到导入结束：归并可能要几分钟，暂存区 30 分钟收不到心跳就会被删掉。
            heartbeat.scheduleAtFixedRate(() -> {
                try { client.call(JSON.createObjectNode().put("op", "importHeartbeat").put("sessionId", session), 30_000); }
                catch (Exception ignored) { /* 下一次心跳再试。 */ }
            }, 0, 20, TimeUnit.SECONDS);
            reporter.scheduleAtFixedRate(() -> printProgress(progress), 60, 60, TimeUnit.SECONDS);
            GraphDefinition graph = graph(plan, options);
            Path work = Files.createTempDirectory(Path.of("").toAbsolutePath(), ".cengyan-import-");
            List<LineClient> connections = new ArrayList<>();
            Map<Integer, List<String>> runs = new ConcurrentHashMap<>();
            try {
                BlockingQueue<LineClient> idle = new ArrayBlockingQueue<>(options.threads);
                for (int i = 0; i < options.threads; i++) {
                    LineClient extra = LineClient.connect(options.nodes);
                    connections.add(extra);
                    extra.login(options.username, options.password);
                    idle.add(extra);
                }
                long phaseStart = System.currentTimeMillis();
                uploadAll(options, files, plan, graph, session, work, idle, runs, progress, broken);
                uploadMs = System.currentTimeMillis() - phaseStart;
                if (broken.get() == null) {
                    phaseStart = System.currentTimeMillis();
                    publishAll(options, plan, session, idle, runs, progress, broken);
                    publishMs = System.currentTimeMillis() - phaseStart;
                }
            } catch (Exception ex) {
                broken.compareAndSet(null, ex);
            } finally {
                for (LineClient extra : connections) closeQuietly(extra);
                deleteTree(work);
                // 成功或失败都清掉各副本上的暂存文件。还没进入发布阶段就失败时，归并结果也不会再有人用，一起删掉；
                // 进入发布阶段以后，落后的副本可能还没应用发布指令，归并结果由副本自己在应用后删除。
                boolean abort = broken.get() != null && !progress.phase.equals("发布");
                try { client.call(JSON.createObjectNode().put("op", "importFinish").put("graph", options.graph).put("sessionId", session)
                        .put("abort", abort), 1_800_000); }
                catch (Exception ex) {
                    if (broken.get() == null) System.out.println(LocalDateTime.now().format(CLOCK) + " WARN  CengyanClient - 清理暂存文件失败，"
                            + "Storage 会在 30 分钟后自动删除：" + text(ex));
                }
            }
        } catch (Exception ex) {
            broken.compareAndSet(null, ex);
        } finally {
            heartbeat.shutdownNow();
            reporter.shutdownNow();
        }
        if (broken.get() != null) failure = text(broken.get());
        printReport(started, LocalDateTime.now(), System.currentTimeMillis() - progress.startedAt, progress, uploadMs, publishMs, session, failure);
        if (failure != null) throw new IllegalStateException(failure);
        return 0;
    }

    /** 读全部文件，按分片攒成上传段并上传。失败时记到 broken 并尽快停下。 */
    private static void uploadAll(Options options, List<Path> files, JsonNode plan, GraphDefinition graph, String session, Path work,
                                  BlockingQueue<LineClient> idle, Map<Integer, List<String>> runs, Progress progress,
                                  AtomicReference<Throwable> broken) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(options.threads);
        List<Future<?>> tasks = Collections.synchronizedList(new ArrayList<>());
        ImportIds ids = null;
        try {
            ids = new ImportIds(work.resolve("ids"));
            ImportBatches batches = new ImportBatches(options.batchBytes, options.bufferBytes);
            ImportIds seen = ids;
            for (Path file : files) {
                if (broken.get() != null) break;
                ImportFiles.read(options.fileType, file, options.delimiter, record -> {
                    if (broken.get() != null) throw new IllegalStateException(text(broken.get()));
                    String id = required(record, options.idColumn, file);
                    try {
                        if (!seen.add(id)) {
                            progress.skipped.incrementAndGet();
                            return;
                        }
                        List<ImportBatches.Ready> ready = new ArrayList<>();
                        place(graph, options, record, id, batches, ready);
                        for (ImportBatches.Ready batch : ready) submit(pool, idle, tasks, options, plan, session, work, batch, runs, progress, broken);
                    } catch (RuntimeException ex) { throw ex; }
                    catch (Exception ex) { throw new IllegalStateException(ex.getMessage(), ex); }
                });
            }
            if (broken.get() == null) {
                for (ImportBatches.Ready batch : batches.finish()) submit(pool, idle, tasks, options, plan, session, work, batch, runs, progress, broken);
            }
            await(tasks, broken);
        } catch (Exception ex) {
            broken.compareAndSet(null, ex);
        } finally {
            if (broken.get() != null) for (Future<?> task : List.copyOf(tasks)) task.cancel(true);
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
            if (ids != null) ids.close();
        }
    }

    private static void submit(ExecutorService pool, BlockingQueue<LineClient> idle, List<Future<?>> tasks, Options options, JsonNode plan,
                               String session, Path work, ImportBatches.Ready batch, Map<Integer, List<String>> runs, Progress progress,
                               AtomicReference<Throwable> broken) throws InterruptedException {
        if (broken.get() != null || batch.rows.isEmpty()) return;
        LineClient worker = idle.take();
        LineClient[] holder = { worker };
        try {
            tasks.add(pool.submit(() -> {
                try {
                    String hex = upload(holder, options, plan, session, work, batch.part, batch.rows, progress, broken);
                    runs.computeIfAbsent(batch.part, ignored -> Collections.synchronizedList(new ArrayList<>())).add(hex);
                    progress.uploaded.addAndGet(batch.primaries);
                } catch (Exception ex) {
                    broken.compareAndSet(null, ex);
                } finally {
                    idle.offer(holder[0]);
                }
                return null;
            }));
        } catch (RuntimeException ex) {
            idle.offer(worker);
            throw ex;
        }
    }

    /** 等全部任务结束。有一个失败就不再等，其余任务由调用方取消。 */
    private static void await(List<Future<?>> tasks, AtomicReference<Throwable> broken) throws InterruptedException {
        for (Future<?> task : List.copyOf(tasks)) {
            if (broken.get() != null) return;
            try { task.get(); }
            catch (ExecutionException ex) {
                broken.compareAndSet(null, ex.getCause() == null ? ex : ex.getCause());
                return;
            }
        }
    }

    /** 写成一个有序上传段，按 4 MiB 分块传给分片的每个副本。 @return 上传段的 SHA-256 */
    private static String upload(LineClient[] client, Options options, JsonNode plan, String session, Path work, int part,
                                 List<ImportSst.Row> rows, Progress progress, AtomicReference<Throwable> broken) throws Exception {
        Path file = Files.createTempFile(work, "part-" + part + "-", ".sst");
        try {
            ImportSst.write(file, rows);
            String hex = ImportSst.hex(ImportSst.sha256(file));
            for (int restart = 0; ; restart++) {
                try {
                    sendChunks(client, options, plan, session, part, hex, file, progress, broken);
                    return hex;
                } catch (Exception ex) {
                    // 副本所在机器重启后可能丢了没 fsync 的半个上传段，从头再传一遍。
                    if (restart < 2 && String.valueOf(ex.getMessage()).contains("导入分块偏移不连续")) continue;
                    throw ex;
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static void sendChunks(LineClient[] client, Options options, JsonNode plan, String session, int part, String hex, Path file,
                                   Progress progress, AtomicReference<Throwable> broken) throws Exception {
        long size = Files.size(file);
        try (InputStream input = Files.newInputStream(file)) {
            long offset = 0;
            do {
                byte[] data = input.readNBytes((int) Math.min(CHUNK, size - offset));
                boolean last = offset + data.length >= size;
                ObjectNode request = JSON.createObjectNode().put("op", "importStage").put("graphId", plan.path("graphId").asLong())
                        .put("part", part).put("sessionId", session).put("checksum", hex).put("offset", offset)
                        .put("last", last).put("data", Base64.getEncoder().encodeToString(data));
                callBatch(client, options, request, 180_000, part, "分块 " + offset, progress, broken);
                offset += data.length;
            } while (offset < size);
        }
    }

    /**
     * 先对全部分片归并，全部成功再发布。归并时就会发现键序、类型、代次这类导入内容问题，
     * 那时一个分片都还没有生效。发布阶段才会发现的只有与已有数据冲突（--unq error）。
     */
    private static void publishAll(Options options, JsonNode plan, String session, BlockingQueue<LineClient> idle,
                                   Map<Integer, List<String>> runs, Progress progress, AtomicReference<Throwable> broken) throws Exception {
        TreeMap<Integer, List<String>> parts = new TreeMap<>(runs);
        progress.partsTotal = parts.size();
        if (parts.isEmpty()) return;
        progress.phase = "归并";
        forEachPart(parts, idle, options, broken, (client, part, request) ->
                callBatch(client, options, request.put("op", "importPrepare"), 1_800_000, part, "归并", progress, broken));
        if (broken.get() != null) return;
        progress.phase = "发布";
        List<String> failed = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        forEachPart(parts, idle, options, firstFailure, (client, part, request) -> {
            try {
                JsonNode result = callBatch(client, options, request.put("op", "importPublish"), 1_800_000, part, "发布", progress, firstFailure);
                progress.published.addAndGet(result.path("affected").asLong());
                progress.skipped.addAndGet(result.path("skipped").asLong());
                progress.replaced.addAndGet(result.path("replaced").asLong());
                progress.partsDone.incrementAndGet();
            } catch (Exception ex) {
                // 一个分片发布失败不拦着其他分片：已经发布的分片收不回来，剩下的也发布完，最后统一报告。
                failed.add("分片 " + part + "：" + text(ex));
            }
        });
        if (firstFailure.get() != null) broken.compareAndSet(null, firstFailure.get());
        else if (!failed.isEmpty()) {
            Collections.sort(failed);
            broken.compareAndSet(null, new IllegalStateException(progress.partsDone.get() + "/" + parts.size() + " 个分片已发布，"
                    + failed.size() + " 个分片发布失败。" + String.join("；", failed.subList(0, Math.min(5, failed.size())))
                    + (failed.size() > 5 ? " 等" : "")));
        }
    }

    private interface PartTask { void run(LineClient[] client, int part, ObjectNode request) throws Exception; }

    private static void forEachPart(TreeMap<Integer, List<String>> parts, BlockingQueue<LineClient> idle, Options options,
                                    AtomicReference<Throwable> broken, PartTask task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(options.threads);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (var entry : parts.entrySet()) {
                tasks.add(pool.submit(() -> {
                    if (broken.get() != null) return null;
                    LineClient[] holder = { idle.take() };
                    try {
                        ObjectNode request = JSON.createObjectNode().put("graphId", options.graphId).put("part", entry.getKey())
                                .put("sessionId", options.session).put("node", options.node).put("typeId", options.typeId)
                                .put("type", options.typeName).put("generation", options.generation).put("conflict", options.conflict);
                        ArrayNode list = request.putArray("runs");
                        synchronized (entry.getValue()) { for (String hex : entry.getValue()) list.add(hex); }
                        task.run(holder, entry.getKey(), request);
                    } catch (Exception ex) {
                        broken.compareAndSet(null, ex);
                    } finally { idle.offer(holder[0]); }
                    return null;
                }));
            }
            await(tasks, broken);
        } finally {
            if (broken.get() != null) for (Future<?> pending : tasks) pending.cancel(true);
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /** 只重试这一次请求。分块按偏移覆盖、归并和发布按会话幂等，重发都是安全的。导入已经失败时不再重试。 */
    private static JsonNode callBatch(LineClient[] client, Options options, ObjectNode request, int timeoutMs, int part, String step,
                                      Progress progress, AtomicReference<Throwable> broken) throws Exception {
        for (int attempt = 0; ; attempt++) {
            if (broken.get() != null) throw new IllegalStateException(text(broken.get()));
            try {
                return client[0].call(request, timeoutMs);
            } catch (Exception ex) {
                String message = ex.getMessage() == null ? "" : ex.getMessage();
                if (broken.get() != null || attempt >= options.retries || !retryable(message)) throw ex;
                progress.retries.incrementAndGet();
                System.out.println(LocalDateTime.now().format(CLOCK) + " INFO  CengyanClient - 分片 " + part + " " + step
                        + " 失败，" + options.retryIntervalSeconds + " 秒后重试，剩余 " + (options.retries - attempt) + " 次：" + message);
                if (options.retryIntervalSeconds > 0) Thread.sleep(options.retryIntervalSeconds * 1000L);
                if (!client[0].open()) client[0] = reconnect(client[0], options);
            }
        }
    }

    /** 集群暂时忙、超时、换主、连接断开可以重试；导入内容和参数错误不重试。 */
    static boolean retryable(String message) {
        if (message == null || message.isBlank()) return true;
        for (String code : List.of("FAILED", "BUSY", "QUEUE_TIMEOUT", "QUERY_TIMEOUT", "GROUP_UNAVAILABLE", "NOT_LEADER", "REPLICA_UNTRUSTED")) {
            if (message.startsWith(code + " ") || message.equals(code)) return true;
        }
        return message.contains("超时") || message.contains("连接") || message.contains("RPC") || message.contains("中断");
    }

    private static LineClient reconnect(LineClient dead, Options options) throws Exception {
        closeQuietly(dead);
        LineClient client = LineClient.connect(options.nodes);
        client.login(options.username, options.password);
        return client;
    }

    private static void closeQuietly(LineClient client) {
        try { client.close(); }
        catch (Exception ignored) { /* 连接关闭失败不影响已经返回的导入结果。 */ }
    }

    private static ScheduledExecutorService daemonScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
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
        byte[] key = EntityCodec.key(encoded, graph).toByteArray();
        int part = EntityCodec.part(node, encoded.getId(), graph);
        ready.addAll(batches.add(part, new ImportSst.Row(key, encoded.toByteArray()), true));
        if (!node) {
            byte[] hint = encoded.toBuilder().clearProperties().build().toByteArray();
            for (boolean outgoing : new boolean[] {true, false}) {
                Value endpoint = outgoing ? encoded.getSource() : encoded.getTarget();
                byte[] adjacency = EntityCodec.adjacency(outgoing, options.typeName, endpoint, encoded.getId(), graph).toByteArray();
                ready.addAll(batches.add(EntityCodec.part(true, endpoint, graph), new ImportSst.Row(adjacency, hint), false));
            }
        }
    }

    private static GraphDefinition graph(JsonNode plan, Options options) {
        int typeId = plan.path("typeId").asInt();
        if (typeId <= 0) throw new IllegalStateException("集群没有返回类型编号，请确认 Compute 与客户端版本一致");
        TypeDefinition.Builder type = TypeDefinition.newBuilder().setName(options.typeName).setNode(options.node).setId(typeId)
                .setIdType(IdType.valueOf(plan.path(options.node ? "vertexIdType" : "edgeIdType").asText("STRING")));
        for (JsonNode property : plan.path("properties")) {
            type.addProperties(PropertyDefinition.newBuilder().setName(property.path("name").asText())
                    .setType(property.path("type").asText()).setNullable(property.path("nullable").asBoolean()));
        }
        options.bind(plan);
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

    private static String required(Map<String, String> record, String column, Path file) {
        String value = record.get(column);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("缺少列 " + column + (file == null ? "" : "：" + file));
        return value;
    }

    private static void printProgress(Progress progress) {
        long now = System.currentTimeMillis();
        long elapsed = Math.max(1, (now - progress.startedAt) / 1000);
        if (progress.phase.equals("上传")) {
            long done = progress.uploaded.get();
            long recent = recentPerSecond(done, progress.sample[1], now - progress.sample[0]);
            progress.sample[0] = now;
            progress.sample[1] = done;
            System.out.println(progressLine(LocalDateTime.now(), elapsed, done, progress.skipped.get(), recent, progress.retries.get()));
        } else {
            System.out.println(publishLine(LocalDateTime.now(), progress.phase, progress.partsDone.get(), progress.partsTotal,
                    progress.published.get(), progress.retries.get()));
        }
    }

    /** 最近一个进度窗口内的速度。窗口按进度间隔约 60 秒。 */
    static long recentPerSecond(long completed, long previousCompleted, long windowMillis) {
        long seconds = Math.max(1, windowMillis / 1000);
        return Math.max(0, completed - previousCompleted) / seconds;
    }

    static String progressLine(LocalDateTime now, long elapsedSeconds, long uploaded, long skipped, long recentPerSecond, long retries) {
        long seconds = Math.max(1, elapsedSeconds);
        return now.format(CLOCK) + " INFO  CengyanClient - [上传] 已上传 " + uploaded + " 条 | 总跳过 " + skipped
                + " 条 | 平均上传速度 " + (uploaded / seconds) + " 条/s | 最新上传速度 " + recentPerSecond
                + " 条/s | 失败重试次数 " + retries + " 次";
    }

    static String publishLine(LocalDateTime now, String phase, int partsDone, int partsTotal, long published, long retries) {
        return now.format(CLOCK) + " INFO  CengyanClient - [" + phase + "] 已发布分片 " + partsDone + "/" + partsTotal
                + " | 已发布 " + published + " 条 | 失败重试次数 " + retries + " 次";
    }

    private static void printReport(LocalDateTime started, LocalDateTime ended, long elapsedMs, Progress progress, long uploadMs, long publishMs,
                                     String session, String failure) {
        System.out.println(ended.format(CLOCK) + " INFO  CengyanClient -");
        for (String line : reportLines(started, ended, elapsedMs, progress.published.get(), progress.skipped.get(), progress.replaced.get(),
                uploadMs, publishMs, session, failure)) System.out.println(line);
    }

    static List<String> reportLines(LocalDateTime started, LocalDateTime ended, long elapsedMs, long completed, long skipped, long replaced,
                                    long uploadMs, long publishMs, String session, String failure) {
        long seconds = Math.max(1, elapsedMs / 1000);
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[] {"起始时间", started.format(CLOCK)});
        rows.add(new String[] {"结束时间", ended.format(CLOCK)});
        rows.add(new String[] {"总耗时", seconds + " s"});
        rows.add(new String[] {"完成条数", completed + " 条"});
        rows.add(new String[] {"跳过条数", skipped + " 条"});
        rows.add(new String[] {"覆盖条数", replaced + " 条"});
        rows.add(new String[] {"平均速度", (completed / seconds) + " 条/s"});
        rows.add(new String[] {"上传耗时", uploadMs / 1000 + " s"});
        rows.add(new String[] {"归并发布耗时", publishMs / 1000 + " s"});
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
        final long bufferBytes;
        final int retries;
        final int retryIntervalSeconds;
        final boolean node;
        final String typeName;
        final String idColumn;
        final String sourceColumn;
        final String targetColumn;
        final List<String> properties;
        /** 以下由 importPlan 的结果填入。 */
        long graphId;
        long generation;
        int typeId;
        String session = "";

        private Options(List<InetSocketAddress> nodes, String username, String password, String graph, String fileType, String files,
                        char delimiter, String conflict, int threads, long batchBytes, long bufferBytes, int retries, int retryIntervalSeconds,
                        boolean node, String typeName, String idColumn, String sourceColumn, String targetColumn, List<String> properties) {
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
            this.bufferBytes = bufferBytes;
            this.retries = retries;
            this.retryIntervalSeconds = retryIntervalSeconds;
            this.node = node;
            this.typeName = typeName;
            this.idColumn = idColumn;
            this.sourceColumn = sourceColumn;
            this.targetColumn = targetColumn;
            this.properties = properties;
        }

        void bind(JsonNode plan) {
            graphId = plan.path("graphId").asLong();
            generation = plan.path("generation").asLong();
            typeId = plan.path("typeId").asInt();
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
            int threads = Integer.parseInt(values.getOrDefault("--thread", "8"));
            if (threads < 1) throw new IllegalArgumentException("--thread 必须是正整数");
            int batchMb = Integer.parseInt(values.getOrDefault("--batch-mb", "64"));
            if (batchMb < 1) throw new IllegalArgumentException("--batch-mb 必须是正整数");
            int bufferMb = Integer.parseInt(values.getOrDefault("--buffer-mb", "1024"));
            if (bufferMb < batchMb) throw new IllegalArgumentException("--buffer-mb 不能小于 --batch-mb");
            int retries = Integer.parseInt(values.getOrDefault("--retry", "5"));
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
                    threads, batchMb * 1024L * 1024L, bufferMb * 1024L * 1024L, retries, retryInterval, node, type, required(values, "--id"),
                    values.get("--src-id"), values.get("--dst-id"), properties);
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
        /** 请求发出后没有读完响应：连接上可能还有一条迟到的响应，不能再用。 */
        private volatile boolean poisoned;

        private LineClient(Socket socket) throws Exception {
            this.socket = socket;
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        }

        static LineClient connect(List<InetSocketAddress> nodes) throws Exception {
            Exception last = null;
            // 从随机位置开始，多个工作连接分摊到不同的计算节点。
            int start = java.util.concurrent.ThreadLocalRandom.current().nextInt(nodes.size());
            for (int i = 0; i < nodes.size(); i++) {
                InetSocketAddress node = nodes.get((start + i) % nodes.size());
                try {
                    Socket socket = new Socket();
                    socket.connect(node, 3000);
                    socket.setTcpNoDelay(true);
                    socket.setKeepAlive(true);
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

        boolean open() { return !poisoned && !socket.isClosed(); }

        /** 等待时间比服务端超时多 30 秒；还没有响应就放弃这条连接，由调用方重连。 */
        synchronized JsonNode call(ObjectNode request, int timeoutMs) throws Exception {
            if (!open()) throw new IllegalStateException("计算节点连接已断开");
            if (!token.isBlank()) request.put("token", token);
            request.put("requestId", UUID.randomUUID().toString());
            request.put("timeoutMs", timeoutMs);
            String line;
            try {
                socket.setSoTimeout(timeoutMs + 30_000);
                writer.write(JSON.writeValueAsString(request));
                writer.write('\n');
                writer.flush();
                line = reader.readLine();
            } catch (SocketTimeoutException ex) {
                poisoned = true;
                throw new IllegalStateException("等待计算节点响应超时（" + (timeoutMs + 30_000) / 1000 + " 秒）", ex);
            } catch (java.io.IOException ex) {
                poisoned = true;
                throw new IllegalStateException("计算节点连接中断：" + ex.getMessage(), ex);
            }
            if (line == null) {
                poisoned = true;
                throw new IllegalStateException("计算节点关闭了连接");
            }
            JsonNode response = JSON.readTree(line);
            if (!"OK".equals(response.path("code").asText())) {
                throw new IllegalStateException(response.path("code").asText() + " " + response.path("message").asText());
            }
            return response;
        }

        @Override public void close() throws Exception { socket.close(); }
    }
}
