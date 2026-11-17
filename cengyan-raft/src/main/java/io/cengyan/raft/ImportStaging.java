package io.cengyan.raft;

import io.cengyan.protocol.EntityKeys;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.ImportChunk;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPrepared;
import io.cengyan.protocol.proto.ClusterProtocol.ImportPublish;
import io.cengyan.protocol.proto.ClusterProtocol.VersionedCell;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 一个分片副本上的导入暂存区。上传段只落在这里，不进分片库，也不进 Raft 日志。
 *
 * <pre>
 * import-staging/sessions/&lt;会话&gt;            心跳时间
 * import-staging/&lt;会话&gt;/runs/&lt;sha256&gt;.partial  正在上传的段
 * import-staging/&lt;会话&gt;/runs/&lt;sha256&gt;.sst      上传完成、校验通过的段
 * import-staging/&lt;会话&gt;/runs/m-NNNNNN.sst/.runs  后台把多个段合成一个，.runs 记下包含哪些段
 * import-staging/&lt;会话&gt;/prepared-&lt;n|e&gt;&lt;类型&gt;/  归并好、待发布的文件
 * </pre>
 *
 * 上传段攒多了就在后台合成大段，发布前的最终归并只需要打开几十个文件。
 */
final class ImportStaging {
    static final long IDLE_MS = 30L * 60L * 1000L;
    /**
     * 归并好、还没在本副本发布的文件留这么久。发布指令在 Leader 上生效后客户端就会结束导入，
     * 落后的副本稍后才应用同一条指令，那时还要用这些文件。
     */
    static final long PREPARED_MS = 24L * 60L * 60L * 1000L;
    /** 未合并的上传段攒到这么多就在后台合成一个。 */
    static final int COMPACT_RUNS = 32;
    /** 发布文件的目标大小。 */
    static final long PUBLISH_FILE_BYTES = 128L * 1024 * 1024;
    private static final long PROCESS_START = System.currentTimeMillis();
    private static final Pattern SESSION = Pattern.compile("[A-Za-z0-9_-]{1,120}");
    private static final Pattern RUN = Pattern.compile("[0-9a-f]{64}\\.sst");
    private static final Logger LOG = Logger.getLogger(ImportStaging.class.getName());
    private static final int MAGIC = 0x43594950;
    private static final String DUPLICATES = "duplicates=";
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    /** 同时进行的最终归并数。归并是 CPU 和顺序写，留一半核给查询和 Raft。 */
    private static final Semaphore PREPARES = new Semaphore(Math.max(2, CORES / 2));
    private static final ThreadPoolExecutor BACKGROUND = new ThreadPoolExecutor(Math.max(1, CORES / 4), Math.max(1, CORES / 4),
            0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), task -> {
                Thread thread = new Thread(task, "storage-import-merge");
                thread.setDaemon(true);
                return thread;
            });
    private static final Set<Path> SCHEDULED = ConcurrentHashMap.newKeySet();

    private final Path root;
    private final Map<Path, Locks> locks = new ConcurrentHashMap<>();

    ImportStaging(Path root) { this.root = root; }

    Path root() { return root; }

    /** 一个会话目录的两把锁：分块写入，以及合并/归并/发布。后者可能持续几十秒，不能挡住分块写入。 */
    private static final class Locks {
        final ReentrantLock stage = new ReentrantLock();
        final ReentrantLock merge = new ReentrantLock();
        /** 已经合进 m-*.sst 的上传段。 */
        final Set<String> merged = ConcurrentHashMap.newKeySet();
    }

    record Status(int runs, int mergedRuns, int coveredRuns, int partials, long bytes, long partialBytes, List<String> prepared) { }

    /** 归并结果。code 不是 OK 时没有文件，发布直接返回这个错误。 */
    record Prepared(String code, String message, long rows, long primaries, long duplicates, long bytes, byte[] digest,
                    List<Path> data, List<Path> directory) {
        boolean ok() { return code.equals("OK"); }
        ImportPrepared proto() {
            return ImportPrepared.newBuilder().setCode(code).setMessage(message).setRows(rows).setPrimaries(primaries)
                    .setDuplicates(duplicates).setBytes(bytes).build();
        }
        static Prepared failed(String code, String message, byte[] digest) {
            return new Prepared(code, message, 0, 0, 0, 0, digest, List.of(), List.of());
        }
    }

    // ------------------------------------------------------------------ 上传

    /**
     * 写入一个分块。同一偏移重发会覆盖，前面缺块时拒绝。最后一块写完后校验整段，fsync 一次再改名。
     * @return 这一块让一个上传段完整了
     */
    boolean stage(ImportChunk chunk, long now) throws IOException {
        String session = chunk.getSessionId();
        validate(session);
        if (chunk.getChecksum().size() != 32) throw new IOException("导入校验和必须是 SHA-256");
        byte[] checksum = chunk.getChecksum().toByteArray();
        String hex = ImportSst.hex(checksum);
        Path runs = runs(session);
        Files.createDirectories(runs);
        Path clock = clock(session);
        if (!Files.exists(clock)) {
            Files.createDirectories(clock.getParent());
            Files.write(clock, bytes(now));
        }
        Locks locks = locks(session);
        locks.stage.lock();
        try {
            Path ready = runs.resolve(hex + ".sst");
            if (Files.exists(ready) || locks.merged.contains(hex)) return false;
            Path partial = runs.resolve(hex + ".partial");
            long size = Files.exists(partial) ? Files.size(partial) : 0;
            if (chunk.getOffset() > size) throw new IOException("导入分块偏移不连续");
            try (FileChannel channel = FileChannel.open(partial, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                ByteBuffer data = chunk.getData().asReadOnlyByteBuffer();
                long position = chunk.getOffset();
                long end = position + data.remaining();
                while (data.hasRemaining()) position += channel.write(data, position);
                if (channel.size() > end) channel.truncate(end);
                if (chunk.getLast()) channel.force(true);
            }
            if (!chunk.getLast()) return false;
            if (!Arrays.equals(ImportSst.sha256(partial), checksum)) {
                Files.deleteIfExists(partial);
                throw new IOException("导入文件校验和不匹配");
            }
            Files.move(partial, ready, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } finally { locks.stage.unlock(); }
    }

    /** 上传段够数时排一次后台合并。同一个会话同时只排一次。 */
    void scheduleCompaction(String session) {
        Path directory = sessionDirectory(session);
        if (!SCHEDULED.add(directory)) return;
        BACKGROUND.execute(() -> {
            try {
                while (compactRuns(session)) { /* 还够数就接着合 */ }
            } catch (IOException | RuntimeException ex) {
                LOG.warning("后台合并上传段没有完成，发布前会一并归并：" + ex.getMessage());
            } finally { SCHEDULED.remove(directory); }
        });
    }

    /** @return 合并了一批 */
    boolean compactRuns(String session) throws IOException {
        Locks locks = locks(session);
        if (!locks.merge.tryLock()) return false;
        try {
            Path runs = runs(session);
            if (!Files.isDirectory(runs)) return false;
            // 进程重启后内存里的记录没了，从合并清单里找回哪些上传段已经合过，免得再合一遍。
            for (List<String> covered : manifests(runs).values()) locks.merged.addAll(covered);
            List<Path> raw = new ArrayList<>();
            for (Path run : rawRuns(runs)) {
                String hex = name(run);
                if (locks.merged.contains(hex)) Files.deleteIfExists(run);
                else raw.add(run);
            }
            if (raw.size() < COMPACT_RUNS) return false;
            raw.sort(null);
            List<Path> batch = raw.subList(0, COMPACT_RUNS);
            String merged = String.format("m-%06d", nextMerged(runs));
            Path output = runs.resolve(merged + ".sst");
            ImportMerge.Outcome outcome = ImportMerge.mergeInto(batch, output);
            List<String> hexes = batch.stream().map(ImportStaging::name).toList();
            Path manifest = runs.resolve(merged + ".runs");
            Path temporary = runs.resolve(merged + ".runs.tmp");
            // 第一行记下这次合并去掉的重复行，最终归并的统计要把它加回来。
            Files.write(temporary, (DUPLICATES + outcome.duplicates() + "\n" + String.join("\n", hexes)).getBytes(StandardCharsets.UTF_8));
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            Files.move(temporary, manifest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            locks.merged.addAll(hexes);
            for (Path run : batch) Files.deleteIfExists(run);
            return true;
        } finally { locks.merge.unlock(); }
    }

    // ------------------------------------------------------------------ 归并

    /**
     * 把这个会话的全部上传段归并成可以直接摄入的文件：数据文件按键类切开、互不重叠，
     * 点类型另外产出点目录文件。已经按同一份清单归并过就直接返回上次的结果。
     * 本副本缺少清单里的段时抛出异常：这不是导入内容的问题，不能作为发布结果写进分片库。
     */
    Prepared prepare(ImportPublish publish) throws IOException {
        String session = publish.getSessionId();
        validate(session);
        byte[] digest = digest(publish);
        Path directory = prepared(session, publish);
        Locks locks = locks(session);
        // 先拿会话锁再拿名额，和发布时的加锁顺序一致。
        locks.merge.lock();
        try {
            PREPARES.acquire();
        } catch (InterruptedException ex) {
            locks.merge.unlock();
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("导入归并被取消");
        }
        try {
            Prepared existing = read(directory);
            if (existing != null && Arrays.equals(existing.digest(), digest)) return existing;
            deleteTree(directory);
            Files.createDirectories(directory);
            Prepared result;
            try {
                // 缺段抛普通 IOException，不写结果：那是本副本的问题，换一次归并可能就好了。
                List<Path> inputs = inputs(session, publish, locks);
                result = merge(publish, inputs, directory, digest);
                long earlier = earlierDuplicates(runs(session));
                if (earlier > 0) result = new Prepared(result.code(), result.message(), result.rows(), result.primaries(),
                        result.duplicates() + earlier, result.bytes(), result.digest(), result.data(), result.directory());
            } catch (ImportMerge.ImportException ex) {
                deleteTree(directory);
                Files.createDirectories(directory);
                result = Prepared.failed(ex.code, ex.getMessage(), digest);
            }
            write(directory, result);
            return result;
        } finally {
            PREPARES.release();
            locks.merge.unlock();
        }
    }

    /** 本副本上这个会话的全部上传段必须正好等于发布清单。 */
    private List<Path> inputs(String session, ImportPublish publish, Locks locks) throws IOException {
        if (publish.getRunsCount() == 0) throw new ImportMerge.ImportException("INVALID_ARGUMENT", "发布清单为空");
        Set<String> wanted = new TreeSet<>();
        for (var run : publish.getRunsList()) {
            if (run.size() != 32) throw new IOException("上传段校验和必须是 SHA-256");
            wanted.add(ImportSst.hex(run.toByteArray()));
        }
        Path runs = runs(session);
        if (!Files.isDirectory(runs)) throw new IOException("本副本没有这个导入会话的上传段：" + session);
        Set<String> available = new HashSet<>();
        List<Path> inputs = new ArrayList<>();
        Map<Path, List<String>> manifests = manifests(runs);
        for (var entry : manifests.entrySet()) {
            inputs.add(entry.getKey());
            available.addAll(entry.getValue());
            locks.merged.addAll(entry.getValue());
        }
        for (Path run : rawRuns(runs)) {
            if (available.contains(name(run))) continue;
            available.add(name(run));
            inputs.add(run);
        }
        Set<String> missing = new TreeSet<>(wanted);
        missing.removeAll(available);
        Set<String> extra = new TreeSet<>(available);
        extra.removeAll(wanted);
        if (!missing.isEmpty() || !extra.isEmpty()) {
            throw new IOException("本副本的上传段与发布清单不一致：缺少 " + missing.size() + " 段，多出 " + extra.size() + " 段");
        }
        inputs.sort(null);
        return inputs;
    }

    private Prepared merge(ImportPublish publish, List<Path> inputs, Path directory, byte[] digest) throws IOException {
        List<Path> pending = new ArrayList<>(inputs);
        int round = 0;
        while (pending.size() > ImportMerge.FAN_IN) {
            List<Path> next = new ArrayList<>();
            for (int start = 0; start < pending.size(); start += ImportMerge.FAN_IN) {
                List<Path> group = pending.subList(start, Math.min(pending.size(), start + ImportMerge.FAN_IN));
                if (group.size() == 1) { next.add(group.getFirst()); continue; }
                Path output = directory.resolve(String.format("tmp-%02d-%05d.sst", round, start / ImportMerge.FAN_IN));
                ImportMerge.mergeInto(group, output);
                next.add(output);
            }
            for (Path file : pending) if (file.startsWith(directory)) Files.deleteIfExists(file);
            pending = next;
            round++;
        }
        boolean node = publish.getNode();
        long generation = publish.getGeneration();
        int typeId = publish.getTypeId();
        long[] primaries = {0};
        try (ImportMerge.Output data = new ImportMerge.Output(directory, "data", PUBLISH_FILE_BYTES, true, true);
             ImportMerge.Output entries = new ImportMerge.Output(directory, "dir", PUBLISH_FILE_BYTES, false, true)) {
            ImportMerge.Outcome outcome = ImportMerge.merge(pending, (key, value) -> {
                byte[] logical = Arrays.copyOfRange(key, 1, key.length);
                byte kind = EntityKeys.kind(logical);
                boolean expected = node ? kind == EntityKeys.NODE && EntityKeys.primary(logical)
                        : (kind == EntityKeys.EDGE && EntityKeys.primary(logical)) || EntityKeys.adjacency(logical);
                if (key[0] != 2 || !expected) throw new ImportMerge.ImportException("INVALID_ARGUMENT",
                        node ? "点类型导入里出现了点以外的键" : "边类型导入里出现了边和邻接以外的键");
                if (EntityKeys.generation(logical) != generation) throw new ImportMerge.ImportException("INVALID_ARGUMENT", "导入键代次与图不一致");
                if (EntityKeys.typeId(logical) != typeId) throw new ImportMerge.ImportException("INVALID_ARGUMENT", "导入键类型与发布的类型不一致");
                if (kind == EntityKeys.NODE || kind == EntityKeys.EDGE) primaries[0]++;
                data.put(key, value);
                if (node) {
                    Entity entity = entity(value);
                    if (entity == null || !entity.getNode() || !entity.getType().equals(publish.getTypeName())
                            || !sameId(entity, EntityKeys.primaryId(logical))) {
                        throw new ImportMerge.ImportException("INVALID_ARGUMENT", "点记录与它的键不一致");
                    }
                    Entity entry = Entity.newBuilder().setNode(true).setType(entity.getType()).setId(entity.getId())
                            .setState(entity.getState()).setIncarnation(entity.getIncarnation()).build();
                    entries.put(ImportSst.physical(EntityKeys.directory(generation, EntityKeys.primaryId(logical))),
                            ImportSst.cell(entry.toByteArray()));
                }
            });
            List<Path> dataFiles = data.finish();
            List<Path> directoryFiles = entries.finish();
            for (Path file : pending) if (file.startsWith(directory)) Files.deleteIfExists(file);
            return new Prepared("OK", "", outcome.rows(), primaries[0], outcome.duplicates(), data.bytes() + entries.bytes(),
                    digest, dataFiles, directoryFiles);
        }
    }

    private static boolean sameId(Entity entity, byte[] id) {
        try { return Arrays.equals(ImportSst.canonical(entity.getId()), id); }
        catch (IOException ex) { return false; }
    }

    private static Entity entity(byte[] cell) {
        try {
            VersionedCell parsed = VersionedCell.parseFrom(cell);
            if (parsed.getVersionsCount() != 1 || !parsed.getVersions(0).getBaseline()) return null;
            return Entity.parseFrom(parsed.getVersions(0).getValue());
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) { return null; }
    }

    /** 归并结果只取决于类型、代次和上传段集合，与冲突策略、上传先后无关。 */
    private static byte[] digest(ImportPublish publish) {
        MessageDigest digest = ImportSst.digest();
        digest.update(ByteBuffer.allocate(13).put((byte) (publish.getNode() ? 1 : 0)).putInt(publish.getTypeId()).putLong(publish.getGeneration()).array());
        digest.update(publish.getTypeName().getBytes(StandardCharsets.UTF_8));
        TreeSet<String> runs = new TreeSet<>();
        for (var run : publish.getRunsList()) runs.add(ImportSst.hex(run.toByteArray()));
        for (String run : runs) digest.update(run.getBytes(StandardCharsets.US_ASCII));
        return digest.digest();
    }

    private static void write(Path directory, Prepared prepared) throws IOException {
        Path temporary = directory.resolve("prepared.bin.tmp");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(temporary))) {
            output.writeInt(MAGIC);
            output.write(prepared.digest());
            output.writeUTF(prepared.code());
            output.writeUTF(prepared.message());
            output.writeLong(prepared.rows());
            output.writeLong(prepared.primaries());
            output.writeLong(prepared.duplicates());
            output.writeLong(prepared.bytes());
            for (List<Path> files : List.of(prepared.data(), prepared.directory())) {
                output.writeInt(files.size());
                for (Path file : files) output.writeUTF(file.getFileName().toString());
            }
        }
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
        Files.move(temporary, directory.resolve("prepared.bin"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** @return 没有归并结果、结果不完整或文件已经缺失时为 null */
    private static Prepared read(Path directory) throws IOException {
        Path file = directory.resolve("prepared.bin");
        if (!Files.isRegularFile(file)) return null;
        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            if (input.readInt() != MAGIC) return null;
            byte[] digest = input.readNBytes(32);
            String code = input.readUTF(), message = input.readUTF();
            long rows = input.readLong(), primaries = input.readLong(), duplicates = input.readLong(), bytes = input.readLong();
            List<List<Path>> groups = new ArrayList<>();
            for (int group = 0; group < 2; group++) {
                int count = input.readInt();
                List<Path> files = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    Path path = directory.resolve(input.readUTF());
                    if (!Files.isRegularFile(path)) return null;
                    files.add(path);
                }
                groups.add(List.copyOf(files));
            }
            return new Prepared(code, message, rows, primaries, duplicates, bytes, digest, groups.get(0), groups.get(1));
        } catch (java.io.EOFException ex) { return null; }
    }

    // ------------------------------------------------------------------ 会话

    void heartbeat(String session, long now) throws IOException {
        validate(session);
        Path clock = clock(session);
        Files.createDirectories(clock.getParent());
        Files.write(clock, bytes(now));
    }

    /**
     * 超过 30 分钟没有心跳的会话删掉上传段；归并好、还没发布的文件留到 24 小时。进程刚启动的 30 分钟内不删：
     * 停机期间收不到心跳，不能因此删掉客户端还在等着发布的数据。
     * @return 删掉了全部文件的会话数
     */
    int expire(long now) throws IOException {
        if (now - PROCESS_START < IDLE_MS) return 0;
        Path sessions = root.resolve("sessions");
        if (!Files.isDirectory(sessions)) return 0;
        int deleted = 0;
        try (Stream<Path> clocks = Files.list(sessions)) {
            for (Path clock : clocks.toList()) {
                String session = clock.getFileName().toString();
                if (!SESSION.matcher(session).matches()) continue;
                byte[] stored;
                try { stored = Files.readAllBytes(clock); }
                catch (NoSuchFileException ex) { continue; }
                long idle = stored.length == 8 ? now - ByteBuffer.wrap(stored).getLong() : Long.MAX_VALUE;
                if (idle < IDLE_MS) continue;
                if (close(session, idle >= PREPARED_MS)) deleted++;
            }
        }
        return deleted;
    }

    /**
     * 删掉会话的暂存文件。正在归并的会话等归并结束再删。
     * @param everything 连归并好、还没发布的文件一起删。客户端在发布之前就放弃导入时才这样做；
     *                   否则这些文件留给还没应用发布指令的副本，发布后或 24 小时后再删
     * @return 会话的文件已经全部删掉
     */
    boolean close(String session, boolean everything) throws IOException {
        validate(session);
        Path directory = sessionDirectory(session);
        if (!Files.exists(directory) && !Files.exists(clock(session))) return false;
        Locks locks = locks(session);
        locks.merge.lock();
        locks.stage.lock();
        try {
            boolean pending = false;
            if (Files.isDirectory(directory)) {
                try (Stream<Path> children = Files.list(directory)) {
                    for (Path child : children.toList()) {
                        boolean prepared = child.getFileName().toString().startsWith("prepared-") && Files.isRegularFile(child.resolve("prepared.bin"));
                        if (prepared && !everything) pending = true;
                        else deleteTree(child);
                    }
                }
            }
            if (pending) return false;
            deleteTree(directory);
            Files.deleteIfExists(clock(session));
            return true;
        } finally {
            locks.stage.unlock();
            locks.merge.unlock();
            if (!Files.exists(directory)) this.locks.remove(directory);
        }
    }

    /** 发布已经落进分片库：上传段和归并结果都没用了。会话目录和心跳留到导入结束。 */
    void released(ImportPublish publish) throws IOException {
        Locks locks = locks(publish.getSessionId());
        locks.merge.lock();
        try {
            deleteTree(prepared(publish.getSessionId(), publish));
            deleteTree(runs(publish.getSessionId()));
            locks.merged.clear();
        } finally { locks.merge.unlock(); }
    }

    /** 发布时持有，防止后台合并或另一次归并同时改动这个会话的文件。 */
    ReentrantLock mergeLock(String session) { return locks(session).merge; }

    Status status(String session) throws IOException {
        validate(session);
        Path runs = runs(session);
        int raw = 0, mergedRuns = 0, covered = 0, partials = 0;
        long bytes = 0, partialBytes = 0;
        if (Files.isDirectory(runs)) {
            Map<Path, List<String>> manifests = manifests(runs);
            mergedRuns = manifests.size();
            Set<String> coveredSet = new HashSet<>();
            for (var entry : manifests.entrySet()) { coveredSet.addAll(entry.getValue()); bytes += Files.size(entry.getKey()); }
            covered = coveredSet.size();
            for (Path run : rawRuns(runs)) if (!coveredSet.contains(name(run))) { raw++; bytes += Files.size(run); }
            try (Stream<Path> files = Files.list(runs)) {
                for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".partial")).toList()) {
                    partials++;
                    partialBytes += Files.size(file);
                }
            }
        }
        List<String> prepared = new ArrayList<>();
        Path directory = sessionDirectory(session);
        if (Files.isDirectory(directory)) {
            try (Stream<Path> children = Files.list(directory)) {
                for (Path child : children.toList()) {
                    String name = child.getFileName().toString();
                    if (name.startsWith("prepared-") && Files.isRegularFile(child.resolve("prepared.bin"))) prepared.add(name.substring(9));
                }
            }
        }
        return new Status(raw, mergedRuns, covered, partials, bytes, partialBytes, List.copyOf(prepared));
    }

    // ------------------------------------------------------------------ 路径

    private Locks locks(String session) { return locks.computeIfAbsent(sessionDirectory(session), ignored -> new Locks()); }
    private Path sessionDirectory(String session) { return root.resolve(session); }
    private Path runs(String session) { return sessionDirectory(session).resolve("runs"); }
    private Path clock(String session) { return root.resolve("sessions").resolve(session); }
    private Path prepared(String session, ImportPublish publish) {
        return sessionDirectory(session).resolve("prepared-" + (publish.getNode() ? "n" : "e") + publish.getTypeId());
    }

    private static List<Path> rawRuns(Path runs) throws IOException {
        try (Stream<Path> files = Files.list(runs)) {
            return files.filter(path -> RUN.matcher(path.getFileName().toString()).matches()).toList();
        }
    }

    private static Map<Path, List<String>> manifests(Path runs) throws IOException {
        Map<Path, List<String>> result = new HashMap<>();
        try (Stream<Path> files = Files.list(runs)) {
            for (Path manifest : files.filter(path -> path.getFileName().toString().matches("m-\\d{6}\\.runs")).toList()) {
                String base = manifest.getFileName().toString();
                Path data = runs.resolve(base.substring(0, base.length() - ".runs".length()) + ".sst");
                if (!Files.isRegularFile(data)) throw new IOException("合并后的上传段丢失：" + data.getFileName());
                List<String> hexes = Files.readAllLines(manifest, StandardCharsets.UTF_8).stream()
                        .filter(line -> !line.isBlank() && !line.startsWith(DUPLICATES)).toList();
                result.put(data, hexes);
            }
        }
        return result;
    }

    private static long earlierDuplicates(Path runs) throws IOException {
        long total = 0;
        try (Stream<Path> files = Files.list(runs)) {
            for (Path manifest : files.filter(path -> path.getFileName().toString().matches("m-\\d{6}\\.runs")).toList()) {
                for (String line : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
                    if (line.startsWith(DUPLICATES)) total += Long.parseLong(line.substring(DUPLICATES.length()).trim());
                }
            }
        }
        return total;
    }

    private static int nextMerged(Path runs) throws IOException {
        int next = 1;
        try (Stream<Path> files = Files.list(runs)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (name.matches("m-\\d{6}\\..*")) next = Math.max(next, Integer.parseInt(name.substring(2, 8)) + 1);
            }
        }
        return next;
    }

    private static String name(Path run) {
        String name = run.getFileName().toString();
        return name.substring(0, name.length() - ".sst".length());
    }

    static void validate(String session) throws IOException {
        if (session == null || !SESSION.matcher(session).matches()) throw new IOException("导入会话号无效");
    }

    private static byte[] bytes(long now) { return ByteBuffer.allocate(8).putLong(now).array(); }

    static void deleteTree(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (Stream<Path> files = Files.walk(path)) {
            for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
        }
    }
}
