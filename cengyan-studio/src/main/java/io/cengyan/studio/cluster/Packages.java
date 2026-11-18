package io.cengyan.studio.cluster;

import io.cengyan.studio.cluster.ClusterState.Role;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Studio 保存的节点程序包，每个一个目录 {@code data/packages/<程序包 ID>/}：
 * {@link Bundle#RELEASE_FILES} 三个文件加 {@code package.json}。程序包 ID 的算法和 {@link Bundle#releaseId()} 相同，
 * 也就是服务器上 {@code /opt/cengyan/releases/} 下的目录名。
 *
 * <p>新程序放进导入目录后扫描即可使用，不需要重启 Studio：单个 storage 或 compute jar 与最新程序包里的其余文件组成新程序包；
 * 完整发布包 tar.gz 取其中 {@code nodes/} 下的文件。导入时检查 jar 完整、入口类正确、Linux 本地库齐全。
 */
public final class Packages {
    static final String META = "package.json";
    /** 导入目录里的文件最后修改后要稳定这么久才处理，避免读到没传完的文件。 */
    static final long SETTLE_MILLIS = 10_000;
    private static final Map<Role, String> MAIN = Map.of(Role.STORAGE, "io.cengyan.storage.StorageServer", Role.COMPUTE, "io.cengyan.compute.ComputeServer");
    private static final Map<Role, List<String>> NATIVES = Map.of(
            Role.STORAGE, List.of("librocksdbjni-linux64.so", "librocksdbjni-linux-aarch64.so"),
            Role.COMPUTE, List.of("librocksdbjni-linux64.so", "librocksdbjni-linux-aarch64.so", "libduckdb_java.so_linux_amd64", "libduckdb_java.so_linux_arm64"));

    /**
     * @param files 文件名 → SHA-256
     * @param source 来源说明，例如"Studio 随附"、"导入 cengyan-compute.jar"
     */
    public record Package(String id, String version, long importedAt, String source, Map<String, String> files) {
        public Package { files = Map.copyOf(files); }
        String sha(Role role) { return files.getOrDefault(jar(role), ""); }
    }

    /** 一个导入目录文件的处理结果。release 为导入成功（或已存在）的程序包 ID。 */
    public record ScanResult(String file, boolean ok, String message, String release) { }

    private final Path root;
    private final Path inbox;
    private long scannedAt;
    private List<ScanResult> lastScan = List.of();

    public Packages(Path root, Path inbox) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        this.inbox = inbox.toAbsolutePath().normalize();
        Files.createDirectories(this.root);
        Files.createDirectories(this.inbox);
    }

    public Path inbox() { return inbox; }
    public synchronized long scannedAt() { return scannedAt; }
    public synchronized List<ScanResult> lastScan() { return lastScan; }

    static String jar(Role role) { return "cengyan-" + role.name().toLowerCase(Locale.ROOT) + ".jar"; }

    /** 最新导入的在前。 */
    public synchronized List<Package> list() throws IOException {
        List<Package> result = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(root)) {
            for (Path dir : dirs.filter(d -> Files.isRegularFile(d.resolve(META))).toList()) {
                try { result.add(ClusterStore.JSON.readValue(dir.resolve(META).toFile(), Package.class)); }
                catch (IOException ex) { /* 写了一半的目录不算程序包 */ }
            }
        }
        result.sort(Comparator.comparingLong(Package::importedAt).reversed().thenComparing(Package::id, Comparator.reverseOrder()));
        return result;
    }

    public Optional<Package> get(String id) throws IOException {
        if (id == null || id.isEmpty() || !safeId(id)) return Optional.empty();
        Path meta = root.resolve(id).resolve(META);
        synchronized (this) {
            if (!Files.isRegularFile(meta)) return Optional.empty();
            return Optional.of(ClusterStore.JSON.readValue(meta.toFile(), Package.class));
        }
    }

    public Package require(String id) throws IOException {
        return get(id).orElseThrow(() -> new ClusterError("NOT_FOUND", "程序包不存在：" + id));
    }

    public Package latest() throws IOException {
        List<Package> all = list();
        if (all.isEmpty()) throw new ClusterError("BUNDLE_MISSING", "Studio 还没有任何节点程序包。请把发布包或节点 jar 放进 " + inbox + " 后扫描");
        return all.getFirst();
    }

    public Path file(String id, String name) {
        if (!safeId(id) || !Bundle.RELEASE_FILES.contains(name)) throw new ClusterError("INVALID_ARGUMENT", "无效的程序包文件：" + id + "/" + name);
        return root.resolve(id).resolve(name);
    }

    /**
     * 从 from 换到 to 时，这个角色的节点是否要重启：本角色的 jar 或日志配置变了才算。
     * 任一程序包 Studio 不认识时只能按 ID 比较。
     */
    public boolean differs(String from, String to, Role role) throws IOException {
        if (Objects.equals(from, to)) return false;
        Optional<Package> a = get(from), b = get(to);
        if (a.isEmpty() || b.isEmpty()) return true;
        return !a.get().sha(role).equals(b.get().sha(role))
                || !a.get().files().getOrDefault("logback.xml", "").equals(b.get().files().getOrDefault("logback.xml", ""));
    }

    /** 登记 Studio 随附的 nodes/ 目录。已经登记过时直接返回。 */
    public synchronized Package importBundle(Bundle bundle) throws IOException {
        Optional<Package> existing = get(bundle.releaseId());
        if (existing.isPresent()) return existing.get();
        Map<String, Path> sources = new LinkedHashMap<>();
        for (String name : Bundle.RELEASE_FILES) sources.put(name, bundle.releaseFile(name));
        return store(bundle.version(), "Studio 随附", sources);
    }

    /** 处理导入目录里的所有文件。成功的文件删除；失败的改名为 .failed 并在结果里说明原因。 */
    public synchronized List<ScanResult> scan() throws IOException {
        List<ScanResult> results = new ArrayList<>();
        List<Path> files;
        try (Stream<Path> list = Files.list(inbox)) {
            files = list.filter(Files::isRegularFile).sorted().toList();
        }
        for (Path file : files) {
            String name = file.getFileName().toString();
            if (name.startsWith(".") || name.endsWith(".part") || name.endsWith(".tmp") || name.endsWith(".sha256") || name.endsWith(".failed")) continue;
            if (System.currentTimeMillis() - Files.getLastModifiedTime(file).toMillis() < SETTLE_MILLIS) {
                results.add(new ScanResult(name, false, "文件刚刚还在变化，可能没有传完，稍后再扫描", ""));
                continue;
            }
            try {
                checkSidecar(file);
                Package imported;
                if (name.endsWith(".jar")) imported = importJar(file);
                else if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) imported = importArchive(file);
                else throw new ClusterError("INVALID_ARGUMENT", "只支持 .jar、.tar.gz、.tgz");
                Files.deleteIfExists(file);
                Files.deleteIfExists(file.resolveSibling(name + ".sha256"));
                results.add(new ScanResult(name, true, "已导入（" + imported.source() + "）", imported.id()));
            } catch (ClusterError | IOException ex) {
                String reason = TaskRunner.message(ex);
                results.add(new ScanResult(name, false, reason, ""));
                try { Files.move(file, file.resolveSibling(name + ".failed"), StandardCopyOption.REPLACE_EXISTING); }
                catch (IOException ignored) { }
            }
        }
        scannedAt = System.currentTimeMillis();
        lastScan = List.copyOf(results);
        return lastScan;
    }

    /** 删除程序包目录。调用方负责确认没有节点在用。 */
    public synchronized void remove(String id) throws IOException {
        require(id);
        deleteTree(root.resolve(id));
    }

    // ---- import ----
    private Package importJar(Path file) throws IOException {
        Role role = verifyJar(file);
        Package base = latest();
        Map<String, Path> sources = new LinkedHashMap<>();
        for (String name : Bundle.RELEASE_FILES) sources.put(name, file(base.id(), name));
        sources.put(jar(role), file);
        return store(base.version(), "导入 " + file.getFileName() + "，其余文件沿用 " + base.id(), sources);
    }

    private Package importArchive(Path file) throws IOException {
        Path work = Files.createTempDirectory(root, ".archive-");
        try {
            String version = "";
            Map<String, Path> found = new LinkedHashMap<>();
            try (InputStream input = new GZIPInputStream(Files.newInputStream(file), 1 << 16)) {
                TarReader tar = new TarReader(input);
                for (TarReader.Entry entry; (entry = tar.next()) != null; ) {
                    String name = entry.name().replaceFirst("^\\./", "");
                    String[] parts = name.split("/");
                    if (parts.length == 3 && parts[1].equals("nodes") && Bundle.RELEASE_FILES.contains(parts[2])) {
                        Path target = work.resolve(parts[2]);
                        try (OutputStream out = Files.newOutputStream(target)) { tar.copy(out); }
                        found.put(parts[2], target);
                    } else if (parts.length == 2 && parts[1].equals("BUILD-INFO.json")) {
                        ByteArrayStream text = new ByteArrayStream();
                        tar.copy(text);
                        version = ClusterStore.JSON.readTree(text.bytes()).path("version").asText("");
                    }
                }
            } catch (java.util.zip.ZipException | java.io.EOFException ex) {
                throw new ClusterError("INVALID_ARGUMENT", "压缩包损坏或不是 tar.gz：" + ex.getMessage());
            }
            if (!found.containsKey(jar(Role.STORAGE)) && !found.containsKey(jar(Role.COMPUTE)))
                throw new ClusterError("INVALID_ARGUMENT", "压缩包里没有 nodes/cengyan-storage.jar 或 nodes/cengyan-compute.jar，不是层岩发布包");
            for (Role role : Role.values()) {
                Path jar = found.get(jar(role));
                if (jar != null && verifyJar(jar) != role) throw new ClusterError("INVALID_ARGUMENT", "nodes/" + jar(role) + " 的入口类不对");
            }
            Map<String, Path> sources = new LinkedHashMap<>();
            Package base = found.size() == Bundle.RELEASE_FILES.size() ? null : latest();
            for (String name : Bundle.RELEASE_FILES) sources.put(name, found.containsKey(name) ? found.get(name) : file(base.id(), name));
            if (version.isEmpty()) version = base == null ? "dev" : base.version();
            return store(version, "导入 " + file.getFileName(), sources);
        } finally {
            deleteTree(work);
        }
    }

    /** 先在临时目录里放好文件和 package.json，再整体改名，扫描时不会看到半个程序包。 */
    private Package store(String version, String source, Map<String, Path> sources) throws IOException {
        Map<String, String> shas = new LinkedHashMap<>();
        for (String name : Bundle.RELEASE_FILES) shas.put(name, Bundle.sha256(sources.get(name)));
        String id = releaseId(version, shas);
        Optional<Package> existing = get(id);
        if (existing.isPresent()) return existing.get();
        Path staging = Files.createTempDirectory(root, ".import-");
        try {
            for (String name : Bundle.RELEASE_FILES) {
                Path target = staging.resolve(name);
                try { Files.createLink(target, sources.get(name)); }
                catch (IOException | UnsupportedOperationException ex) { Files.copy(sources.get(name), target); }
            }
            Package result = new Package(id, version, System.currentTimeMillis(), source, shas);
            ClusterStore.write(staging.resolve(META), result);
            Files.move(staging, root.resolve(id), StandardCopyOption.ATOMIC_MOVE);
            return result;
        } catch (IOException | RuntimeException ex) {
            deleteTree(staging);
            throw ex;
        }
    }

    /** 版本号加三个文件摘要的摘要前 10 位。和 {@link Bundle#releaseId()} 必须一致。 */
    static String releaseId(String version, Map<String, String> shas) {
        MessageDigest digest = Bundle.sha();
        for (String name : Bundle.RELEASE_FILES) digest.update(HexFormat.of().parseHex(shas.get(name)));
        return version + "-" + HexFormat.of().formatHex(digest.digest()).substring(0, 10);
    }

    /** 检查 jar 完整（逐项校验 CRC）、入口类是存储或计算节点、Linux 本地库齐全，返回角色。 */
    static Role verifyJar(Path jar) throws IOException {
        Manifest manifest = null;
        Set<String> names = new HashSet<>();
        try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(jar), StandardCharsets.UTF_8)) {
            byte[] buffer = new byte[1 << 16];
            for (ZipEntry entry; (entry = zip.getNextEntry()) != null; ) {
                names.add(entry.getName());
                if (entry.getName().equals("META-INF/MANIFEST.MF")) manifest = new Manifest(zip);
                else while (zip.read(buffer) >= 0) { /* 读完整个条目才会校验 CRC */ }
            }
        } catch (java.util.zip.ZipException | java.io.EOFException ex) {
            throw new ClusterError("INVALID_ARGUMENT", jar.getFileName() + " 不是完整的 jar：" + ex.getMessage());
        }
        if (names.isEmpty()) throw new ClusterError("INVALID_ARGUMENT", jar.getFileName() + " 不是 jar");
        String main = manifest == null ? "" : String.valueOf(manifest.getMainAttributes().getValue("Main-Class"));
        Role role = MAIN.entrySet().stream().filter(e -> e.getValue().equals(main)).map(Map.Entry::getKey).findFirst()
                .orElseThrow(() -> new ClusterError("INVALID_ARGUMENT", jar.getFileName() + " 不是存储或计算节点程序（Main-Class: " + main + "）"));
        for (String library : NATIVES.get(role))
            if (!names.contains(library)) throw new ClusterError("INVALID_ARGUMENT", jar.getFileName() + " 缺少 Linux 本地库 " + library);
        return role;
    }

    /** 同目录下有 <文件名>.sha256 时先核对摘要。 */
    private static void checkSidecar(Path file) throws IOException {
        Path sidecar = file.resolveSibling(file.getFileName() + ".sha256");
        if (!Files.isRegularFile(sidecar)) return;
        String expected = Files.readString(sidecar).strip().split("\\s+")[0].toLowerCase(Locale.ROOT);
        if (!expected.equals(Bundle.sha256(file))) throw new ClusterError("INVALID_ARGUMENT", "SHA-256 与 " + sidecar.getFileName() + " 不一致，文件可能没传完整");
    }

    private static boolean safeId(String id) { return id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"); }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }

    private static final class ByteArrayStream extends java.io.ByteArrayOutputStream {
        byte[] bytes() { return toByteArray(); }
    }

    /** 只读普通文件的最小 tar 读取器：支持 ustar 前缀、PAX path 和 GNU 长文件名。 */
    static final class TarReader {
        record Entry(String name, long size, char type) { }
        private final InputStream input;
        private long remaining;
        private long padding;

        TarReader(InputStream input) { this.input = input; }

        Entry next() throws IOException {
            skip(remaining + padding);
            remaining = 0;
            padding = 0;
            String longName = null;
            while (true) {
                byte[] header = input.readNBytes(512);
                if (header.length < 512 || allZero(header)) return null;
                String name = text(header, 0, 100);
                if (text(header, 257, 5).equals("ustar")) {
                    String prefix = text(header, 345, 155);
                    if (!prefix.isEmpty()) name = prefix + "/" + name;
                }
                long size = number(header, 124, 12);
                char type = (char) header[156];
                long pad = (512 - size % 512) % 512;
                if (type == 'x' || type == 'L') {
                    byte[] data = input.readNBytes(Math.toIntExact(size));
                    skip(pad);
                    if (type == 'L') longName = new String(data, StandardCharsets.UTF_8).replace("\0", "");
                    else {
                        String path = pax(data, "path");
                        if (path != null) longName = path;
                    }
                    continue;
                }
                if (longName != null) name = longName;
                remaining = size;
                padding = pad;
                if (type == '0' || type == '\0') return new Entry(name, size, '0');
                skip(remaining + padding);
                remaining = 0;
                padding = 0;
                longName = null;
            }
        }

        /** 把当前条目的内容写到 out。 */
        void copy(OutputStream out) throws IOException {
            byte[] buffer = new byte[1 << 16];
            while (remaining > 0) {
                int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) throw new java.io.EOFException("tar 条目被截断");
                out.write(buffer, 0, read);
                remaining -= read;
            }
        }

        private void skip(long count) throws IOException {
            while (count > 0) {
                long skipped = input.skip(count);
                if (skipped <= 0) {
                    if (input.read() < 0) throw new java.io.EOFException("tar 被截断");
                    skipped = 1;
                }
                count -= skipped;
            }
        }

        private static String pax(byte[] data, String key) {
            String text = new String(data, StandardCharsets.UTF_8);
            for (String record : text.split("\n")) {
                int space = record.indexOf(' '), eq = record.indexOf('=');
                if (space > 0 && eq > space && record.substring(space + 1, eq).equals(key)) return record.substring(eq + 1);
            }
            return null;
        }

        private static boolean allZero(byte[] block) {
            for (byte b : block) if (b != 0) return false;
            return true;
        }

        private static String text(byte[] header, int offset, int length) {
            int end = offset;
            while (end < offset + length && header[end] != 0) end++;
            return new String(header, offset, end - offset, StandardCharsets.UTF_8);
        }

        /** 八进制数字；最高位为 1 时是 GNU 的 base-256 大文件写法。 */
        private static long number(byte[] header, int offset, int length) {
            if ((header[offset] & 0x80) != 0) {
                long value = 0;
                for (int i = offset + 1; i < offset + length; i++) value = (value << 8) | (header[i] & 0xff);
                return value;
            }
            String text = text(header, offset, length).trim();
            return text.isEmpty() ? 0 : Long.parseLong(text, 8);
        }
    }
}
