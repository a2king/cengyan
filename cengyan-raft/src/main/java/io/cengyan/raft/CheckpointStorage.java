package io.cengyan.raft;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 本机快照是一份 RocksDB Checkpoint 硬链接，不把整库再打成 zip。
 * 工作库在 {@code db/}，重启时直接打开；硬链接只用来把这一刻的文件发给没有数据的副本。
 */
final class CheckpointStorage implements StateMachineStorage {
    static final String DB_DIRECTORY = "db";
    private static final Pattern SNAPSHOT = Pattern.compile("snap-(\\d+)_(\\d+)");
    private File stateMachineDir;
    private volatile SnapshotInfo latest;
    private volatile Path live;

    @Override public void init(RaftStorage storage) {
        stateMachineDir = storage.getStorageDir().getStateMachineDir();
    }

    @Override public SnapshotInfo getLatestSnapshot() { return latest; }

    @Override public void format() throws IOException {
        if (stateMachineDir == null) return;
        latest = null;
        live = null;
        try (var children = Files.list(stateMachineDir.toPath())) {
            for (Path child : children.toList()) deleteTree(child);
        }
    }

    @Override public void cleanupOldSnapshots(SnapshotRetentionPolicy policy) throws IOException {
        int keep = policy == null ? 1 : Math.max(1, policy.getNumSnapshotsRetained());
        List<Path> snaps = snapshots();
        snaps.sort(Comparator.comparingLong((Path path) -> termIndex(path).getIndex()).reversed());
        for (int i = keep; i < snaps.size(); i++) {
            if (snaps.get(i).equals(live)) continue;
            deleteTree(snaps.get(i));
        }
    }

    void retain(Path directory) { live = directory; }

    /** 重新登记已经存在的硬链接快照，供重启后继续发给落后副本。 */
    void reload() throws IOException {
        latest = null;
        Path newest = null;
        for (Path snap : snapshots()) {
            if (snap.equals(live) || !Files.exists(snap.resolve("CURRENT"))) continue;
            if (newest == null || termIndex(snap).getIndex() > termIndex(newest).getIndex()) newest = snap;
        }
        if (newest != null) latest = describe(newest, termIndex(newest));
    }

    /**
     * 把当前库硬链接成一份快照目录。SST 与工作库共用磁盘块。
     * 调用方随后用 {@link #seal} 计算校验和并发布；这两步分开，避免校验期间挡住写入。
     */
    void link(RocksDB db, long term, long index) throws IOException, RocksDBException {
        if (stateMachineDir == null) throw new IOException("快照目录尚未就绪");
        Path directory = stateMachineDir.toPath().resolve(name(term, index));
        if (latest != null && latest.getIndex() == index && Files.exists(directory.resolve("CURRENT"))) return;
        if (Files.exists(directory.resolve("CURRENT")) && (latest == null || latest.getIndex() != index)) {
            // 上次发布中断：目录还在，直接补校验和。
            return;
        }
        deleteTree(directory);
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(directory.toString());
        }
    }

    SnapshotInfo seal(long term, long index, long bytesPerSecond) throws IOException {
        Path directory = stateMachineDir.toPath().resolve(name(term, index));
        if (!Files.exists(directory.resolve("CURRENT"))) throw new IOException("快照目录不存在：" + directory);
        if (latest != null && latest.getIndex() == index) return latest;
        SnapshotInfo info = describe(directory, TermIndex.valueOf(term, index), bytesPerSecond);
        latest = info;
        return info;
    }

    /** 打开刚安装的快照后，补一份硬链接和校验和，使 Ratis 能把已应用位置识别为这份快照。 */
    SnapshotInfo capture(RocksDB db, long term, long index, long bytesPerSecond) throws IOException, RocksDBException {
        link(db, term, index);
        return seal(term, index, bytesPerSecond);
    }

    static Path dbDirectory(Path stateMachineDir) { return stateMachineDir.resolve(DB_DIRECTORY); }

    /** 落后副本刚装上的快照目录。工作库不存在时由调用方改名为 {@code db}。 */
    static Path installedSnapshot(Path stateMachineDir) throws IOException {
        if (!Files.isDirectory(stateMachineDir)) return null;
        Path newest = null;
        try (var children = Files.list(stateMachineDir)) {
            for (Path child : children.toList()) {
                if (!SNAPSHOT.matcher(child.getFileName().toString()).matches()) continue;
                if (!Files.exists(child.resolve("CURRENT"))) continue;
                if (newest == null || termIndex(child).getIndex() > termIndex(newest).getIndex()) newest = child;
            }
        }
        return newest;
    }

    static TermIndex termIndex(Path directory) {
        Matcher matcher = SNAPSHOT.matcher(directory.getFileName().toString());
        if (!matcher.matches()) throw new IllegalArgumentException("快照目录名称无效：" + directory.getFileName());
        return TermIndex.valueOf(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)));
    }

    static String name(long term, long index) { return "snap-" + term + "_" + index; }

    private List<Path> snapshots() throws IOException {
        List<Path> found = new ArrayList<>();
        if (stateMachineDir == null || !Files.isDirectory(stateMachineDir.toPath())) return found;
        try (var children = Files.list(stateMachineDir.toPath())) {
            for (Path child : children.toList()) {
                if (SNAPSHOT.matcher(child.getFileName().toString()).matches()) found.add(child);
            }
        }
        return found;
    }

    private SnapshotInfo describe(Path directory, TermIndex position) throws IOException {
        return describe(directory, position, Long.MAX_VALUE);
    }

    private SnapshotInfo describe(Path directory, TermIndex position, long bytesPerSecond) throws IOException {
        List<FileInfo> files = new ArrayList<>();
        long started = System.nanoTime();
        long read = 0;
        try (var children = Files.list(directory)) {
            for (Path file : children.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).sorted().toList()) {
                if (file.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX)) continue;
                MD5Hash digest = storedDigest(file);
                if (digest == null) {
                    digest = MD5FileUtil.computeMd5ForFile(file.toFile());
                    MD5FileUtil.saveMD5File(file.toFile(), digest);
                    read += Files.size(file);
                    pace(read, started, bytesPerSecond);
                }
                files.add(new FileInfo(file.toAbsolutePath(), digest));
            }
        }
        if (files.isEmpty()) throw new IOException("快照目录没有文件：" + directory);
        return new FileListSnapshotInfo(files, position);
    }

    private static MD5Hash storedDigest(Path file) {
        File digest = new File(file.toFile().getPath() + MD5FileUtil.MD5_SUFFIX);
        if (!digest.isFile()) return null;
        try { return MD5FileUtil.readStoredMd5ForFile(file.toFile()); }
        catch (IOException ex) { return null; }
    }

    private static void pace(long read, long started, long bytesPerSecond) throws IOException {
        if (bytesPerSecond <= 0 || bytesPerSecond == Long.MAX_VALUE) return;
        long target = (long) ((double) read * 1_000_000_000L / bytesPerSecond);
        long wait = target - (System.nanoTime() - started);
        if (wait > 0) LockSupport.parkNanos(wait);
        if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("快照校验被取消");
    }

    static void deleteTree(Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
        if (Files.isSymbolicLink(path)) throw new IOException("快照路径不能是符号链接：" + path);
        if (Files.isRegularFile(path)) { Files.delete(path); return; }
        try (var files = Files.walk(path)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
        }
    }
}
