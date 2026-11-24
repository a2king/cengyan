package io.cengyan.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 新副本接收快照时，Ratis 把每次尝试写到 Group 的 {@code tmp/snapshot-<请求号>/}，装完才改名。
 * 一次尝试失败后，Leader 换新请求号从头重发，旧目录 Ratis 不会删（只登记了对非空目录无效的 deleteOnExit）。
 * 反复失败时这些残留能把数据盘写满。这里负责删掉它们。
 */
final class SnapshotTemps {
    static final String PREFIX = "snapshot-";

    private SnapshotTemps() { }

    record Purged(int directories, long bytes) {
        static final Purged NONE = new Purged(0, 0);
    }

    /**
     * 每个 Group 同一时刻只会有一次快照安装在写，所以只保留最近有写入的那一个目录，而且它空闲不超过 {@code idleMillis}；
     * 其余全部删除。{@code idleMillis} 为 0 时全部删除（进程刚启动、还没有安装在进行时用）。
     */
    static Purged purge(Path tmpDirectory, long idleMillis, long nowMillis) throws IOException {
        if (tmpDirectory == null || !Files.isDirectory(tmpDirectory, LinkOption.NOFOLLOW_LINKS)) return Purged.NONE;
        List<Path> attempts = new ArrayList<>();
        try (var children = Files.list(tmpDirectory)) {
            for (Path child : children.toList()) {
                if (child.getFileName().toString().startsWith(PREFIX) && Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) attempts.add(child);
            }
        }
        if (attempts.isEmpty()) return Purged.NONE;
        List<long[]> activity = new ArrayList<>();
        for (int i = 0; i < attempts.size(); i++) activity.add(new long[] {i, lastWrite(attempts.get(i))});
        activity.sort(Comparator.comparingLong((long[] item) -> item[1]).reversed());
        int deleted = 0;
        long bytes = 0;
        for (int rank = 0; rank < activity.size(); rank++) {
            Path attempt = attempts.get((int) activity.get(rank)[0]);
            boolean current = rank == 0 && idleMillis > 0 && nowMillis - activity.get(rank)[1] < idleMillis;
            if (current) continue;
            bytes += delete(attempt);
            deleted++;
        }
        return new Purged(deleted, bytes);
    }

    /** 最近有写入的那次尝试已收到的字节数；没有接收目录时为 -1。 */
    static long receiving(Path tmpDirectory) throws IOException {
        if (tmpDirectory == null || !Files.isDirectory(tmpDirectory, LinkOption.NOFOLLOW_LINKS)) return -1;
        Path newest = null;
        long newestWrite = Long.MIN_VALUE;
        try (var children = Files.list(tmpDirectory)) {
            for (Path child : children.toList()) {
                if (!child.getFileName().toString().startsWith(PREFIX) || !Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) continue;
                long write = lastWrite(child);
                if (write > newestWrite) { newest = child; newestWrite = write; }
            }
        }
        if (newest == null) return -1;
        long bytes = 0;
        try (var files = Files.walk(newest)) {
            for (Path file : files.toList()) {
                try { if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) bytes += Files.size(file); }
                catch (NoSuchFileException ignored) { }
            }
        } catch (UncheckedIOException ex) {
            if (!(ex.getCause() instanceof NoSuchFileException)) throw ex.getCause();
        }
        return bytes;
    }

    /** 目录内最近一次写入的时间。Ratis 按分块追加写，正在接收的目录里总有文件刚被改过。 */
    private static long lastWrite(Path directory) throws IOException {
        long latest = Files.getLastModifiedTime(directory, LinkOption.NOFOLLOW_LINKS).toMillis();
        try (var files = Files.walk(directory)) {
            for (Path file : files.toList()) {
                try { latest = Math.max(latest, Files.getLastModifiedTime(file, LinkOption.NOFOLLOW_LINKS).toMillis()); }
                catch (NoSuchFileException ignored) { }
            }
        } catch (UncheckedIOException ex) {
            if (!(ex.getCause() instanceof NoSuchFileException)) throw ex.getCause();
        }
        return latest;
    }

    /** 删除整棵目录，返回释放的字节数。不跟随符号链接；已被别人删掉的文件跳过。 */
    private static long delete(Path directory) throws IOException {
        long bytes = 0;
        List<Path> paths;
        try (var files = Files.walk(directory)) { paths = files.sorted(Comparator.reverseOrder()).toList(); }
        catch (UncheckedIOException ex) {
            if (ex.getCause() instanceof NoSuchFileException) return 0;
            throw ex.getCause();
        }
        for (Path path : paths) {
            try {
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) bytes += Files.size(path);
                Files.deleteIfExists(path);
            } catch (NoSuchFileException ignored) { }
        }
        return bytes;
    }
}
