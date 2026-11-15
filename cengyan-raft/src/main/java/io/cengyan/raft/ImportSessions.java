package io.cengyan.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** 导入会话的心跳。超过 30 分钟没有心跳，就删除这次会话留在副本上的暂存文件。 */
final class ImportSessions {
    static final long IDLE_MS = 30L * 60L * 1000L;
    private static final Pattern SESSION = Pattern.compile("[A-Za-z0-9_-]{1,120}");
    private static final String[] SUFFIXES = {".partial", ".sst", ".apply.sst", ".inserted", ".pending", ".session", ".link"};

    private ImportSessions() { }

    static void bind(Path staging, String sessionId, String checksumHex, long now) throws IOException {
        validate(sessionId);
        Files.createDirectories(staging.resolve("sessions"));
        Path clock = staging.resolve("sessions").resolve(sessionId);
        if (!Files.exists(clock)) Files.write(clock, bytes(now));
        Path owner = staging.resolve(checksumHex + ".session");
        if (Files.exists(owner)) {
            String existing = Files.readString(owner);
            if (!existing.equals(sessionId)) throw new IOException("导入文件已属于另一个会话");
            return;
        }
        Files.writeString(owner, sessionId);
    }

    static void heartbeat(Path staging, String sessionId, long now) throws IOException {
        validate(sessionId);
        Files.createDirectories(staging.resolve("sessions"));
        Files.write(staging.resolve("sessions").resolve(sessionId), bytes(now));
    }

    /** @return 删除的暂存文件数。已经摄入数据库的键不在这里删除。 */
    static int expire(Path staging, long now) throws IOException {
        Path sessions = staging.resolve("sessions");
        if (!Files.isDirectory(sessions)) return 0;
        int deleted = 0;
        try (Stream<Path> clocks = Files.list(sessions)) {
            for (Path clock : clocks.toList()) {
                String sessionId = clock.getFileName().toString();
                if (!SESSION.matcher(sessionId).matches()) continue;
                long last = read(clock);
                if (now - last < IDLE_MS) continue;
                deleted += deleteSession(staging, sessionId);
                Files.deleteIfExists(clock);
            }
        }
        return deleted;
    }

    /** 导入结束或提交成功后删掉这个会话留下的暂存文件。 */
    static int close(Path staging, String sessionId) throws IOException {
        validate(sessionId);
        int deleted = deleteSession(staging, sessionId);
        Path clock = staging.resolve("sessions").resolve(sessionId);
        if (Files.deleteIfExists(clock)) deleted++;
        return deleted;
    }

    private static int deleteSession(Path staging, String sessionId) throws IOException {
        int deleted = 0;
        try (Stream<Path> owners = Files.list(staging)) {
            for (Path owner : owners.filter(path -> path.getFileName().toString().endsWith(".session")).toList()) {
                if (!sessionId.equals(Files.readString(owner))) continue;
                String hex = owner.getFileName().toString().substring(0, owner.getFileName().toString().length() - ".session".length());
                for (String suffix : SUFFIXES) {
                    if (Files.deleteIfExists(staging.resolve(hex + suffix))) deleted++;
                }
            }
        }
        return deleted;
    }

    private static void validate(String sessionId) throws IOException {
        if (sessionId == null || !SESSION.matcher(sessionId).matches()) throw new IOException("导入会话号无效");
    }

    private static byte[] bytes(long now) { return ByteBuffer.allocate(8).putLong(now).array(); }

    private static long read(Path clock) throws IOException {
        byte[] bytes = Files.readAllBytes(clock);
        if (bytes.length != 8) throw new IOException("导入会话心跳损坏");
        return ByteBuffer.wrap(bytes).getLong();
    }
}
