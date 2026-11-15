package io.cengyan.studio.cluster;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Studio 管理员账号：首次打开时设置密码，PBKDF2 加盐哈希存在 {@code data/admin.json}。
 * 会话令牌只保存在内存中，8 小时过期；Studio 重启后需要重新登录。
 */
public final class AdminAuth {
    static final int MIN_PASSWORD = 8;
    private static final int ITERATIONS = 210_000;
    private static final long SESSION_MS = 8L * 3600 * 1000;
    private static final int MAX_FAILURES = 5;
    private static final long LOCK_MS = 5L * 60 * 1000;
    private static final SecureRandom RANDOM = new SecureRandom();

    public record Stored(String salt, String hash, int iterations) { }

    private final Path file;
    private volatile Stored stored;
    private final Map<String, Long> sessions = new ConcurrentHashMap<>();
    private int failures;
    private long lockedUntil;

    public AdminAuth(Path dataDir) throws IOException {
        file = dataDir.resolve("admin.json");
        stored = Files.exists(file) ? ClusterStore.JSON.readValue(file.toFile(), Stored.class) : null;
    }

    public boolean initialized() { return stored != null; }

    /** 只在尚未设置管理员时可用，设置后立即返回登录令牌。 */
    public synchronized String setup(String password) throws IOException {
        if (stored != null) throw new ClusterError("ALREADY_INITIALIZED", "管理员已设置，请登录");
        save(password);
        return issue();
    }

    public synchronized String login(String password) {
        if (stored == null) throw new ClusterError("NOT_INITIALIZED", "请先设置管理员密码");
        long now = System.currentTimeMillis();
        if (now < lockedUntil) throw new ClusterError("LOCKED", "密码错误次数过多，请 " + ((lockedUntil - now) / 1000 + 1) + " 秒后再试");
        if (!matches(password)) {
            if (++failures >= MAX_FAILURES) { lockedUntil = now + LOCK_MS; failures = 0; }
            throw new ClusterError("UNAUTHORIZED", "管理员密码错误");
        }
        failures = 0;
        return issue();
    }

    public synchronized void change(String current, String next) throws IOException {
        if (stored == null || !matches(current)) throw new ClusterError("UNAUTHORIZED", "当前密码错误");
        save(next);
        sessions.clear();
    }

    public boolean valid(String token) {
        if (token == null || token.isBlank()) return false;
        Long expires = sessions.get(token);
        if (expires == null) return false;
        if (expires < System.currentTimeMillis()) { sessions.remove(token); return false; }
        return true;
    }

    public void logout(String token) { if (token != null) sessions.remove(token); }

    private String issue() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        sessions.put(token, System.currentTimeMillis() + SESSION_MS);
        return token;
    }

    private void save(String password) throws IOException {
        if (password == null || password.length() < MIN_PASSWORD) throw new ClusterError("INVALID_ARGUMENT", "管理员密码至少 " + MIN_PASSWORD + " 位");
        byte[] salt = new byte[16];
        RANDOM.nextBytes(salt);
        Stored next = new Stored(Base64.getEncoder().encodeToString(salt), hash(password, salt, ITERATIONS), ITERATIONS);
        ClusterStore.write(file, next);
        stored = next;
    }

    private boolean matches(String password) {
        if (password == null) return false;
        byte[] salt = Base64.getDecoder().decode(stored.salt());
        return MessageDigest.isEqual(hash(password, salt, stored.iterations()).getBytes(), stored.hash().getBytes());
    }

    private static String hash(String password, byte[] salt, int iterations) {
        try {
            var spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
            return Base64.getEncoder().encodeToString(SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded());
        } catch (Exception ex) { throw new IllegalStateException(ex); }
    }
}
