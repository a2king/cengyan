package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Studio 登录与鉴权（docs/user-permissions.md 第 5 节）。只有一套账号：数据库账号。
 *
 * <ul>
 *   <li>数据库会话：令牌由 Compute 签发，Studio 通过 {@code session} 操作取回用户和角色，缓存 10 秒。</li>
 *   <li>本地会话：托管模式下数据库不可用时，root 用 Studio 保存的数据库 root 密码兜底登录。
 *       令牌以 {@value #LOCAL_PREFIX} 开头，只在内存中，8 小时过期；只能使用集群管理和运维监控。</li>
 * </ul>
 * 不再有单独的 Studio 管理员密码；旧版本的 {@code data/admin.json} 只在初始化 root 密码前用来确认身份。
 */
public final class StudioAuth {
    public record Principal(String user, String role, boolean local) {
        public boolean admin() { return "admin".equals(role); }
        /** 写入任务记录的操作人。 */
        public String operator() { return local ? user + "(local)" : user; }
    }

    /** 用数据库会话令牌调用 Compute 的 {@code session} 操作。 */
    public interface SessionLookup { JsonNode session(String token) throws IOException; }

    public static final String LOCAL_PREFIX = "local.";
    public static final String ROOT = "root";
    private static final long SESSION_MS = 8L * 3600 * 1000;
    private static final long CACHE_MS = 10_000;
    private static final int MAX_FAILURES = 5;
    private static final long LOCK_MS = 5L * 60 * 1000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private record Legacy(String salt, String hash, int iterations) { }
    private record Cached(Principal principal, long expiresAt) { }

    private final ClusterManager manager;
    private final Path legacyFile;
    private final SessionLookup lookup;
    private final Map<String, Long> local = new ConcurrentHashMap<>();
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();
    private int failures;
    private long lockedUntil;

    /** @param manager 托管集群；为 null 表示非托管模式（没有本地兜底） */
    public StudioAuth(ClusterManager manager, Path dataDir, SessionLookup lookup) {
        this.manager = manager;
        this.legacyFile = manager == null || dataDir == null ? null : dataDir.resolve("admin.json");
        this.lookup = lookup;
    }

    public boolean managed() { return manager != null; }

    /** 托管模式下是否已设置数据库 root 密码；非托管模式总是 true。 */
    public boolean initialized() { return manager == null || !manager.state().rootPassword().isEmpty(); }

    /** 旧版本设置过 Studio 管理员密码、但还没设置 root 密码：初始化前要先输入旧管理员密码。 */
    public boolean legacyPending() { return !initialized() && legacyFile != null && Files.exists(legacyFile); }

    /** 首次部署设置数据库 root 密码，并以本地 root 登录。 */
    public synchronized String setup(String password, String legacyPassword) throws IOException {
        if (manager == null) throw new ClusterError("NOT_FOUND", "非托管模式没有初始化");
        if (initialized()) throw new ClusterError("ALREADY_INITIALIZED", "root 密码已设置，请登录");
        if (legacyPending()) {
            checkLock();
            if (!legacyMatches(legacyPassword)) { fail(); throw new ClusterError("UNAUTHORIZED", "旧管理员密码不正确"); }
        }
        manager.setRootPassword(password);
        failures = 0;
        return issue();
    }

    /** 数据库不可用时 root 的本地兜底登录：与 Studio 保存的数据库 root 密码比对。 */
    public synchronized String loginLocal(String user, String password) {
        if (manager == null || !initialized()) throw new ClusterError("UNAVAILABLE", "数据库不可用");
        checkLock();
        if (!ROOT.equals(user) || !constantEquals(password, manager.state().rootPassword())) {
            fail();
            throw new ClusterError("UNAUTHORIZED", "用户名或密码不正确");
        }
        failures = 0;
        return issue();
    }

    /** 当前会话的用户和角色。数据库不可用时抛出 {@code DB_UNAVAILABLE}。 */
    public Principal resolve(String token) {
        if (token == null || token.isBlank()) throw new ClusterError("UNAUTHORIZED", "请先登录");
        long now = System.currentTimeMillis();
        if (token.startsWith(LOCAL_PREFIX)) {
            Long expires = local.get(token);
            if (expires == null || expires < now) { local.remove(token); throw new ClusterError("UNAUTHORIZED", "本地登录已过期，请重新登录"); }
            return new Principal(ROOT, "admin", true);
        }
        Cached cached = cache.get(token);
        if (cached != null && cached.expiresAt() > now) return cached.principal();
        JsonNode reply;
        try { reply = lookup.session(token); }
        catch (IOException ex) { throw new ClusterError("DB_UNAVAILABLE", "数据库不可用（" + ex.getMessage() + "），可用 root 重新登录进入集群管理"); }
        String code = reply.path("code").asText();
        if (code.equals("UNAUTHENTICATED")) { cache.remove(token); throw new ClusterError("UNAUTHORIZED", "登录已失效，请重新登录"); }
        if (!code.equals("OK")) throw new ClusterError("DB_UNAVAILABLE", "数据库不可用（" + reply.path("message").asText(code) + "），可用 root 重新登录进入集群管理");
        Principal principal = new Principal(reply.path("user").asText(), reply.path("role").asText(), false);
        if (cache.size() > 1000) cache.entrySet().removeIf(e -> e.getValue().expiresAt() <= now);
        cache.put(token, new Cached(principal, now + CACHE_MS));
        return principal;
    }

    public Principal requireAdmin(String token) {
        Principal principal = resolve(token);
        if (!principal.admin()) throw new ClusterError("FORBIDDEN", "需要管理员权限");
        return principal;
    }

    public boolean isLocal(String token) { return token != null && token.startsWith(LOCAL_PREFIX); }

    public void logout(String token) {
        if (token == null) return;
        local.remove(token);
        cache.remove(token);
    }

    /**
     * root 刚通过数据库验证了这个密码（数据库登录或改密码成功）：与 Studio 保存的不同时更新，
     * 让集群操作和兜底登录跟数据库保持一致。
     */
    public void rootVerified(String password) throws IOException {
        if (manager == null || !initialized() || password == null || password.isEmpty()) return;
        if (!constantEquals(password, manager.state().rootPassword())) manager.syncRootPassword(password);
    }

    /** 本地会话改 root 密码：只在还没有计算节点（数据库里还没有 root）时允许。 */
    public synchronized void changeLocal(String current, String password) throws IOException {
        if (manager == null) throw new ClusterError("NOT_FOUND", "非托管模式");
        if (manager.hasJoinedCompute()) throw new ClusterError("DB_UNAVAILABLE", "数据库不可用时不能修改 root 密码，请等数据库恢复后用数据库账号登录再改");
        if (!constantEquals(current, manager.state().rootPassword())) throw new ClusterError("UNAUTHORIZED", "当前密码不正确");
        manager.setRootPassword(password);
    }

    private String issue() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        String token = LOCAL_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        local.put(token, System.currentTimeMillis() + SESSION_MS);
        return token;
    }

    private void checkLock() {
        long now = System.currentTimeMillis();
        if (now < lockedUntil) throw new ClusterError("LOCKED", "密码错误次数过多，请 " + ((lockedUntil - now) / 1000 + 1) + " 秒后再试");
    }

    private void fail() {
        if (++failures >= MAX_FAILURES) { lockedUntil = System.currentTimeMillis() + LOCK_MS; failures = 0; }
    }

    private boolean legacyMatches(String password) {
        if (password == null) return false;
        try {
            Legacy stored = ClusterStore.JSON.readValue(legacyFile.toFile(), Legacy.class);
            var spec = new PBEKeySpec(password.toCharArray(), Base64.getDecoder().decode(stored.salt()), stored.iterations(), 256);
            String hash = Base64.getEncoder().encodeToString(SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded());
            return MessageDigest.isEqual(hash.getBytes(StandardCharsets.UTF_8), stored.hash().getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) { return false; }
    }

    private static boolean constantEquals(String a, String b) {
        if (a == null || b == null) return false;
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
