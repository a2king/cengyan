package io.cengyan.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.cengyan.protocol.proto.ClusterProtocol.*;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 用户凭据及签名密钥存 Metadata，令牌可在任意 Compute 验证，无客户端服务端会话。
 *
 * <p>角色线性三级 {@code read < write < admin}，见 docs/user-permissions.md。root 是内置管理员：
 * 存储里的 role 为历史值 {@code write}（不迁移，兼容旧版 Compute），{@link #level} 按名字判定为 admin。
 * 这里只保留 root 的硬规则（不能删除、不能改角色），调用者是否有权限由 Compute 判断。
 */
public final class TokenAuthority {
    /** {@code enabled} 只为兼容旧版本 dropUser 留下的停用记录；新记录固定为 true。 */
    public record User(String name, String salt, String digest, String role, long version, boolean enabled) { }
    public record Claims(String user, String role, long version, long expiresAt, String cluster) { }
    private record Keys(String publicKey, String privateKey) { }
    public static final String ROOT = "root";
    public static final int READ = 1, WRITE = 2, ADMIN = 3;
    public static final Set<String> ROLES = Set.of("read", "write", "admin");
    public static final int MIN_PASSWORD = 8, MAX_PASSWORD = 128;
    private static final ObjectMapper JSON = new ObjectMapper();
    private final StorageGateway storage;
    private volatile Keys keys;
    public TokenAuthority(StorageGateway storage) { this.storage = storage; }

    /** 用户的权限级别：root 一律为 admin；未知角色为 0（无权限）。 */
    public static int level(String user, String role) {
        if (ROOT.equals(user)) return ADMIN;
        if (role == null) return 0;
        return switch (role) { case "read" -> READ; case "write" -> WRITE; case "admin" -> ADMIN; default -> 0; };
    }
    /** 对外展示的角色：root 显示为 admin。 */
    public static String effectiveRole(String user, String role) {
        return switch (level(user, role)) { case READ -> "read"; case WRITE -> "write"; case ADMIN -> "admin"; default -> role; };
    }
    public static void checkPassword(String password) {
        if (password == null || password.length() < MIN_PASSWORD || password.length() > MAX_PASSWORD) {
            throw new ClusterException("INVALID_ARGUMENT", "密码长度需要 " + MIN_PASSWORD + " 到 " + MAX_PASSWORD + " 位");
        }
    }

    public void initialize(String rootPassword) {
        if (rootPassword == null || rootPassword.isEmpty()) throw new IllegalArgumentException("root 密码不能为空");
        if (storage.get(0, 0, key(ROOT)) == null) {
            // root_password 来自 Compute 配置，历史部署可能短于 8 位，这里不套用新密码规则。
            try { insert("bootstrap-root", ROOT, rootPassword, "write"); }
            catch (ClusterException ex) {
                if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT", "USER_EXISTS").contains(ex.code()) || storage.get(0, 0, key(ROOT)) == null) throw ex;
            }
        }
        keys();
    }
    public String login(String user, String password) {
        User record = user(user);
        byte[] salt = record == null ? new byte[16] : Base64.getDecoder().decode(record.salt());
        byte[] calculated = hash(password == null ? "" : password, salt);
        if (record == null || !record.enabled() || !MessageDigest.isEqual(calculated, Base64.getDecoder().decode(record.digest()))) {
            throw new ClusterException("UNAUTHENTICATED", "用户名或密码不正确");
        }
        Claims claims = new Claims(record.name(), record.role(), record.version(), System.currentTimeMillis() + TimeUnit.HOURS.toMillis(8), storage.clusterId());
        try {
            byte[] bytes = JSON.writeValueAsBytes(claims);
            Signature signature = Signature.getInstance("Ed25519");
            signature.initSign(KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(keys().privateKey()))));
            signature.update(bytes);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes) + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign());
        } catch (Exception ex) { throw new ClusterException("INTERNAL", "无法签发用户令牌", ex); }
    }
    public Claims verify(String token) {
        Claims claims = verifySignature(token, keys().publicKey(), storage.clusterId());
        User record = user(claims.user());
        if (record == null || !record.enabled() || record.version() != claims.version() || !record.role().equals(claims.role())) {
            throw new ClusterException("UNAUTHENTICATED", "令牌认证版本已失效，请重新登录");
        }
        return claims;
    }
    public String publicKey() { return keys().publicKey(); }
    public User publicUser(String name) {
        User record = user(name);
        if (record == null) return null;
        return new User(record.name(), "", "", record.role(), record.version(), record.enabled());
    }
    public static Claims verifySignature(String token, String publicKey, String cluster) {
        try {
            String[] parts = token.split("\\.", -1);
            if (parts.length != 2 || token.length() > 4096) throw new GeneralSecurityException();
            byte[] bytes = Base64.getUrlDecoder().decode(parts[0]);
            Signature signature = Signature.getInstance("Ed25519");
            signature.initVerify(KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(publicKey))));
            signature.update(bytes);
            if (!signature.verify(Base64.getUrlDecoder().decode(parts[1]))) throw new GeneralSecurityException();
            Claims claims = JSON.readValue(bytes, Claims.class);
            if (!cluster.equals(claims.cluster()) || System.currentTimeMillis() >= claims.expiresAt()) throw new GeneralSecurityException();
            return claims;
        } catch (ClusterException ex) { throw ex; }
        catch (Exception ex) { throw new ClusterException("UNAUTHENTICATED", "令牌无效或已过期，请重新登录"); }
    }
    public void create(String requestId, String name, String password, String role) {
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) throw new ClusterException("INVALID_ARGUMENT", "用户名无效：以字母开头，只含字母、数字、下划线，最长 128 位");
        if (!ROLES.contains(role)) throw new ClusterException("INVALID_ARGUMENT", "用户角色无效，只能是 read、write、admin");
        checkPassword(password);
        insert(requestId, name, password, role);
    }
    private void insert(String requestId, String name, String password, String role) {
        ByteString operation = operation("create", name, password, role);
        ByteString operationKey = TransactionEngine.text("auth/requests/" + requestId);
        if (replayed(operationKey, operation)) return;
        if (storage.get(0, 0, key(name)) != null) throw new ClusterException("USER_EXISTS", "用户已存在：" + name);
        byte[] salt = new byte[16]; new SecureRandom().nextBytes(salt);
        // 版本用毫秒时间戳：删除后重建同名用户时，旧令牌里的版本不会与新记录重合。
        User record = new User(name, Base64.getEncoder().encodeToString(salt), Base64.getEncoder().encodeToString(hash(password, salt)), role,
                System.currentTimeMillis(), true);
        try {
            try {
                storage.write(0, 0, StorageGateway.batch("user.create/" + requestId).addConditions(StorageGateway.condition(key(name), null))
                        .addConditions(StorageGateway.condition(operationKey, null))
                        .addMutations(TransactionEngine.put(operationKey, operation))
                        .addMutations(TransactionEngine.put(key(name), ByteString.copyFrom(JSON.writeValueAsBytes(record)))).build());
            } catch (ClusterException ex) {
                if (replayed(operationKey, operation)) return;
                if (storage.get(0, 0, key(name)) != null) throw new ClusterException("USER_EXISTS", "用户已存在：" + name);
                throw ex;
            }
        } catch (java.io.IOException ex) { throw new ClusterException("INTERNAL", "用户记录编码失败", ex); }
    }
    /** 修改密码和/或角色。null 表示不改。停用/启用已去掉，见 docs/user-permissions.md 第 4.2 节。 */
    public void update(String requestId, String name, String password, String role) {
        ByteString operation = operation("update", name, password, role);
        ByteString operationKey = TransactionEngine.text("auth/requests/" + requestId);
        if (replayed(operationKey, operation)) return;
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) throw new ClusterException("USER_NOT_FOUND", "用户不存在");
        ByteString old = storage.get(0, 0, key(name));
        if (old == null) throw new ClusterException("USER_NOT_FOUND", "用户不存在：" + name);
        try {
            User previous = JSON.readValue(old.toByteArray(), User.class);
            if (role != null && !ROLES.contains(role)) throw new ClusterException("INVALID_ARGUMENT", "用户角色无效，只能是 read、write、admin");
            if (ROOT.equals(name) && role != null) throw new ClusterException("FORBIDDEN", "不能修改 root 的角色");
            byte[] salt = new byte[16];
            String saltText = previous.salt(), digest = previous.digest();
            if (password != null) {
                checkPassword(password);
                new SecureRandom().nextBytes(salt);
                saltText = Base64.getEncoder().encodeToString(salt); digest = Base64.getEncoder().encodeToString(hash(password, salt));
            }
            User next = new User(name, saltText, digest, role == null ? previous.role() : role,
                    Math.max(previous.version() + 1, System.currentTimeMillis()), previous.enabled());
            try {
                storage.write(0, 0, StorageGateway.batch("user.update/" + requestId).addConditions(StorageGateway.condition(key(name), old))
                        .addConditions(StorageGateway.condition(operationKey, null))
                        .addMutations(TransactionEngine.put(operationKey, operation))
                        .addMutations(TransactionEngine.put(key(name), ByteString.copyFrom(JSON.writeValueAsBytes(next)))).build());
            } catch (ClusterException ex) { if (!replayed(operationKey, operation)) throw ex; }
        } catch (java.io.IOException ex) { throw new ClusterException("INTERNAL", "用户记录编码失败", ex); }
    }
    private boolean replayed(ByteString key, ByteString operation) {
        ByteString previous = storage.get(0, 0, key);
        if (previous == null) return false;
        if (!MessageDigest.isEqual(previous.toByteArray(), operation.toByteArray())) throw new ClusterException("REQUEST_CONFLICT", "相同用户管理 requestId 对应不同内容");
        return true;
    }
    /** 真删除：删掉用户记录，同名可以重新创建。旧令牌因用户不存在（或重建后版本不同）而失效。 */
    public void delete(String requestId, String name) {
        ByteString operation = operation("delete", name, null, null);
        ByteString operationKey = TransactionEngine.text("auth/requests/" + requestId);
        if (replayed(operationKey, operation)) return;
        if (ROOT.equals(name)) throw new ClusterException("FORBIDDEN", "不能删除 root");
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) throw new ClusterException("USER_NOT_FOUND", "用户不存在");
        ByteString old = storage.get(0, 0, key(name));
        if (old == null) throw new ClusterException("USER_NOT_FOUND", "用户不存在：" + name);
        try {
            storage.write(0, 0, StorageGateway.batch("user.delete/" + requestId).addConditions(StorageGateway.condition(key(name), old))
                    .addConditions(StorageGateway.condition(operationKey, null))
                    .addMutations(TransactionEngine.put(operationKey, operation))
                    .addMutations(Mutation.newBuilder().setKey(key(name)).setDelete(true)).build());
        } catch (ClusterException ex) { if (!replayed(operationKey, operation)) throw ex; }
    }
    /** 全部用户，不含 salt、digest，按用户名排序。 */
    public List<User> list() {
        List<User> result = new ArrayList<>();
        ByteString after = ByteString.EMPTY;
        String cursor = "";
        do {
            ScanPage page = storage.scan(0, 0, TransactionEngine.text("auth/user/"), after, 100, cursor);
            for (var item : page.getEntriesList()) {
                try {
                    User record = JSON.readValue(item.getValue().toByteArray(), User.class);
                    result.add(new User(record.name(), "", "", record.role(), record.version(), record.enabled()));
                } catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "用户记录损坏", ex); }
            }
            if (!page.getMore()) break;
            after = page.getLastKey();
            cursor = page.getCursorId();
        } while (true);
        result.sort(Comparator.comparing(User::name));
        return List.copyOf(result);
    }
    private ByteString operation(String method, String user, String password, String role) {
        try {
            // 使用集群私密密钥计算请求指纹，不能保存明文密码或可快速字典比对的裸 SHA 摘要。
            var mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(Base64.getDecoder().decode(keys().privateKey()), "HmacSHA256"));
            byte[] input = JSON.writeValueAsBytes(Arrays.asList(method, user, password, role));
            return ByteString.copyFrom(mac.doFinal(input));
        } catch (Exception ex) { throw new ClusterException("INTERNAL", "用户管理请求指纹生成失败", ex); }
    }
    private User user(String name) {
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}")) return null;
        ByteString bytes = storage.get(0, 0, key(name));
        if (bytes == null) return null;
        try { return JSON.readValue(bytes.toByteArray(), User.class); }
        catch (Exception ex) { throw new ClusterException("CORRUPT_DATA", "用户记录损坏", ex); }
    }
    private Keys keys() {
        if (keys != null) return keys;
        ByteString key = TransactionEngine.text("auth/signing/v1");
        ByteString bytes = storage.get(0, 0, key);
        try {
            if (bytes == null) {
                KeyPair pair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                Keys generated = new Keys(Base64.getEncoder().encodeToString(pair.getPublic().getEncoded()), Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded()));
                try {
                    storage.write(0, 0, StorageGateway.batch("auth.keys/" + UUID.randomUUID()).addConditions(StorageGateway.condition(key, null))
                            .addMutations(TransactionEngine.put(key, ByteString.copyFrom(JSON.writeValueAsBytes(generated)))).build());
                } catch (ClusterException ex) { if (!ex.code().equals("WRITE_CONFLICT")) throw ex; }
                bytes = storage.get(0, 0, key);
            }
            keys = JSON.readValue(bytes.toByteArray(), Keys.class);
            return keys;
        } catch (Exception ex) { throw new ClusterException("INTERNAL", "签名密钥无法初始化", ex); }
    }
    private static ByteString key(String name) { return TransactionEngine.text("auth/user/" + name); }
    private static byte[] hash(String password, byte[] salt) {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 600_000, 256);
        try { return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded(); }
        catch (GeneralSecurityException ex) { throw new IllegalStateException(ex); }
        finally { spec.clearPassword(); }
    }
}
