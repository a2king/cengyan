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

/** 用户凭据及签名密钥存 Metadata，令牌可在任意 Compute 验证，无客户端服务端会话。 */
public final class TokenAuthority {
    public record User(String name, String salt, String digest, String role, long version, boolean enabled) { }
    public record Claims(String user, String role, long version, long expiresAt, String cluster) { }
    private record Keys(String publicKey, String privateKey) { }
    private static final ObjectMapper JSON = new ObjectMapper();
    private final StorageGateway storage;
    private volatile Keys keys;
    public TokenAuthority(StorageGateway storage) { this.storage = storage; }

    public void initialize(String rootPassword) {
        if (rootPassword == null || rootPassword.isEmpty()) throw new IllegalArgumentException("root 密码不能为空");
        if (storage.get(0, 0, key("root")) == null) {
            try { create("bootstrap-root", "root", rootPassword, "write"); }
            catch (ClusterException ex) {
                if (!Set.of("WRITE_CONFLICT", "REQUEST_CONFLICT").contains(ex.code()) || storage.get(0, 0, key("root")) == null) throw ex;
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
        if (name == null || !name.matches("[\\p{L}][\\p{L}\\p{N}_]{0,127}") || password == null || password.isEmpty()
                || !Set.of("read", "write").contains(role)) throw new ClusterException("INVALID_ARGUMENT", "用户、密码或角色无效");
        ByteString operation = operation("create", name, password, role, null);
        ByteString operationKey = TransactionEngine.text("auth/requests/" + requestId);
        if (replayed(operationKey, operation)) return;
        byte[] salt = new byte[16]; new SecureRandom().nextBytes(salt);
        User record = new User(name, Base64.getEncoder().encodeToString(salt), Base64.getEncoder().encodeToString(hash(password, salt)), role, 1, true);
        try {
            try {
                storage.write(0, 0, StorageGateway.batch("user.create/" + requestId).addConditions(StorageGateway.condition(key(name), null))
                        .addConditions(StorageGateway.condition(operationKey, null))
                        .addMutations(TransactionEngine.put(operationKey, operation))
                        .addMutations(TransactionEngine.put(key(name), ByteString.copyFrom(JSON.writeValueAsBytes(record)))).build());
            } catch (ClusterException ex) { if (!replayed(operationKey, operation)) throw ex; }
        } catch (java.io.IOException ex) { throw new ClusterException("INTERNAL", "用户记录编码失败", ex); }
    }
    public void update(String requestId, String name, String password, String role, Boolean enabled) {
        ByteString operation = operation("update", name, password, role, enabled);
        ByteString operationKey = TransactionEngine.text("auth/requests/" + requestId);
        if (replayed(operationKey, operation)) return;
        ByteString old = storage.get(0, 0, key(name));
        if (old == null) throw new ClusterException("USER_NOT_FOUND", "用户不存在");
        try {
            User previous = JSON.readValue(old.toByteArray(), User.class);
            if (role != null && !Set.of("read", "write").contains(role)) throw new ClusterException("INVALID_ARGUMENT", "用户角色无效");
            byte[] salt = new byte[16];
            String saltText = previous.salt(), digest = previous.digest();
            if (password != null) {
                if (password.isEmpty()) throw new ClusterException("INVALID_ARGUMENT", "密码不能为空");
                new SecureRandom().nextBytes(salt);
                saltText = Base64.getEncoder().encodeToString(salt); digest = Base64.getEncoder().encodeToString(hash(password, salt));
            }
            User next = new User(name, saltText, digest, role == null ? previous.role() : role, previous.version() + 1,
                    enabled == null ? previous.enabled() : enabled);
            if (previous.name().equals("root") && (!next.enabled() || !next.role().equals("write"))) throw new ClusterException("FORBIDDEN", "不能禁用或降级 root");
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
    private ByteString operation(String method, String user, String password, String role, Boolean enabled) {
        try {
            // 使用集群私密密钥计算请求指纹，不能保存明文密码或可快速字典比对的裸 SHA 摘要。
            var mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(Base64.getDecoder().decode(keys().privateKey()), "HmacSHA256"));
            byte[] input = JSON.writeValueAsBytes(Arrays.asList(method, user, password, role, enabled));
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
