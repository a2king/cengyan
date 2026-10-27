package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * 用户存在目录列族里。密码只存盐和 PBKDF2 摘要，会话本身不落盘。
 */
public final class Auth {
    public static final int ROLE_READ = 1;
    public static final int ROLE_WRITE = 2;

    static final int ITERATIONS = 600_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BYTES = 32;
    private static final int RECORD_BYTES = SALT_BYTES + HASH_BYTES + 1;
    private static final byte[] DUMMY_SALT = new byte[SALT_BYTES];
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Logger log = LoggerFactory.getLogger(Auth.class);

    private final GraphStore store;

    public Auth(GraphStore store) {
        if (store == null) {
            throw new IllegalArgumentException("存储不能为空");
        }
        this.store = store;
    }

    public record User(String name, int role) {
    }

    /** 一个用户都没有时创建读写用户 root。已经有用户则不动密码。 */
    public boolean ensureRoot(String password) {
        checkPassword(password);
        return store.withWriteLock(() -> {
            if (anyUser()) {
                return false;
            }
            write("root", password, ROLE_WRITE);
            log.info("已创建读写用户 root");
            return true;
        });
    }

    public void createUser(String name, String password, int role) {
        Catalog.checkIdentifier(name);
        checkPassword(password);
        checkRole(role);
        byte[] salt = newSalt();
        byte[] digest = hash(password, salt);
        store.withWriteLock(() -> {
            if (load(name) != null) {
                throw new CengyanException(ErrorCode.NAME_CONFLICT, "用户已存在");
            }
            store.mutate(record(name, salt, digest, role));
            return null;
        });
    }

    public void setPassword(String name, String password) {
        Catalog.checkIdentifier(name);
        checkPassword(password);
        byte[] salt = newSalt();
        byte[] digest = hash(password, salt);
        store.withWriteLock(() -> {
            Stored stored = load(name);
            if (stored == null) {
                throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "用户不存在");
            }
            store.mutate(record(name, salt, digest, stored.role));
            return null;
        });
    }

    public void setRole(String name, int role) {
        Catalog.checkIdentifier(name);
        checkRole(role);
        store.withWriteLock(() -> {
            Stored stored = load(name);
            if (stored == null) {
                throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "用户不存在");
            }
            if (stored.role == ROLE_WRITE && role != ROLE_WRITE && writers() <= 1) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "至少要留一个读写用户");
            }
            store.mutate(record(name, stored.salt, stored.hash, role));
            return null;
        });
    }

    public void dropUser(String name) {
        Catalog.checkIdentifier(name);
        store.withWriteLock(() -> {
            Stored stored = load(name);
            if (stored == null) {
                throw new CengyanException(ErrorCode.TYPE_NOT_FOUND, "用户不存在");
            }
            if (stored.role == ROLE_WRITE && writers() <= 1) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "至少要留一个读写用户");
            }
            StoreBatch batch = new StoreBatch();
            batch.delete(Cf.CATALOG, KeyCodec.userKey(name));
            store.mutate(batch);
            return null;
        });
    }

    /** 用户不存在和密码错误返回同一个空结果，避免泄漏哪一项不对。 */
    public User authenticate(String name, String password) {
        if (password == null) {
            password = "";
        }
        boolean named = false;
        if (name != null && !name.isEmpty()) {
            try {
                Catalog.checkIdentifier(name);
                named = true;
            } catch (CengyanException ex) {
                named = false;
            }
        }
        Stored stored = null;
        if (named) {
            try (ReadSession session = store.read()) {
                stored = decode(session.get(Cf.CATALOG, KeyCodec.userKey(name)));
            }
        }
        byte[] salt = stored == null ? DUMMY_SALT : stored.salt;
        byte[] digest = hash(password, salt);
        if (stored == null || !MessageDigest.isEqual(digest, stored.hash)) {
            return null;
        }
        return new User(name, stored.role);
    }

    private void write(String name, String password, int role) {
        byte[] salt = newSalt();
        store.mutate(record(name, salt, hash(password, salt), role));
    }

    private boolean anyUser() {
        try (ReadSession session = store.read()) {
            return !session.keysWithPrefix(Cf.CATALOG, KeyCodec.userPrefix()).isEmpty();
        }
    }

    private int writers() {
        int count = 0;
        try (ReadSession session = store.read()) {
            for (byte[] key : session.keysWithPrefix(Cf.CATALOG, KeyCodec.userPrefix())) {
                Stored stored = decode(session.get(Cf.CATALOG, key));
                if (stored != null && stored.role == ROLE_WRITE) {
                    count++;
                }
            }
        }
        return count;
    }

    private Stored load(String name) {
        try (ReadSession session = store.read()) {
            return decode(session.get(Cf.CATALOG, KeyCodec.userKey(name)));
        }
    }

    private static StoreBatch record(String name, byte[] salt, byte[] digest, int role) {
        byte[] value = new byte[RECORD_BYTES];
        System.arraycopy(salt, 0, value, 0, SALT_BYTES);
        System.arraycopy(digest, 0, value, SALT_BYTES, HASH_BYTES);
        value[RECORD_BYTES - 1] = (byte) role;
        StoreBatch batch = new StoreBatch();
        batch.put(Cf.CATALOG, KeyCodec.userKey(name), value);
        return batch;
    }

    private static Stored decode(byte[] value) {
        if (value == null) {
            return null;
        }
        if (value.length != RECORD_BYTES) {
            throw new CengyanException(ErrorCode.INTERNAL, "用户记录不完整");
        }
        int role = value[RECORD_BYTES - 1] & 0xFF;
        if (role != ROLE_READ && role != ROLE_WRITE) {
            throw new CengyanException(ErrorCode.INTERNAL, "用户记录不完整");
        }
        return new Stored(
                Arrays.copyOfRange(value, 0, SALT_BYTES),
                Arrays.copyOfRange(value, SALT_BYTES, SALT_BYTES + HASH_BYTES),
                role);
    }

    private static byte[] newSalt() {
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        return salt;
    }

    private static byte[] hash(String password, byte[] salt) {
        char[] chars = password.toCharArray();
        PBEKeySpec spec = new PBEKeySpec(chars, salt, ITERATIONS, HASH_BYTES * 8);
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return factory.generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException ex) {
            throw new CengyanException(ErrorCode.INTERNAL, "密码无法校验", ex);
        } finally {
            Arrays.fill(chars, '\0');
            spec.clearPassword();
        }
    }

    private static void checkPassword(String password) {
        if (password == null || password.isEmpty()) {
            throw new CengyanException(ErrorCode.SYNTAX, "密码不能为空");
        }
    }

    private static void checkRole(int role) {
        if (role != ROLE_READ && role != ROLE_WRITE) {
            throw new CengyanException(ErrorCode.SYNTAX, "角色只能是 read 或 write");
        }
    }

    private record Stored(byte[] salt, byte[] hash, int role) {
    }
}
