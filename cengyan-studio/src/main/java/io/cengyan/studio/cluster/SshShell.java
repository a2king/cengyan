package io.cengyan.studio.cluster;

import com.jcraft.jsch.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;

/** JSch 实现。主机公钥在认证之前校验，指纹不一致直接断开，不发送密码。 */
final class SshShell implements RemoteShell {
    private final Session session;
    private final String hostKey;

    private SshShell(Session session, String hostKey) {
        this.session = session;
        this.hostKey = hostKey;
    }

    @Override public String hostKey() { return hostKey; }

    @Override public Result exec(String command, Duration timeout) throws IOException {
        ChannelExec channel = null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            channel.setErrStream(err, true);
            InputStream out = channel.getInputStream();
            channel.connect(15_000);
            ByteArrayOutputStream stdout = new ByteArrayOutputStream();
            long deadline = System.nanoTime() + timeout.toNanos();
            byte[] buffer = new byte[8192];
            while (true) {
                while (out.available() > 0) {
                    int read = out.read(buffer);
                    if (read < 0) break;
                    stdout.write(buffer, 0, read);
                }
                if (channel.isClosed() && out.available() == 0) break;
                if (System.nanoTime() > deadline) throw new ClusterError("REMOTE_TIMEOUT", "远程命令超时（" + timeout.toSeconds() + " 秒）");
                try { Thread.sleep(50); }
                catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new ClusterError("CANCELED", "远程命令被中断"); }
            }
            return new Result(channel.getExitStatus(), stdout.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
        } catch (JSchException ex) {
            throw new IOException("SSH 命令执行失败：" + ex.getMessage(), ex);
        } finally {
            if (channel != null) channel.disconnect();
        }
    }

    @Override public void upload(Path local, String remote, String mode) throws IOException {
        try (InputStream input = Files.newInputStream(local)) { put(input, remote, mode); }
    }

    @Override public void write(String remote, byte[] content, String mode) throws IOException {
        put(new ByteArrayInputStream(content), remote, mode);
    }

    /** 先传到同目录临时文件再改名，目标文件不会出现半截内容。 */
    private void put(InputStream input, String remote, String mode) throws IOException {
        String parent = remote.substring(0, Math.max(1, remote.lastIndexOf('/')));
        String temporary = remote + ".upload";
        run("mkdir -p " + RemoteShell.quote(parent), Duration.ofSeconds(30));
        ChannelSftp sftp = null;
        try {
            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(15_000);
            sftp.put(input, temporary);
            sftp.chmod(Integer.parseInt(mode, 8), temporary);
        } catch (JSchException | SftpException ex) {
            throw new IOException("上传 " + remote + " 失败：" + ex.getMessage(), ex);
        } finally {
            if (sftp != null) sftp.disconnect();
        }
        run("mv -f " + RemoteShell.quote(temporary) + " " + RemoteShell.quote(remote), Duration.ofSeconds(30));
    }

    @Override public void close() { session.disconnect(); }

    static String fingerprint(byte[] key) {
        try { return "SHA256:" + Base64.getEncoder().withoutPadding().encodeToString(MessageDigest.getInstance("SHA-256").digest(key)); }
        catch (Exception ex) { throw new IllegalStateException(ex); }
    }

    /** 私钥保存在 {@code data/ssh/id_rsa}（600），首次使用时生成 RSA 3072。 */
    static final class Factory implements RemoteShell.Factory {
        private final Path privateKey;
        private final Path publicKeyFile;

        Factory(Path dataDir) throws IOException {
            Path dir = dataDir.resolve("ssh");
            Files.createDirectories(dir);
            ClusterStore.restrict(dir, true);
            privateKey = dir.resolve("id_rsa");
            publicKeyFile = dir.resolve("id_rsa.pub");
            if (!Files.exists(privateKey)) generate();
        }

        private void generate() throws IOException {
            try {
                KeyPair pair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 3072);
                ByteArrayOutputStream priv = new ByteArrayOutputStream(), pub = new ByteArrayOutputStream();
                pair.writePrivateKey(priv);
                pair.writePublicKey(pub, "");
                pair.dispose();
                Path temporary = privateKey.resolveSibling("id_rsa.tmp");
                Files.write(temporary, priv.toByteArray());
                ClusterStore.restrict(temporary, false);
                Files.move(temporary, privateKey, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                Files.writeString(publicKeyFile, pub.toString(StandardCharsets.US_ASCII).strip() + "\n");
            } catch (JSchException ex) { throw new IOException("生成 Studio SSH 密钥失败", ex); }
        }

        @Override public String publicKey() throws IOException {
            String[] fields = Files.readString(publicKeyFile).strip().split("\\s+");
            return fields[0] + " " + fields[1];
        }

        @Override public RemoteShell open(String host, int port, String password, String expectedHostKey) throws IOException {
            JSch jsch = new JSch();
            Pinned pinned = new Pinned(expectedHostKey);
            jsch.setHostKeyRepository(pinned);
            try {
                if (password == null) jsch.addIdentity(privateKey.toString());
                Session session = jsch.getSession("root", host, port);
                if (password != null) {
                    session.setPassword(password);
                    session.setConfig("PreferredAuthentications", "password,keyboard-interactive");
                } else session.setConfig("PreferredAuthentications", "publickey");
                session.setConfig("StrictHostKeyChecking", "yes");
                session.setServerAliveInterval(15_000);
                session.setTimeout(30_000);
                session.connect(15_000);
                return new SshShell(session, pinned.seen);
            } catch (JSchException ex) {
                if (pinned.mismatch) throw new ClusterError("HOST_KEY_CHANGED", "服务器 " + host + " 的主机公钥与首次添加时不一致，已拒绝连接。若服务器重装过系统，请移除后重新添加");
                String message = ex.getMessage() == null ? "" : ex.getMessage();
                if (message.contains("Auth fail") || message.contains("Auth cancel"))
                    throw new ClusterError("SSH_AUTH_FAILED", password != null ? "root 密码错误，或服务器禁止 root 密码登录" : "免密登录失败，Studio 公钥可能已被删除");
                throw new ClusterError("SSH_UNREACHABLE", "无法连接 " + host + ":" + port + "：" + message, ex);
            }
        }
    }

    /** 首次连接记录指纹；之后只接受同一指纹。 */
    private static final class Pinned implements HostKeyRepository {
        private final String expected;
        volatile String seen = "";
        volatile boolean mismatch;
        Pinned(String expected) { this.expected = expected == null ? "" : expected; }
        @Override public int check(String host, byte[] key) {
            seen = fingerprint(key);
            if (expected.isEmpty() || expected.equals(seen)) return OK;
            mismatch = true;
            return CHANGED;
        }
        @Override public void add(HostKey hostkey, UserInfo ui) { }
        @Override public void remove(String host, String type) { }
        @Override public void remove(String host, String type, byte[] key) { }
        @Override public String getKnownHostsRepositoryID() { return "cengyan-studio"; }
        @Override public HostKey[] getHostKey() { return new HostKey[0]; }
        @Override public HostKey[] getHostKey(String host, String type) { return new HostKey[0]; }
    }
}
