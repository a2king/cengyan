package io.cengyan.studio.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

/** 一条到受管服务器的 root 会话。测试用假实现替换。 */
public interface RemoteShell extends AutoCloseable {
    record Result(int exit, String stdout, String stderr) {
        public boolean ok() { return exit == 0; }
        public String output() { return (stdout + (stderr.isBlank() ? "" : "\n" + stderr)).strip(); }
    }

    /** 本次连接看到的主机公钥指纹，格式 SHA256:base64。 */
    String hostKey();

    Result exec(String command, Duration timeout) throws IOException;

    void upload(Path local, String remote, String mode) throws IOException;

    void write(String remote, byte[] content, String mode) throws IOException;

    default void write(String remote, String content, String mode) throws IOException {
        write(remote, content.getBytes(StandardCharsets.UTF_8), mode);
    }

    /** 执行命令，非零退出码转成带输出的异常。 */
    default String run(String command, Duration timeout) throws IOException {
        Result result = exec(command, timeout);
        if (!result.ok()) throw new ClusterError("REMOTE_FAILED", "远程命令失败（退出码 " + result.exit() + "）：" + result.output());
        return result.stdout();
    }

    @Override void close();

    /** 打开会话。password 非空时用密码登录，否则用 Studio 私钥。expectedHostKey 非空时必须一致。 */
    interface Factory {
        RemoteShell open(String host, int port, String password, String expectedHostKey) throws IOException;
        /** Studio 的公钥（authorized_keys 一行，不含注释）。 */
        String publicKey() throws IOException;
    }

    /** 单引号包裹，用于把任意值安全地拼进 shell 命令。 */
    static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
