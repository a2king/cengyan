package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.exception.ConnectionException;
import com.cengyan.core.exception.ProtocolException;
import com.cengyan.core.exception.ReadTimeoutException;
import com.cengyan.core.exception.RequestTooLargeException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 一条 JSON Lines 连接：一行请求、一行响应。首次请求时才建连。
 *
 * <p>任何 IO 错误、超时或无法解析的响应都会让连接作废（{@link #reusable()} 为 false）：
 * 请求发出后没有读完响应，连接上可能还会到达一条迟到的响应，不能再复用。
 *
 * <p>Compute 端口本身不终止 TLS；传入 {@link SSLContext} 时连接的是前置的 TLS 终止代理。
 */
public final class LineConnection implements Connection {
    /** 服务端 LineBasedFrameDecoder 的单行上限；超过时服务端直接断开连接。 */
    public static final int MAX_LINE_BYTES = 8 * 1024 * 1024;
    private static final int READ_CHUNK = 64 * 1024;

    private final Address address;
    private final SSLContext sslContext;
    private Socket socket;
    private InputStream input;
    private OutputStream output;
    private volatile boolean broken;
    private final byte[] buffer = new byte[READ_CHUNK];
    private int bufferStart;
    private int bufferEnd;

    public LineConnection(Address address, SSLContext sslContext) {
        this.address = address;
        this.sslContext = sslContext;
    }

    @Override
    public Address address() {
        return address;
    }

    @Override
    public boolean reusable() {
        return !broken && (socket == null || !socket.isClosed());
    }

    @Override
    public Map<String, Object> request(Map<String, Object> payload, long timeoutMillis) {
        if (broken) throw new ConnectionException("连接已作废", "CONNECTION_ERROR", null);
        // 先编码再连网：编码失败（如 NaN）或超长时请求一个字节都没发出，连接保持可用。
        byte[] data = Json.encode(payload);
        if (data.length > MAX_LINE_BYTES) {
            throw new RequestTooLargeException("请求 " + data.length + " 字节，超过服务端单行 " + MAX_LINE_BYTES + " 字节上限，未发送",
                    "REQUEST_TOO_LARGE", null);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1, timeoutMillis));
        try {
            if (socket == null) connect(timeoutMillis);
            output.write(data);
            output.write('\n');
            output.flush();
            return parse(readLine(deadline));
        } catch (SocketTimeoutException ex) {
            broken = true;
            close();
            throw new ReadTimeoutException("读取响应超时", "READ_TIMEOUT", null, ex);
        } catch (IOException ex) {
            broken = true;
            close();
            throw new ConnectionException(String.valueOf(ex.getMessage()), "CONNECTION_ERROR", null, ex);
        }
    }

    private void connect(long timeoutMillis) throws IOException {
        Socket plain = new Socket();
        try {
            plain.connect(new InetSocketAddress(address.host(), address.port()), (int) Math.min(Integer.MAX_VALUE, Math.max(1, timeoutMillis)));
            plain.setTcpNoDelay(true);
            plain.setKeepAlive(true);
            Socket connected = plain;
            if (sslContext != null) {
                SSLSocket secure = (SSLSocket) sslContext.getSocketFactory().createSocket(plain, address.host(), address.port(), true);
                SSLParameters parameters = secure.getSSLParameters();
                // 校验证书主机名：默认的 SSLSocket 不做这一步。
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                secure.setSSLParameters(parameters);
                secure.setSoTimeout((int) Math.min(Integer.MAX_VALUE, Math.max(1, timeoutMillis)));
                secure.startHandshake();
                connected = secure;
            }
            socket = connected;
            input = connected.getInputStream();
            output = new java.io.BufferedOutputStream(connected.getOutputStream(), 64 * 1024);
        } catch (IOException ex) {
            try {
                plain.close();
            } catch (IOException ignored) {
                // 已经在失败路径上
            }
            throw ex;
        }
    }

    /** 读到换行为止。每次读之前按剩余预算设置超时，慢速滴流也不会越过截止时间。 */
    private byte[] readLine(long deadline) throws IOException {
        ByteArrayOutputStream line = null;
        while (true) {
            for (int i = bufferStart; i < bufferEnd; i++) {
                if (buffer[i] == '\n') {
                    byte[] result;
                    if (line == null) {
                        result = new byte[i - bufferStart];
                        System.arraycopy(buffer, bufferStart, result, 0, result.length);
                    } else {
                        line.write(buffer, bufferStart, i - bufferStart);
                        result = line.toByteArray();
                    }
                    bufferStart = i + 1;
                    return result;
                }
            }
            if (bufferEnd > bufferStart) {
                if (line == null) line = new ByteArrayOutputStream(READ_CHUNK * 2);
                line.write(buffer, bufferStart, bufferEnd - bufferStart);
            }
            bufferStart = bufferEnd = 0;
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0) throw new SocketTimeoutException("响应超过截止时间");
            socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE, remaining));
            int read = input.read(buffer, 0, buffer.length);
            if (read < 0) throw new IOException("服务端关闭了连接");
            bufferEnd = read;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parse(byte[] line) {
        Object value;
        try {
            value = Json.decode(line);
        } catch (IllegalArgumentException ex) {
            broken = true;
            close();
            throw new ProtocolException("服务端响应不是合法 JSON", "PROTOCOL_ERROR", Collections.<String, Object>emptyMap(), ex);
        }
        if (!(value instanceof Map)) {
            broken = true;
            close();
            throw new ProtocolException("服务端响应不是 JSON 对象", "PROTOCOL_ERROR", null);
        }
        return (Map<String, Object>) value;
    }

    @Override
    public void close() {
        broken = true;
        Socket current = socket;
        if (current != null) {
            try {
                current.close();
            } catch (IOException ignored) {
                // 关闭失败不影响调用方
            }
        }
    }
}
