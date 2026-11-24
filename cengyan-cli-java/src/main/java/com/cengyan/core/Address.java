package com.cengyan.core;

/** 计算节点的客户端地址 (host, port)。不可变。 */
public final class Address {
    private final String host;
    private final int port;

    public Address(String host, int port) {
        if (host == null || host.trim().isEmpty()) throw new IllegalArgumentException("计算节点地址的 host 不能为空");
        if (port < 1 || port > 65535) throw new IllegalArgumentException("计算节点端口必须在 1 到 65535 之间：" + port);
        this.host = host;
        this.port = port;
    }

    public static Address of(String host, int port) {
        return new Address(host, port);
    }

    /** 解析 {@code host:port}；IPv6 写成 {@code [::1]:19669}。 */
    public static Address parse(String text) {
        if (text == null) throw new IllegalArgumentException("地址不能为空");
        String value = text.trim();
        String host;
        String port;
        if (value.startsWith("[")) {
            int end = value.indexOf("]:");
            if (end < 0) throw new IllegalArgumentException("地址格式应为 [ipv6]:port：" + text);
            host = value.substring(1, end);
            port = value.substring(end + 2);
        } else {
            int colon = value.lastIndexOf(':');
            if (colon <= 0 || value.indexOf(':') != colon) throw new IllegalArgumentException("地址格式应为 host:port：" + text);
            host = value.substring(0, colon);
            port = value.substring(colon + 1);
        }
        try {
            return new Address(host, Integer.parseInt(port));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("端口不是数字：" + text, ex);
        }
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Address)) return false;
        Address that = (Address) other;
        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 31 + port;
    }

    @Override
    public String toString() {
        return host.indexOf(':') >= 0 ? "[" + host + "]:" + port : host + ":" + port;
    }
}
