package com.cengyan.core.internal;

import com.cengyan.core.Address;

import java.util.Map;

/** 到一个计算节点的一条连接。同一时间只承载一个请求；实现不必是线程安全的。 */
public interface Connection {
    /**
     * 发送一条请求并读取一行响应。
     *
     * @param timeoutMillis 本次往返（含建连）的剩余预算，必须为正
     * @throws com.cengyan.core.exception.ConnectionException  建连失败或连接断开
     * @throws com.cengyan.core.exception.ReadTimeoutException 预算内没有收到响应
     * @throws com.cengyan.core.exception.ProtocolException    响应不是 JSON 对象
     * @throws com.cengyan.core.exception.RequestTooLargeException 编码后超过单行上限，未发送
     * @throws IllegalArgumentException 请求含无法编码的值，未发送
     */
    Map<String, Object> request(Map<String, Object> payload, long timeoutMillis);

    /** 连接仍可复用：没有出过错、没有超时、没有被关闭。 */
    boolean reusable();

    Address address();

    void close();

    /** 创建（尚未建连的）连接。 */
    interface Factory {
        Connection open(Address address);
    }
}
