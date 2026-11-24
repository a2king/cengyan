package com.cengyan.core.internal;

import com.cengyan.core.Address;

import java.util.List;
import java.util.Map;

/** {@code ClusterClient} 与网络之间的边界；测试用假实现替换。 */
public interface Transport extends AutoCloseable {
    /**
     * 发送一条请求，返回服务端响应（不检查 code）。
     *
     * @param timeoutMillis 总预算，null 用默认超时
     * @param write         写请求：断线时用同一 requestId 在其他节点重试，最终无法确认时返回 code=FAILED
     * @param address       指定目标节点；null 用当前默认入口。指定节点失败时改走默认入口
     */
    Map<String, Object> request(Map<String, Object> payload, Long timeoutMillis, boolean write, Address address);

    /** 替换计算节点地址表。 */
    void updateAddresses(List<Address> addresses);

    @Override
    void close();
}
