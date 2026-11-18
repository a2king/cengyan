package com.cengyan.core.internal;

import com.cengyan.core.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/** 假连接工厂：记录每次请求（地址 + 请求副本），按脚本返回或抛出。 */
final class FakeConnections implements Connection.Factory {
    static final class Sent {
        final Address address;
        final Map<String, Object> payload;
        final long timeoutMillis;

        Sent(Address address, Map<String, Object> payload, long timeoutMillis) {
            this.address = address;
            this.payload = payload;
            this.timeoutMillis = timeoutMillis;
        }
    }

    final List<Sent> sent = Collections.synchronizedList(new ArrayList<Sent>());
    final List<Address> opened = Collections.synchronizedList(new ArrayList<Address>());
    final List<Address> closed = Collections.synchronizedList(new ArrayList<Address>());
    private final BiFunction<Address, Map<String, Object>, Object> script;

    /** script 返回 Map 作为响应，返回 RuntimeException 则抛出（并作废连接）。 */
    FakeConnections(BiFunction<Address, Map<String, Object>, Object> script) {
        this.script = script;
    }

    static FakeConnections ok() {
        return new FakeConnections((address, payload) -> response("OK"));
    }

    static Map<String, Object> response(String code) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("code", code);
        return map;
    }

    @Override
    public Connection open(Address address) {
        opened.add(address);
        return new Connection() {
            private boolean broken;
            private boolean isClosed;

            @Override
            public Map<String, Object> request(Map<String, Object> payload, long timeoutMillis) {
                sent.add(new Sent(address, new LinkedHashMap<String, Object>(payload), timeoutMillis));
                Object outcome = script.apply(address, payload);
                if (outcome instanceof RuntimeException) {
                    // 与 LineConnection 一致：发送前就拒绝的请求（编码失败、超长）不作废连接。
                    if (!(outcome instanceof IllegalArgumentException)
                            && !(outcome instanceof com.cengyan.core.exception.RequestTooLargeException)) broken = true;
                    throw (RuntimeException) outcome;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) outcome;
                return result;
            }

            @Override
            public boolean reusable() {
                return !broken && !isClosed;
            }

            @Override
            public Address address() {
                return address;
            }

            @Override
            public void close() {
                if (!isClosed) closed.add(address);
                isClosed = true;
            }
        };
    }
}
