package io.cengyan.rpc;

import com.google.protobuf.ByteString;
import java.util.Objects;

/** 不持有 Netty 引用计数缓冲区，异步处理不依赖 channelRead 生命周期。 */
public record RpcFrame(Type type, long correlationId, long streamId, ByteString payload) {
    public enum Type {
        REQUEST(1), RESPONSE(2), OPEN(3), DATA_BATCH(4), CREDIT(5), CANCEL(6), END(7), ERROR(8), PING(9), PONG(10);
        private final int wireId;
        Type(int wireId) { this.wireId = wireId; }
        public int wireId() { return wireId; }
        static Type fromWire(int id) {
            for (Type type : values()) if (type.wireId == id) return type;
            throw new IllegalArgumentException("未知 RPC 消息类型：" + id);
        }
    }

    public RpcFrame {
        Objects.requireNonNull(type);
        Objects.requireNonNull(payload);
        if (correlationId <= 0 || streamId < 0) throw new IllegalArgumentException("RPC 请求标识无效");
    }
}
