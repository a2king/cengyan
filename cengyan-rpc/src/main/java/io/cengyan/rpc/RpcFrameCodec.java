package io.cengyan.rpc;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.TooLongFrameException;
import java.util.List;

/** v1 帧头：magic(4)、version(2)、type(1)、flags(1)、correlation(8)、stream(8)、length(4)。 */
public final class RpcFrameCodec {
    public static final int HEADER_BYTES = 28;
    public static final int MAGIC = 0x43475250;
    public static final int VERSION = 1;

    private RpcFrameCodec() { }

    public static final class Encoder extends MessageToByteEncoder<RpcFrame> {
        private final int maxPayload;
        public Encoder(int maxPayload) {
            if (maxPayload < 1) throw new IllegalArgumentException("帧大小上限必须为正数");
            this.maxPayload = maxPayload;
        }
        @Override
        protected void encode(ChannelHandlerContext ctx, RpcFrame frame, ByteBuf out) {
            if (frame.payload().size() > maxPayload) throw new TooLongFrameException("RPC 消息超过大小上限");
            out.writeInt(MAGIC).writeShort(VERSION).writeByte(frame.type().wireId()).writeByte(0)
                    .writeLong(frame.correlationId()).writeLong(frame.streamId()).writeInt(frame.payload().size());
            out.writeBytes(frame.payload().asReadOnlyByteBuffer());
        }
    }

    public static final class Decoder extends ByteToMessageDecoder {
        private final int maxPayload;
        public Decoder(int maxPayload) {
            if (maxPayload < 1) throw new IllegalArgumentException("帧大小上限必须为正数");
            this.maxPayload = maxPayload;
        }
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < HEADER_BYTES) return;
            in.markReaderIndex();
            if (in.readInt() != MAGIC || in.readUnsignedShort() != VERSION) {
                throw new CorruptedFrameException("RPC magic 或协议版本不匹配");
            }
            int typeId = in.readUnsignedByte();
            if (in.readUnsignedByte() != 0) throw new CorruptedFrameException("RPC flags 不受支持");
            long correlation = in.readLong();
            long stream = in.readLong();
            int length = in.readInt();
            if (length < 0) throw new CorruptedFrameException("RPC 消息长度不能为负数");
            if (length > maxPayload) throw new TooLongFrameException("RPC 消息超过大小上限");
            RpcFrame.Type type;
            try {
                type = RpcFrame.Type.fromWire(typeId);
                if (correlation <= 0 || stream < 0) throw new IllegalArgumentException();
            } catch (IllegalArgumentException ex) {
                throw new CorruptedFrameException("RPC 消息类型或标识无效", ex);
            }
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }
            ByteString payload = ByteString.copyFrom(in.nioBuffer(in.readerIndex(), length));
            in.skipBytes(length);
            out.add(new RpcFrame(type, correlation, stream, payload));
        }
    }
}
