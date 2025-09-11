package me.xiaoying.moebroker.api.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import me.xiaoying.moebroker.api.utils.SerializationUtil;

import java.io.Serializable;

public class SerializableEncoder extends MessageToByteEncoder<Serializable> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {
        byte[] bytes = SerializationUtil.serialize(msg);

        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }
}