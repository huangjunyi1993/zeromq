package com.huangjunyi1993.zeromq.base.coder;

import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 消息包编码器
 * Created by huangjunyi on 2022/8/13.
 */
public class ZeroProtocolEncoder extends MessageToByteEncoder<ZeroProtocol> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ZeroProtocol protocol, ByteBuf out) throws Exception {
        out.writeInt(protocol.getLen());
        out.writeInt(protocol.getSerializationType());
        out.writeInt(protocol.getMessageType());
        out.writeLong(protocol.getId());
        out.writeBytes(protocol.getBody());
    }

}
