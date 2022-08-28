package com.huangjunyi1993.zeromq.base.coder;

import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

/**
 * 协议包解码器
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroProtocalDecoder extends ReplayingDecoder<Void> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() >= 20) {
            int len = in.readInt();
            int serializationType = in.readInt();
            int messageType = in.readInt();
            long id = in.readLong();
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            ZeroProtocol protocol = new ZeroProtocol(len, serializationType, messageType, id, bytes);
            out.add(protocol);
        }
    }
}
