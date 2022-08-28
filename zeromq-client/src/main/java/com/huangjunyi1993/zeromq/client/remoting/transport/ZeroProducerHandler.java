package com.huangjunyi1993.zeromq.client.remoting.transport;

import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.client.remoting.support.ZeroFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.RESPONSE;

/**
 * 生成者客户单处理器
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroProducerHandler extends SimpleChannelInboundHandler<ZeroProtocol> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ZeroProtocol protocol) throws Exception {
        if (protocol.getLen() > 0 && RESPONSE.getType() == protocol.getMessageType()) {
            Serializer serializer = SerializerFactory.getSerializer(protocol.getSerializationType());
            Response response = serializer.deserialize(protocol.getBody());
            ZeroFuture.received(response);
        }
    }
}
