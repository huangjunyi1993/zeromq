package com.huangjunyi1993.zeromq.client.remoting.transport;

import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.client.remoting.support.ZeroFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.RESPONSE;

/**
 * 生产者客户端处理器
 * Created by huangjunyi on 2022/8/14.
 */
@ChannelHandler.Sharable
public class ZeroProducerHandler extends SimpleChannelInboundHandler<ZeroProtocol> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ZeroProtocol protocol) throws Exception {
        // 生产者发送消息后，接收到服务端响应
        if (protocol.getLen() > 0 && RESPONSE.getType() == protocol.getMessageType()) {
            // 根据服务端返回协议报文中的序列化类型，从序列化工厂，获取对应的序列化器
            Serializer serializer = SerializerFactory.getSerializer(protocol.getSerializationType());
            // 系列化协议报文体
            Response response = serializer.deserialize(protocol.getBody());
            // 触发消息发送回执的回调，唤醒阻塞等待的线程
            ZeroFuture.received(response);
        }
    }
}
