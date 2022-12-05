package com.huangjunyi1993.zeromq.client.remoting.transport;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.entity.ZeroAck;
import com.huangjunyi1993.zeromq.base.exception.ConsumerException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.client.consumer.AbstractConsumerBootStrap;
import com.huangjunyi1993.zeromq.client.remoting.support.ZeroFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.NO_CONSUMABLE_NEWS;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.ACK;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.REQUEST;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.RESPONSE;

/**
 * 消费者客户端处理器
 * Created by huangjunyi on 2022/8/19.
 */
@ChannelHandler.Sharable
public class ZeroConsumerHandler extends SimpleChannelInboundHandler<ZeroProtocol> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConsumerHandler.class);

    // 消费者启动引导器
    private AbstractConsumerBootStrap consumerBootStrap;

    public ZeroConsumerHandler(AbstractConsumerBootStrap consumerBootStrap) {
        // 保存引导器
        this.consumerBootStrap = consumerBootStrap;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ZeroProtocol protocol) throws Exception {
        if (protocol.getLen() > 0 && RESPONSE.getType() == protocol.getMessageType()) {

            // 根据协议报文中的序列化类型，进行发序列化
            Serializer serializer = SerializerFactory.getSerializer(protocol.getSerializationType());
            Response<Message> response = serializer.deserialize(protocol.getBody());

            // ack类型响应
            if (response.isAck()) {
                ZeroFuture.received(response);
                return;
            }

            ctx.executor().submit(() -> {

                // 消息拉取失败？
                if (!response.isSuccess()) {
                    LOGGER.info("fetch message failed: {}", response.errorMessage());
                    // 没有消息了
                    if (NO_CONSUMABLE_NEWS.getCode() == response.errorCode()) {
                        ctx.executor().schedule(() -> {
                            ZeroFuture.removeRequest(response.getId());
                            ZeroFuture.received(response);
                        }, 1, TimeUnit.SECONDS);
                        return;
                    }

                    ZeroFuture.removeRequest(response.getId());
                    ZeroFuture.received(response);
                    return;
                }

                long offset = response.getOffset();
                List<Message> messages = response.getData();
                for (Message message : messages) {
                    // 回调顶层消费者（消费者启动器），分发任务
                    if (!consumerBootStrap.onMessage(message)) {
                        break;
                    }
                    // 偏移量++
                    offset++;
                }

                // 回送ack
                // 取出全局请求信息表中对应的请求信息
                Request request = ZeroFuture.removeRequest(response.getId());
                // 请求信息记录了topic、groupId等，服务端更新offset需要
                ZeroAck ack = ZeroAck.success(offset, request.getTopic(), request.getConsumerGroupId(), request.getConsumerId(), request.getSerializationType());
                Serializer serializer1;
                try {
                    // 序列化
                    serializer1 = SerializerFactory.getSerializer(request.getSerializationType());
                    byte[] bytes = serializer1.serialize(ack);
                    // 包装为协议报文对象
                    ZeroProtocol protocol1 = new ZeroProtocol(bytes.length, request.getSerializationType(), ACK.getType(), response.getId(), bytes);
                    // 发送数据到通道
                    ctx.channel().writeAndFlush(protocol1);
                } catch (Exception e) {
                    throw new ConsumerException("send ack happen error", e);
                }

            });
        }
    }
}
