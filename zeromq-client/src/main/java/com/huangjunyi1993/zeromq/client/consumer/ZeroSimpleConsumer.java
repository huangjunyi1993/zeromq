package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;

import java.io.IOException;

import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;

/**
 * 简单消费者：自动进行消息反序列化
 * Created by huangjunyi on 2022/8/28.
 */
public abstract class ZeroSimpleConsumer implements Consumer {
    @Override
    public boolean onMessage(Message message) throws IOException, ClassNotFoundException {
        Serializer serializer = SerializerFactory.getSerializer((int) message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE));
        Object messageBody = serializer.deserialize(message.getBody());
        return onMessageBody(message, messageBody);
    }

    protected abstract boolean onMessageBody(Message message, Object messageBody);
}
