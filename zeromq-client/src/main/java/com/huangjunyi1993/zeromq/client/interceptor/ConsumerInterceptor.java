package com.huangjunyi1993.zeromq.client.interceptor;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.consumer.Consumer;

/**
 * 消费者拦截器
 * Created by huangjunyi on 2022/8/14.
 */
public interface ConsumerInterceptor {
    boolean postOnError(Consumer consumer, Message message, String topic, boolean e);

    void postHandlerMessage(Consumer consumer, Message message, String topic);

    void preHandlerMessage(Consumer consumer, Message message, String topic);

    int order();
}
