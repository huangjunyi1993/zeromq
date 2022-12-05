package com.huangjunyi1993.zeromq.client.interceptor;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.consumer.Consumer;

/**
 * 消费者拦截器
 * Created by huangjunyi on 2022/8/14.
 */
public interface ConsumerInterceptor {

    /**
     * 消息消费异常后置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     * @param e 消费结果
     * @return
     */
    boolean postOnError(Consumer consumer, Message message, String topic, boolean e);

    /**
     * 消息消费后置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     */
    void postHandlerMessage(Consumer consumer, Message message, String topic);

    /**
     * 消息消费前置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     */
    void preHandlerMessage(Consumer consumer, Message message, String topic);

    /**
     * 排序
     * @return
     */
    int order();
}
