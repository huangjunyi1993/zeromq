package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.interceptor.ConsumerInterceptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 消费者客户端基础启动器：定义了拦截器逻辑
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class BaseConsumerBootStrap extends AbstractConsumerBootStrap {

    private List<ConsumerInterceptor> interceptorChain = new ArrayList<>();

    public BaseConsumerBootStrap(AbstractConfig config) {
        super(config);
    }

    @Override
    protected boolean postOnError(Consumer consumer, Message message, String topic, Exception e, boolean result) {
        boolean[] currResult = {result};
        interceptorChain.forEach(interceptor -> currResult[0] = interceptor.postOnError(consumer, message, topic, currResult[0]));
        return currResult[0];
    }

    @Override
    protected void postHandlerMessage(Consumer consumer, Message message, String topic) {
        interceptorChain.forEach(interceptor -> interceptor.postHandlerMessage(consumer, message, topic));
    }

    @Override
    protected void preHandlerMessage(Consumer consumer, Message message, String topic) {
        interceptorChain.forEach(interceptor -> interceptor.preHandlerMessage(consumer, message, topic));
    }

    public void registerInterceptor(ConsumerInterceptor interceptor) {
        interceptorChain.add(interceptor);
        interceptorChain.sort(Comparator.comparingInt(ConsumerInterceptor::order));
    }
}
