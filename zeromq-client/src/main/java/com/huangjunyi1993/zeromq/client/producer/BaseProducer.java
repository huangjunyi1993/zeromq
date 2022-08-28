package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.interceptor.ProducerInterceptor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 消息生产者抽象类，定义了拦截器逻辑，可点击拦截器
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class BaseProducer extends AbstractProducer {

    private List<ProducerInterceptor> interceptorChain = new ArrayList<>();

    public BaseProducer(AbstractConfig config) {
        super(config);
    }

    @Override
    protected void postOnError(Message message, Response response, Exception e) {
        interceptorChain.forEach(interceptor -> interceptor.postOnError(message, response, e));
    }

    @Override
    protected void postResponseReceived(Message message, Response response) {
        interceptorChain.forEach(interceptor -> interceptor.postResponseReceived(message, response));
    }

    @Override
    protected void preSendMessage(Message message) {
        interceptorChain.forEach(interceptor -> interceptor.preSendMessage(message));
    }

    public void registerInterceptor(ProducerInterceptor interceptor) {
        interceptorChain.add(interceptor);
        interceptorChain.sort(Comparator.comparingInt(ProducerInterceptor::order));
    }

}
