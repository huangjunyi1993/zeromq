package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.interceptor.ProducerInterceptor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 消息生产者抽象类，定义了拦截器逻辑，可定制拦截器链
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class BaseProducer extends AbstractProducer {

    // 拦截器链
    private List<ProducerInterceptor> interceptorChain = new ArrayList<>();

    public BaseProducer(AbstractConfig config) {
        super(config);
    }

    @Override
    protected void postOnError(Message message, Response response, Exception e) {
        // 当发送消息发生错误时，执行拦截器链里的拦截器的错误后置拦截
        interceptorChain.forEach(interceptor -> interceptor.postOnError(message, response, e));
    }

    @Override
    protected void postResponseReceived(Message message, Response response) {
        // 当发送消息后，执行拦截器链里的拦截器的发送后置拦截
        interceptorChain.forEach(interceptor -> interceptor.postResponseReceived(message, response));
    }

    @Override
    protected void preSendMessage(Message message) {
        // 当发送消息前，执行拦截器链里的拦截器的发送前置拦截
        interceptorChain.forEach(interceptor -> interceptor.preSendMessage(message));
    }

    /**
     * 注册拦截器
     * @param interceptor
     */
    public void registerInterceptor(ProducerInterceptor interceptor) {
        interceptorChain.add(interceptor);
        interceptorChain.sort(Comparator.comparingInt(ProducerInterceptor::order));
    }

}
