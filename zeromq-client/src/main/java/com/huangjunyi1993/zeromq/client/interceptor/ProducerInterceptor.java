package com.huangjunyi1993.zeromq.client.interceptor;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;

/**
 * 生产者拦截器
 * Created by huangjunyi on 2022/8/14.
 */
public interface ProducerInterceptor {

    int order();

    void postOnError(Message message, Response response, Exception e);

    void postResponseReceived(Message message, Response response);

    void preSendMessage(Message message);

}
