package com.huangjunyi1993.zeromq.client.interceptor;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;

/**
 * 生产者拦截器
 * Created by huangjunyi on 2022/8/14.
 */
public interface ProducerInterceptor {

    /**
     * 拦截器排序
     * @return
     */
    int order();

    /**
     * 发送消息发生错误后置拦截
     * @param message
     * @param response
     * @param e
     */
    void postOnError(Message message, Response response, Exception e);

    /**
     * 发送消息后置拦截
     * @param message
     * @param response
     */
    void postResponseReceived(Message message, Response response);

    /**
     * 消息发送前置拦截
     * @param message
     */
    void preSendMessage(Message message);

}
