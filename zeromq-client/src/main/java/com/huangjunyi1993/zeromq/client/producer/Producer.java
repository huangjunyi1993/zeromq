package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.listener.ProducerListener;

/**
 * 生成者接口
 * Created by huangjunyi on 2022/8/11.
 */
public interface Producer {

    /**
     * 同步消息发送
     * @param message 消息
     * @return
     */
    boolean sendMessage(Message message);

    /**
     * 异步消息发送
     * @param message 消息
     * @param producerListener 发送者消息发送监听器，发送消息后接收到响应时回调
     * @return
     */
    boolean sendMessageAsync(Message message, ProducerListener producerListener);

}
