package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.listener.ProducerListener;

/**
 * 生成者接口
 * Created by huangjunyi on 2022/8/11.
 */
public interface Producer {

    boolean sendMessage(Message message);

    boolean sendMessageAsync(Message message, ProducerListener producerListener);

}
