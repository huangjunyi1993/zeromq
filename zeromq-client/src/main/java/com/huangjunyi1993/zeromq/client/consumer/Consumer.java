package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.base.entity.Message;

import java.io.IOException;

/**
 * 单个消费者
 * Created by huangjunyi on 2022/8/14.
 */
public interface Consumer {

    boolean onMessage(Message message) throws IOException, ClassNotFoundException;

}
