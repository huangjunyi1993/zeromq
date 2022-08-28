package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.base.entity.Message;

/**
 * 用于测试的消息处理
 * Created by huangjunyi on 2022/8/26.
 */
public class PrintlnConsumer extends ZeroSimpleConsumer {

    @Override
    protected boolean onMessageBody(Message message, Object messageBody) {
        System.out.println((String) messageBody);
        return true;
    }

}
