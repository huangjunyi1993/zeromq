package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于测试的消息处理
 * Created by huangjunyi on 2022/8/26.
 */
public class PrintlnConsumer extends ZeroSimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintlnConsumer.class);

    @Override
    protected boolean onMessageBody(Message message, Object messageBody) {
        LOGGER.info("Receive a message: {}", messageBody);
        return true;
    }

}
