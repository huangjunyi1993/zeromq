package com.huangjunyi1993.zeromq.core.handler;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_ACK;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.ACK;

/**
 * 处理器：处理消费者ACK
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroAckHandler implements Handler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroAckHandler.class);

    @Override
    public void handle(Context context) {
        LOGGER.info("Receive an ACK sent back by the consumer: {}", (context.getVariable(CONTEXT_VARIABLE_ACK)));
    }

    @Override
    public int messageType() {
        return ACK.getType();
    }
}
