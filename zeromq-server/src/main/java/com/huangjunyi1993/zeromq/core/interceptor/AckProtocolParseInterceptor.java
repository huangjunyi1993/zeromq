package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Ack;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.handler.ZeroAckHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.*;

/**
 * 拦截器：消费者回送ACK协议解析
 * Created by huangjunyi on 2022/8/21.
 */
public class AckProtocolParseInterceptor extends AbstractProtocolParseInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckProtocolParseInterceptor.class);

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroAckHandler;
    }

    @Override
    public void pre(Context context) {
        super.pre(context);
        Ack ack = deserialize(context);
        context.setVariable(CONTEXT_VARIABLE_ACK, ack);
        if (ack.topic() == null || "".equals(ack.topic())) {
            LOGGER.info("The ack topic is null");
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, ACK_TOPIC_IS_NULL.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The ack topic is null");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("The ack topic is null");
        }

    }

    @Override
    public void after(Context context) {

    }
}
