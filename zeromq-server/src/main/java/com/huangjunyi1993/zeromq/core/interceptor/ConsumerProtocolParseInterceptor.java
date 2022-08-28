package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.handler.ZeroConsumerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.*;

/**
 * 拦截器：消费者协议解析
 * Created by huangjunyi on 2022/8/21.
 */
public class ConsumerProtocolParseInterceptor extends AbstractProtocolParseInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerProtocolParseInterceptor.class);

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroConsumerHandler;
    }

    @Override
    public void pre(Context context) {
        super.pre(context);
        Request request = deserialize(context);
        context.setVariable(CONTEXT_VARIABLE_REQUEST, request);
        if (request.getTopic() == null || "".equals(request.getTopic())) {
            LOGGER.info("The request topic is null");
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, REQUEST_TOPIC_IS_NULL.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The request topic is null");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("The request topic is null");
        }
        if (request.getBatch() <= 0) {
            LOGGER.info("Batch illegal");
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, BATCH_ILLEGAL.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "Batch illegal");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("Batch illegal");
        }
    }

    @Override
    public void after(Context context) {

    }

}
