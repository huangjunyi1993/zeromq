package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.handler.ZeroProducerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.*;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.*;

/**
 * 拦截器：生产者协议解析
 * Created by huangjunyi on 2022/8/21.
 */
public class ProducerProtocolParseInterceptor extends AbstractProtocolParseInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerProtocolParseInterceptor.class);

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroProducerHandler;
    }

    @Override
    public void pre(Context context) {
        super.pre(context);

        //反序列化报文体
        Message message = deserialize(context);
        context.setVariable(CONTEXT_VARIABLE_MESSAGE, message);

        ZeroProtocol protocal = context.getProtocal();
        if (message.getHead(MESSAGE_HEAD_ID) == null) {
            message.putHead(MESSAGE_HEAD_ID, protocal.getId());
        }

        // 消息id
        if ((long) message.getHead(MESSAGE_HEAD_ID) != protocal.getId()) {
            LOGGER.info("The message ID is inconsistent with the protocol header ID: messageId={}, protocalHeadId={}", message.getHead(MESSAGE_HEAD_ID), protocal.getId());
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, ID_NOT_CONSISTENT.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The message ID is inconsistent with the protocol header ID");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("The message ID is inconsistent with the protocol header ID");
        }

        // 消息头不包含序列化类型，设置序列化类型到消息头
        if (message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE) == null) {
            message.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, protocal.getSerializationType());
        }

        // 消息头设置的序列化类型，是否和协议报文一致
        if ((int) message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE) != protocal.getSerializationType()) {
            LOGGER.info("The message serialization type is inconsistent: message={}, protocal={}", message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE), protocal.getSerializationType());
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, SERIALIZATION_TYPE_NOT_CONSISTENT.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The message serialization type is inconsistent");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("The message serialization type is inconsistent");
        }

        // 消息头是否没有指定topic
        if (message.getHead(MESSAGE_HEAD_TOPIC) == null) {
            LOGGER.info("The message topic is null");
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, MESSAGE_TOPIC_IS_NULL.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The message topic is null");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("The message topic is null");
        }
        context.setVariable(CONTEXT_VARIABLE_TOPIC, message.getHead(MESSAGE_HEAD_TOPIC));
    }

    @Override
    public void after(Context context) {

    }

}
