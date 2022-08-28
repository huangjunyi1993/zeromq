package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.core.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_BODY;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.DESERIALIZE_FAILED;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.PROTOCAL_BODY_EMPTY;

/**
 * 拦截器：协议解析抽象类
 * Created by huangjunyi on 2022/8/21.
 */
public abstract class AbstractProtocolParseInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProtocolParseInterceptor.class);

    @Override
    public void pre(Context context) {
        ZeroProtocol protocal = context.getProtocal();

        context.setVariable(CONTEXT_VARIABLE_BODY_LEN, protocal.getLen());
        context.setVariable(CONTEXT_VARIABLE_ID, protocal.getId());
        context.setVariable(CONTEXT_VARIABLE_MESSAGE_TYPE, protocal.getMessageType());

        byte[] body = protocal.getBody();
        if (body == null || body.length == 0) {
            LOGGER.info("The protocol body cannot be empty");
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, PROTOCAL_BODY_EMPTY.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "The protocol body cannot be empty");
            throw new ServerHandleException("The protocol body cannot be empty");
        }
        context.setVariable(CONTEXT_VARIABLE_BODY, body);

        int serializationType = protocal.getSerializationType();
        context.setVariable(CONTEXT_VARIABLE_SERIALIZATION_TYPE, serializationType);
    }

    protected <T> T deserialize(Context context){
        try {
            ZeroProtocol protocal = context.getProtocal();
            Serializer serializer = SerializerFactory.getSerializer(protocal.getSerializationType());
            return serializer.deserialize(protocal.getBody());
        } catch (IOException |ClassNotFoundException e) {
            LOGGER.info("Message deserialization failed: ", e);
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, DESERIALIZE_FAILED.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "Message deserialization failed");
            throw new ServerHandleException("Message deserialization failed: ", e);
        }
    }

    @Override
    public int order() {
        return 1;
    }
}
