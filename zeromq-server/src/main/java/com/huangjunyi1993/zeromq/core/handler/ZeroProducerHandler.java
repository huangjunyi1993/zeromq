package com.huangjunyi1993.zeromq.core.handler;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.writer.SpinMessageWriter;
import com.huangjunyi1993.zeromq.core.writer.SynchronizedMessageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.*;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.MESSAGE;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;

/**
 * 处理器：处理生成者请求
 * Created by huangjunyi on 2022/8/20.
 */
public class ZeroProducerHandler implements Handler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroProducerHandler.class);

    @Override
    public void handle(Context context) {
        // 消息
        byte[] bytes = (byte[]) context.getVariable(CONTEXT_VARIABLE_BODY);
        // 主题
        String topic = (String) context.getVariable(CONTEXT_VARIABLE_TOPIC);
        // 序列化类型
        int serializationType = (int) context.getVariable(CONTEXT_VARIABLE_SERIALIZATION_TYPE);
        try {
            // 获取消息写入器代理，写入消息日志
            GlobalConfiguration.getMessageWriterProxy().writeMessageLog(topic, bytes, serializationType);
        } catch (Exception e) {
            LOGGER.info("An exception occurred while writing a message to the log file: ", e);
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, IO_EXCEPTION.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "An exception occurred while writing a message to the log file");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("An exception occurred while writing a message to the log file");
        }
    }

    @Override
    public int messageType() {
        return MESSAGE.getType();
    }
}
