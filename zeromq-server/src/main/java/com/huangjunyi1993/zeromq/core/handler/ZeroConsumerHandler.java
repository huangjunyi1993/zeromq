package com.huangjunyi1993.zeromq.core.handler;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.exception.ServerHandleException;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.util.FileUtil;
import com.huangjunyi1993.zeromq.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.constants.ServerConstant.INDEX_ITEM_LENGTH;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.IO_EXCEPTION;
import static com.huangjunyi1993.zeromq.base.enums.ErrorCodeEnum.NO_CONSUMABLE_NEWS;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.REQUEST;

/**
 * 处理器：处理消费者请求
 * Created by huangjunyi on 2022/8/20.
 */
public class ZeroConsumerHandler implements Handler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConsumerHandler.class);

    @Override
    public void handle(Context context) {
        try {
            // 以消费者消费偏移量为索引，读取索引文件对应的消息日志文件偏移量
            Request request = (Request) context.getVariable(CONTEXT_VARIABLE_REQUEST);
            String topic = request.getTopic();
            long consumerOffset = (long) context.getVariable(CONTEXT_VARIABLE_CONSUMER_OFFSET);
            // 定位最新索引文件的文件名，消费偏移量除以单个索引文件的最大条数（默认10000），就是最新的文件名
            long index = consumerOffset / GlobalConfiguration.get().getIndexFileCapacity();
            // 索引文件内偏移量：在该索引文件中，是第几条索引
            long indexOffset = consumerOffset % GlobalConfiguration.get().getIndexFileCapacity();
            // 根据消费者偏移量定位索引文件名
            String indexFileName = GlobalConfiguration.get().getIndexPath() + File.separator + topic + File.separator + index + ".index";
            if (!FileUtil.exists(indexFileName) || FileUtil.getFileSize(indexFileName) <= indexOffset * INDEX_ITEM_LENGTH) {
                handleNoConsumableMessage(context);
                return;
            }
            // 根据索引文件名和文件内偏移量，读取消息日志偏移量
            long messageLogOffset = IOUtil.readIndex(indexFileName, indexOffset);
            if (messageLogOffset == 0 && (FileUtil.getOffsetOfFileName(indexFileName) != 0 || indexOffset != 0)) {
                handleNoConsumableMessage(context);
                return;
            }

            // 根据日志文件偏移量和请求批次读取消息
            // 根据日志偏移量，确定目标日志文件名
            String logFileName = FileUtil.determineTargetLogFile(GlobalConfiguration.get().getLogPath() + File.separator + topic, messageLogOffset);
            // 获取文件名代表的偏移量
            long offsetOfFileName = FileUtil.getOffsetOfFileName(logFileName);
            // 文件内偏移量 = 日志偏移量 - 文件名代表的偏移量
            long messageOffsetInLogFile = messageLogOffset - offsetOfFileName;
            // 按批次读取消息 也是以内存映射的方式
            List<Message> messages = IOUtil.readLog(logFileName, messageOffsetInLogFile, request.getBatch());

            // 把message的序列化类型重写为客户端要求的类型，结果写入context
            // messages.forEach(message -> message.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, context.getVariable(CONTEXT_VARIABLE_SERIALIZATION_TYPE)));
            context.setVariable(CONTEXT_VARIABLE_RESULT, messages);
            context.setVariable(CONTEXT_VARIABLE_BATCH_OFFSET, consumerOffset);
        } catch (IOException|ClassNotFoundException e) {
            LOGGER.info("Reading the message log failed:{}", e);
            context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, IO_EXCEPTION.getCode());
            context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "Reading the message log failed");
            context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
            throw new ServerHandleException("Reading the message log failed");
        }

    }

    private void handleNoConsumableMessage(Context context) {
        LOGGER.info("No consumable message");
        context.setVariable(CONTEXT_VARIABLE_ERROR_CODE, NO_CONSUMABLE_NEWS.getCode());
        context.setVariable(CONTEXT_VARIABLE_ERROR_MESSAGE, "No consumable message");
        context.setVariable(CONTEXT_VARIABLE_HANDLE_SUCCESS, false);
    }

    @Override
    public int messageType() {
        return REQUEST.getType();
    }
}
