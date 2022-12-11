package com.huangjunyi1993.zeromq.core.writer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * 消息日志写入器接口
 * Created by huangjunyi on 2022/12/11.
 */
public interface MessageWriter {

    /**
     * 日志写入
     * @param topic 入职所属主题
     * @param bytes 日志信息
     * @param serializationType 序列化类型
     */
    void writeMessageLog(String topic, byte[] bytes, int serializationType) throws IOException, InterruptedException, IllegalAccessException, InvocationTargetException, InstantiationException;

}
