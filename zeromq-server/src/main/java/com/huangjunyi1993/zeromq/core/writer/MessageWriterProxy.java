package com.huangjunyi1993.zeromq.core.writer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息写入器代理
 * Created by huangjunyi on 2022/12/11.
 */
public class MessageWriterProxy implements MessageWriter {

    private volatile static MessageWriterProxy messageWriterProxyInstance;

    private static final Object LOCK_OBJECT = new Object();

    private static Constructor<? extends MessageWriter> realWriterConstructor;

    // 消息日志写入器实例表 topic => 写入器实例
    private static final Map<String, MessageWriter> TOPIC_MESSAGE_LOG_UTIL_MAP = new ConcurrentHashMap<>();
    // 写入策略
    private final String writeStrategy;
    // 刷盘策略
    private final String flushStrategy;

    private static MessageWriter getMessageWriterByTopic(String topic) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        MessageWriter messageWriter = TOPIC_MESSAGE_LOG_UTIL_MAP.get(topic);
        if (null == messageWriter) {
            messageWriter = realWriterConstructor.newInstance();
            TOPIC_MESSAGE_LOG_UTIL_MAP.put(topic, messageWriter);
        }
        return messageWriter;
    }

    private MessageWriterProxy(String writeStrategy, String flushStrategy) throws NoSuchMethodException {
        this.writeStrategy = writeStrategy;
        this.flushStrategy = flushStrategy;
        ServiceLoader<MessageWriter> loader = ServiceLoader.load(MessageWriter.class);
        for (MessageWriter writer : loader) {
            WriteStrategy annotation = writer.getClass().getAnnotation(WriteStrategy.class);
            if (annotation.value().equals(writeStrategy)) {
                this.realWriterConstructor = writer.getClass().getConstructor();
            }
        }
    }

    @Override
    public void writeMessageLog(String topic, byte[] bytes, int serializationType) throws IOException, InterruptedException, IllegalAccessException, InstantiationException, InvocationTargetException {
        // 一个topic对应一个日志工具类实例 消息持久化到日志文件 并且建立索引
        ((AbstractMessageWriter) getMessageWriterByTopic(topic)).writeMessageLog(topic, bytes, serializationType, this.flushStrategy);
    }

    public static MessageWriterProxy getInstance(String writeStrategy, String flushStrategy) throws NoSuchMethodException {
        // 如果写入器代理没有创建，则创建
        if (messageWriterProxyInstance == null) {
            synchronized (LOCK_OBJECT) {
                if (messageWriterProxyInstance == null) {
                    messageWriterProxyInstance = new MessageWriterProxy(writeStrategy, flushStrategy);
                }
            }
        }
        return messageWriterProxyInstance;
    }

}
