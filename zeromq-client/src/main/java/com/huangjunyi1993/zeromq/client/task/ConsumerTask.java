package com.huangjunyi1993.zeromq.client.task;

import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.entity.ZeroRequest;
import com.huangjunyi1993.zeromq.base.exception.ConsumerException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.client.config.BrokerServerUrl;
import com.huangjunyi1993.zeromq.client.config.ConsumerConfig;
import com.huangjunyi1993.zeromq.client.remoting.support.ZeroFuture;
import com.huangjunyi1993.zeromq.client.util.GlobalIdGenerator;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.REQUEST;

/**
 * 消费者定时任务：定时向指定的服务器发送请求
 * Created by huangjunyi on 2022/8/17.
 */
public class ConsumerTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTask.class);

    // 任务停止标志
    private volatile boolean shutdown;

    // 通道（与服务端建立的连接）
    private Channel channel;

    // 目标服务器
    private BrokerServerUrl brokerServerUrl;

    // 当前任务订阅的topic
    private String topic;

    // 全局配置
    private ConsumerConfig config;

    public ConsumerTask(Channel channel, BrokerServerUrl brokerServerUrl, String topic, ConsumerConfig config) {
        this.channel = channel;
        this.brokerServerUrl = brokerServerUrl;
        this.topic = topic;
        this.config = config;
    }

    /**
     * 终止任务
     */
    public void shutdown() {
        this.shutdown = true;
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    @Override
    public void run() {
        // 如果不是终止，循环运行
        while (!shutdown) {
            // 生成请求对象
            Request request = new ZeroRequest((int) GlobalIdGenerator.nextId(), this.topic, this.config.getGroupId(), this.config.getConsumerId(), this.config.getBatch(), this.config.getSerializationType());
            try {

                // 根据配置的序列化类型，从序列化工厂获取序列化器，进行序列化
                Serializer serializer = SerializerFactory.getSerializer(this.config.getSerializationType());
                byte[] bytes = serializer.serialize(request);
                // 包装为协议报文对象
                ZeroProtocol protocol = new ZeroProtocol(bytes.length, this.config.getSerializationType(), REQUEST.getType(), request.getId(), bytes);

                // 生成回执，并会缓存到全局回执表，request对象缓存到全局请求信息表（回送ack时获取topic和groupId信息）
                ZeroFuture zeroFuture = ZeroFuture.newProducerFuture(request);
                // 发送数据到通道
                channel.writeAndFlush(protocol);
                if (config.getTimeout() <= 0) {
                    // 没有配置等待时间，阻塞等待
                    zeroFuture.get();
                } else {
                    try {
                        // 等待消息，超时返回
                        zeroFuture.get(config.getTimeout(), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        LOGGER.info("request server timeout");
                    }
                }
            } catch (Exception e) {
                shutdown();
                throw new ConsumerException("consumer task running happen error", e);
            }
        }
    }

    public void start() {
        new Thread(this).start();
    }
}
