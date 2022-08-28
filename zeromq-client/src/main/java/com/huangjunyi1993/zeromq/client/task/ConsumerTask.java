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

    private volatile boolean shutdown;

    private Channel channel;

    private BrokerServerUrl brokerServerUrl;

    private String topic;

    private ConsumerConfig config;

    public ConsumerTask(Channel channel, BrokerServerUrl brokerServerUrl, String topic, ConsumerConfig config) {
        this.channel = channel;
        this.brokerServerUrl = brokerServerUrl;
        this.topic = topic;
        this.config = config;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    @Override
    public void run() {
        while (!shutdown) {
            Request request = new ZeroRequest((int) GlobalIdGenerator.nextId(), this.topic, this.config.getGroupId(), this.config.getConsumerId(), this.config.getBatch(), this.config.getSerializationType());
            try {
                Serializer serializer = SerializerFactory.getSerializer(this.config.getSerializationType());
                byte[] bytes = serializer.serialize(request);
                ZeroProtocol protocol = new ZeroProtocol(bytes.length, this.config.getSerializationType(), REQUEST.getType(), request.getId(), bytes);
                ZeroFuture zeroFuture = ZeroFuture.newProducerFuture(request);
                channel.writeAndFlush(protocol);
                if (config.getTimeout() <= 0) {
                    zeroFuture.get();
                } else {
                    try {
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
