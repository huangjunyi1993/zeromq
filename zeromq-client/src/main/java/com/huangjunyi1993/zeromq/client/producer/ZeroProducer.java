package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.config.ProducerConfig;
import com.huangjunyi1993.zeromq.client.remoting.transport.ZeroProducerHandler;

/**
 * 消息生产者
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroProducer extends BaseProducer {

    // 全局配置
    private AbstractConfig config;

    public ZeroProducer(AbstractConfig config) {
        super(config);
        this.config = config;
    }

    /**
     * 创建一个使用默认配置的消息生产者
     * @return
     */
    public static ZeroProducer newDefaultProducer() {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.addChannelHandler(new ZeroProducerHandler());
        return new ZeroProducer(producerConfig);
    }

    /**
     * 发送消息，增加了消息发送重试
     * @param message
     * @return
     */
    @Override
    public boolean sendMessage(Message message) {
        boolean result;
        if (config instanceof ProducerConfig) {
            ProducerConfig producerConfig = (ProducerConfig) config;
            // 发送消息
            result = super.sendMessage(message);
            // 如果发送失败，根据配置的重试次数进行重试
            int retryCount = 0;
            while (!result && retryCount <= producerConfig.getSendRetry()) {
                result = super.sendMessage(message);
                retryCount++;
            }
        } else {
            result = super.sendMessage(message);
        }
        return result;
    }

}
