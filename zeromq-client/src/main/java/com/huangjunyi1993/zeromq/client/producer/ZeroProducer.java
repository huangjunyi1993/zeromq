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

    private AbstractConfig config;

    public ZeroProducer(AbstractConfig config) {
        super(config);
        this.config = config;
    }

    public static ZeroProducer newDefaultProducer() {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.addChannelHandler(new ZeroProducerHandler());
        return new ZeroProducer(producerConfig);
    }

    @Override
    public boolean sendMessage(Message message) {
        boolean result;
        if (config instanceof ProducerConfig) {
            ProducerConfig producerConfig = (ProducerConfig) config;
            result = super.sendMessage(message);
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
