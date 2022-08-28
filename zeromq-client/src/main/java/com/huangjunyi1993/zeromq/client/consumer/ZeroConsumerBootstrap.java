package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.config.ConsumerConfig;

/**
 * 消费者客户端启动器
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroConsumerBootstrap extends BaseConsumerBootStrap {

    public ZeroConsumerBootstrap(AbstractConfig config) {
        super(config);
        this.config = config;
    }

    public static ZeroConsumerBootstrap newDefaultConsumerBootstrap() {
        ConsumerConfig config = new ConsumerConfig();
        ZeroConsumerBootstrap zeroConsumerBootstrap = new ZeroConsumerBootstrap(config);
        return zeroConsumerBootstrap;
    }

}
