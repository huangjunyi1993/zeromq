package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.client.config.ConsumerConfig;
import org.junit.Test;

/**
 * 消费者测试类
 * Created by huangjunyi on 2022/8/25.
 */
public class ConsumerTest {

    @Test
    public void test() throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroupId(1);
        consumerConfig.setConsumerId(1);
        consumerConfig.setBatch(100);
        ZeroConsumerBootstrap zeroConsumerBootstrap = new ZeroConsumerBootstrap(consumerConfig);
        zeroConsumerBootstrap.register("hello", PrintlnConsumer.class);
        zeroConsumerBootstrap.register("world", PrintlnConsumer.class);
        if (zeroConsumerBootstrap.start()) {
            Thread.sleep(Integer.MAX_VALUE);
        }

    }

}
