package com.huangjunyi1993.zeromq.client.consumer;

import com.huangjunyi1993.zeromq.client.config.ConsumerConfig;
import org.junit.Test;

/**
 * 消费者测试类
 * Created by huangjunyi on 2022/8/25.
 */
public class ConsumerTest {

    @Test
    public void testConsumer1() throws Exception {
        start(1, 1);
    }

    @Test
    public void testConsumer2() throws Exception {
        start(1, 2);
    }

    @Test
    public void testConsumer3() throws Exception {
        start(1, 3);
    }

    private void start(int groupId, int consumerId) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroupId(groupId);
        consumerConfig.setConsumerId(consumerId);
        consumerConfig.setBatch(100);
        consumerConfig.setCorePoolSize(1);
        ZeroConsumerBootstrap zeroConsumerBootstrap = new ZeroConsumerBootstrap(consumerConfig);
        zeroConsumerBootstrap.register("hello", PrintlnConsumer.class);
        zeroConsumerBootstrap.register("world", PrintlnConsumer.class);
        if (zeroConsumerBootstrap.start()) {
            Thread.sleep(Integer.MAX_VALUE);
        }
    }

}
