package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.ZeroMessage;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_TOPIC;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.HESSIAN2_SERIALIZATION;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * 生产者测试类
 * Created by huangjunyi on 2022/8/24.
 */
public class ProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTest.class);

    @Test
    public void testSyncSend() throws InterruptedException, IOException {
        ZeroProducer zeroProducer = ZeroProducer.newDefaultProducer();
        Serializer jdkSerializer = SerializerFactory.getSerializer(JDK_NATIVE_SERIALIZATION.getType());
        Serializer hessian2Serializer = SerializerFactory.getSerializer(HESSIAN2_SERIALIZATION.getType());
        int i = 0;
        int j = 0;
        int k = 0;

        while (true) {
            ZeroMessage zeroMessage1 = new ZeroMessage();
            ZeroMessage zeroMessage2 = new ZeroMessage();

            initMessage(zeroMessage1, zeroMessage2, jdkSerializer, hessian2Serializer, i++, j++, k++);

            boolean b1 = zeroProducer.sendMessage(zeroMessage1);
            boolean b2 = zeroProducer.sendMessage(zeroMessage2);
            LOGGER.info("send message b1: {} b2: {}", b1, b2);
        }
    }

    @Test
    public void testAsyncSendOneTime() throws InterruptedException, IOException {
        ZeroProducer zeroProducer = ZeroProducer.newDefaultProducer();
        Serializer jdkSerializer = SerializerFactory.getSerializer(JDK_NATIVE_SERIALIZATION.getType());
        Serializer hessian2Serializer = SerializerFactory.getSerializer(HESSIAN2_SERIALIZATION.getType());
        int i = 0;
        int j = 0;
        int k = 0;

        int total = 10000;
        CountDownLatch countDownLatch = new CountDownLatch(total * 2);
        while (k < 10000) {
            ZeroMessage zeroMessage1 = new ZeroMessage();
            ZeroMessage zeroMessage2 = new ZeroMessage();

            initMessage(zeroMessage1, zeroMessage2, jdkSerializer, hessian2Serializer, i++, j++, k++);

            zeroProducer.sendMessageAsync(zeroMessage1, response -> {
                LOGGER.info("received response: {}" + response);
                countDownLatch.countDown();
            });
            zeroProducer.sendMessageAsync(zeroMessage2, response -> {
                LOGGER.info("received response: {}" + response);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
    }

    @Test
    public void testAsyncSendInterval() throws InterruptedException, IOException {
        ZeroProducer zeroProducer = ZeroProducer.newDefaultProducer();
        Serializer jdkSerializer = SerializerFactory.getSerializer(JDK_NATIVE_SERIALIZATION.getType());
        Serializer hessian2Serializer = SerializerFactory.getSerializer(HESSIAN2_SERIALIZATION.getType());
        int i = 0;
        int j = 0;
        int k = 0;

        while (true) {
            ZeroMessage zeroMessage1 = new ZeroMessage();
            ZeroMessage zeroMessage2 = new ZeroMessage();

            initMessage(zeroMessage1, zeroMessage2, jdkSerializer, hessian2Serializer, i++, j++, k++);

            boolean b1 = zeroProducer.sendMessageAsync(zeroMessage1, response -> LOGGER.info("received response: {}" + response));
            boolean b2 = zeroProducer.sendMessageAsync(zeroMessage2, response -> LOGGER.info("received response: {}" + response));
            LOGGER.info("send message b1: {} b2: {}", b1, b2);
            if (k % 100 == 0) LockSupport.parkNanos(2000000000);
        }
    }

    @Test
    public void testAsyncSendNoInterval() throws InterruptedException, IOException {
        ZeroProducer zeroProducer = ZeroProducer.newDefaultProducer();
        Serializer jdkSerializer = SerializerFactory.getSerializer(JDK_NATIVE_SERIALIZATION.getType());
        Serializer hessian2Serializer = SerializerFactory.getSerializer(HESSIAN2_SERIALIZATION.getType());
        int i = 0;
        int j = 0;
        int k = 0;

        while (true) {
            ZeroMessage zeroMessage1 = new ZeroMessage();
            ZeroMessage zeroMessage2 = new ZeroMessage();

            initMessage(zeroMessage1, zeroMessage2, jdkSerializer, hessian2Serializer, i++, j++, k++);

            zeroProducer.sendMessageAsync(zeroMessage1, response -> LOGGER.info("received response: {}" + response));
            zeroProducer.sendMessageAsync(zeroMessage2, response -> LOGGER.info("received response: {}" + response));
        }
    }

    private void initMessage(ZeroMessage zeroMessage1, ZeroMessage zeroMessage2, Serializer jdkSerializer, Serializer hessian2Serializer, int i, int j, int k) throws IOException {
        zeroMessage1.putHead(MESSAGE_HEAD_TOPIC, "hello");
        zeroMessage2.putHead(MESSAGE_HEAD_TOPIC, "world");
        if (k % 2 == 0) {
            zeroMessage1.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, JDK_NATIVE_SERIALIZATION.getType());
            zeroMessage2.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, JDK_NATIVE_SERIALIZATION.getType());
            zeroMessage1.setBody(jdkSerializer.serialize("hello" + i));
            zeroMessage2.setBody(jdkSerializer.serialize("world" + j));
        } else {
            zeroMessage1.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, HESSIAN2_SERIALIZATION.getType());
            zeroMessage2.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, HESSIAN2_SERIALIZATION.getType());
            zeroMessage1.setBody(hessian2Serializer.serialize("hello" + i));
            zeroMessage2.setBody(hessian2Serializer.serialize("world" + j));
        }
    }

}
