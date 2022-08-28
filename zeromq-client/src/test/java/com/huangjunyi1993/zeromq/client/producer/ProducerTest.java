package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.ZeroMessage;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import org.junit.Test;

import java.io.IOException;

import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_TOPIC;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.HESSIAN2_SERIALIZATION;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * 生成者测试类
 * Created by huangjunyi on 2022/8/24.
 */
public class ProducerTest {

    @Test
    public void test() throws InterruptedException, IOException {
        ZeroProducer zeroProducer = ZeroProducer.newDefaultProducer();
        Serializer jdkSerializer = SerializerFactory.getSerializer(JDK_NATIVE_SERIALIZATION.getType());
        Serializer hessian2Serializer = SerializerFactory.getSerializer(HESSIAN2_SERIALIZATION.getType());
        int i = 0;
        int j = 0;
        int k = 0;
        while (true) {
            ZeroMessage zeroMessage1 = new ZeroMessage();
            zeroMessage1.putHead(MESSAGE_HEAD_TOPIC, "hello");

            ZeroMessage zeroMessage2 = new ZeroMessage();
            zeroMessage2.putHead(MESSAGE_HEAD_TOPIC, "world");

            if (k % 2 == 0) {
                zeroMessage1.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, JDK_NATIVE_SERIALIZATION.getType());
                zeroMessage2.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, JDK_NATIVE_SERIALIZATION.getType());
                zeroMessage1.setBody(jdkSerializer.serialize("hello" + i++));
                zeroMessage2.setBody(jdkSerializer.serialize("world" + j++));
            } else {
                zeroMessage1.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, HESSIAN2_SERIALIZATION.getType());
                zeroMessage2.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, HESSIAN2_SERIALIZATION.getType());
                zeroMessage1.setBody(hessian2Serializer.serialize("hello" + i++));
                zeroMessage2.setBody(hessian2Serializer.serialize("world" + j++));
            }

            zeroProducer.sendMessage(zeroMessage1);
            zeroProducer.sendMessage(zeroMessage2);

            k++;
        }
    }

}
