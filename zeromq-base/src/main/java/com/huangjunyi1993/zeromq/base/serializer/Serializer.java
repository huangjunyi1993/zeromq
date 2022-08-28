package com.huangjunyi1993.zeromq.base.serializer;

import java.io.IOException;

/**
 * 序列化器接口
 * Created by huangjunyi on 2022/8/13.
 */
public interface Serializer {

    <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException;

    <T> byte[] serialize(T t) throws IOException;

    int getType();

}
