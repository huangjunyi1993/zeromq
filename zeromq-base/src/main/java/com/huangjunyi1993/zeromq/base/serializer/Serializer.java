package com.huangjunyi1993.zeromq.base.serializer;

import java.io.IOException;

/**
 * 序列化器接口
 * Created by huangjunyi on 2022/8/13.
 */
public interface Serializer {

    /**
     * 发序列化
     */
    <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException;

    /**
     * 序列化
     */
    <T> byte[] serialize(T t) throws IOException;

    /**
     * 序列化类型
     */
    int getType();

}
