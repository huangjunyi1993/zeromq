package com.huangjunyi1993.zeromq.base.entity;

import java.io.Serializable;

/**
 * 客户端请求接口
 * Created by huangjunyi on 2022/8/19.
 */
public interface Request extends Serializable {

    long getId();

    String getTopic();

    int getConsumerGroupId();

    int getConsumerId();

    int getBatch();

    int getSerializationType();

}
