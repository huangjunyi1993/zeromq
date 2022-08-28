package com.huangjunyi1993.zeromq.base.entity;

import java.io.Serializable;

/**
 * ack接口
 * Created by huangjunyi on 2022/8/19.
 */
public interface Ack extends Serializable {

    long offset();

    String topic();

    int consumerGroupId();

    int consumerId();

    int success();

}
