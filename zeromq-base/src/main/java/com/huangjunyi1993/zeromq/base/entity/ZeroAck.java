package com.huangjunyi1993.zeromq.base.entity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.ACK_SUCCESS;

/**
 * ACK（消费者消费成功消息后返回ACK）
 * Created by huangjunyi on 2022/8/19.
 */
public class ZeroAck implements Ack {

    private static final long serialVersionUID = -6734648248126286782L;
    private long offset;
    private String topic;
    private int consumerGroupId;
    private int consumerId;
    private int serializationType;
    private int success;

    public ZeroAck(long offset, String topic, int consumerGroupId, int consumerId, int serializationType, int success) {
        this.offset = offset;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.consumerId = consumerId;
        this.serializationType = serializationType;
        this.success = success;
    }

    public static ZeroAck success(long offset, String topic, int consumerGroupId, int consumerId, int serializationType) {
        return new ZeroAck(offset, topic, consumerGroupId, consumerId, serializationType, ACK_SUCCESS);
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public String topic() {
        return this.topic;
    }

    @Override
    public int consumerGroupId() {
        return this.consumerGroupId;
    }

    @Override
    public int consumerId() {
        return this.consumerId;
    }

    @Override
    public int success() {
        return 0;
    }

    public int getSerializationType() {
        return serializationType;
    }

    @Override
    public String toString() {
        return "ZeroAck{" +
                "offset=" + offset +
                ", topic='" + topic + '\'' +
                ", consumerGroupId=" + consumerGroupId +
                ", consumerId=" + consumerId +
                ", serializationType=" + serializationType +
                ", success=" + success +
                '}';
    }

}
