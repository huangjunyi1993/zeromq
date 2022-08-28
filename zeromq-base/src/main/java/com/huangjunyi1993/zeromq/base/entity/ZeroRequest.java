package com.huangjunyi1993.zeromq.base.entity;

/**
 * 客户端请求（消费者拉取消息用）
 * Created by huangjunyi on 2022/8/19.
 */
public class ZeroRequest implements Request {

    private static final long serialVersionUID = 310723464624101022L;
    private long id;
    private String topic;
    private int consumerGroupId;
    private int consumerId;
    private int batch;
    private int serializationType;

    public ZeroRequest(long id, String topic, int consumerGroupId, int consumerId, int batch, int serializationType) {
        this.id = id;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.consumerId = consumerId;
        this.batch = batch;
        this.serializationType = serializationType;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public String getTopic() {
        return this.topic;
    }

    @Override
    public int getConsumerGroupId() {
        return this.consumerGroupId;
    }

    @Override
    public int getConsumerId() {
        return this.consumerId;
    }

    @Override
    public int getBatch() {
        return this.batch;
    }

    @Override
    public int getSerializationType() {
        return this.serializationType;
    }

    @Override
    public String toString() {
        return "ZeroRequest{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", consumerGroupId=" + consumerGroupId +
                ", consumerId=" + consumerId +
                ", batch=" + batch +
                ", serializationType=" + serializationType +
                '}';
    }
}
