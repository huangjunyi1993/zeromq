package com.huangjunyi1993.zeromq.client.config;

import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * 消费者配置类
 * Created by huangjunyi on 2022/8/14.
 */
public class ConsumerConfig extends AbstractConfig {

    private long waitOnNoMessage;

    private int groupId;

    private int consumerId;

    private int batch;

    private int serializationType;

    public ConsumerConfig() {
        this.waitOnNoMessage = 0;
        this.groupId = 0;
        this.consumerId = 0;
        this.batch = 1;
        serializationType = JDK_NATIVE_SERIALIZATION.getType();
    }

    public long getWaitOnNoMessage() {
        return waitOnNoMessage;
    }

    public void setWaitOnNoMessage(long waitOnNoMessage) {
        this.waitOnNoMessage = waitOnNoMessage;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public int getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(int consumerId) {
        this.consumerId = consumerId;
    }

    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public int getSerializationType() {
        return serializationType;
    }

    public void setSerializationType(int serializationType) {
        this.serializationType = serializationType;
    }
}
