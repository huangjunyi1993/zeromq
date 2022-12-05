package com.huangjunyi1993.zeromq.client.config;

/**
 * 生产者配置类
 * Created by huangjunyi on 2022/8/12.
 */
public class ProducerConfig extends AbstractConfig {

    /**
     * 默认配置
     */
    public ProducerConfig() {
        this.sendRetry = 0;
    }

    private int sendRetry;

    public int getSendRetry() {
        return sendRetry;
    }

    public void setSendRetry(int sendRetry) {
        this.sendRetry = sendRetry;
    }

}
