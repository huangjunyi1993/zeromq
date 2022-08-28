package com.huangjunyi1993.zeromq.client.config;

/**
 * 生成者配置类
 * Created by huangjunyi on 2022/8/12.
 */
public class ProducerConfig extends AbstractConfig {

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
