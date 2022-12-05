package com.huangjunyi1993.zeromq.client.config;

import io.netty.channel.ChannelHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 抽象配置类，定义了生成者消费者的公共配置
 * Created by huangjunyi on 2022/8/12.
 */
public abstract class AbstractConfig {

    /**
     * 默认配置
     */
    public AbstractConfig() {
        zkUrl = "localhost:2181";
        timeout = 0L;
    }

    // zk地址
    protected String zkUrl;

    // 处理器
    protected List<ChannelHandler> channelHandlers;

    private long timeout;

    public String getZkUrl() {
        return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    public List<ChannelHandler> getChannelHandlers() {
        return channelHandlers;
    }

    public void addChannelHandler(ChannelHandler channelHandler) {
        if (Objects.isNull(channelHandlers)) {
            channelHandlers = new ArrayList<>();
        }
        channelHandlers.add(channelHandler);
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
