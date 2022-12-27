package com.huangjunyi1993.zeromq.client.config;

import io.netty.channel.ChannelHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 抽象配置类，定义了生产者消费者的公共配置
 * Created by huangjunyi on 2022/8/12.
 */
public abstract class AbstractConfig {

    // zk地址
    protected String zkUrl;

    // 处理器
    protected List<ChannelHandler> channelHandlers;

    // 等待时间
    private long timeout;

    // 线程池核心线程数
    private int corePoolSize;

    // 线程池最大线程数 消费者设置无效
    private int maxPoolSize;

    // 非核心线程最大空闲时间
    private long keepAliveTime;

    // 线程池队列容量 消费者设置无效
    private int threadPoolQueueCapacity;

    private int nettyThreads;

    /**
     * 默认配置
     */
    public AbstractConfig() {
        this.zkUrl = "localhost:2181";
        this.timeout = 0L;
        this.corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        this.maxPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        this.nettyThreads = 1;
    }

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

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public int getThreadPoolQueueCapacity() {
        return threadPoolQueueCapacity;
    }

    public void setThreadPoolQueueCapacity(int threadPoolQueueCapacity) {
        this.threadPoolQueueCapacity = threadPoolQueueCapacity;
    }

    public int getNettyThreads() {
        return nettyThreads;
    }

    public void setNettyThreads(int nettyThreads) {
        this.nettyThreads = nettyThreads;
    }
}
