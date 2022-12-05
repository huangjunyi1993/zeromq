package com.huangjunyi1993.zeromq.client.config;

/**
 * 服务端ip端口信息
 * Created by huangjunyi on 2022/8/14.
 */
public class BrokerServerUrl {

    // ip或域名
    private String host;

    // 端口
    private int port;

    public BrokerServerUrl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerServerUrl that = (BrokerServerUrl) o;

        if (port != that.port) return false;
        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return this.getHost() + ":" + this.getPort();
    }
}
