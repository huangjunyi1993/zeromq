package com.huangjunyi1993.zeromq.client.util;

import com.huangjunyi1993.zeromq.client.config.BrokerServerUrl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 客户端工具类
 * Created by huangjunyi on 2022/8/14.
 */
public class ClientUtil {

    /**
     * 解析获取服务端ip端口信息
     * @param brokers
     * @return
     */
    public static List<BrokerServerUrl> parseBrokerServerUrls(List<String> brokers) {
        // zk的服务器信息：/zero/brokers/{ip}:{port}
        return brokers.stream().map(brokerServerUrl -> {
            String[] strings = brokerServerUrl.split(":");
            return new BrokerServerUrl(strings[0], Integer.valueOf(strings[1]));
        }).collect(Collectors.toList());
    }

}
