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
     * @param bytes
     * @return
     */
    public static List<BrokerServerUrl> parseBrokerServerUrls(byte[] bytes) {
        String string = new String(bytes);
        // zk存储的服务器信息：ip:port,ip:port,ip:port
        String[] brokerServerUrls = string.split(",");
        return Arrays.stream(brokerServerUrls).map(brokerServerUrl -> {
            String[] strings = brokerServerUrl.split(":");
            return new BrokerServerUrl(strings[0], Integer.valueOf(strings[1]));
        }).collect(Collectors.toList());
    }

}
