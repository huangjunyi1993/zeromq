package com.huangjunyi1993.zeromq.client.remoting.transport;

import com.huangjunyi1993.zeromq.base.coder.ZeroProtocalDecoder;
import com.huangjunyi1993.zeromq.base.coder.ZeroProtocolEncoder;
import com.huangjunyi1993.zeromq.base.exception.RemotingException;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.config.BrokerServerUrl;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Netty客户端
 * Created by huangjunyi on 2022/8/11.
 */
public class NettyClient {

    // 服务器信息 => NettyClient
    private static final Map<BrokerServerUrl, NettyClient> NETTY_CLIENT_MAP = new HashMap<>();

    // IO通道缓存
    private volatile Channel channelCache;

    // NIO事件循环组，相当于线程池
    private EventLoopGroup eventLoopGroup;

    // 全局配置
    private AbstractConfig config;

    // netty启动引导器
    private Bootstrap bootstrap;

    public static final Object CLIENT_LOCK_OBJ = new Object();

    private NettyClient(EventLoopGroup eventLoopGroup, AbstractConfig config, Bootstrap bootstrap) {
        this.eventLoopGroup = eventLoopGroup;
        this.config = config;
        this.bootstrap = bootstrap;
    }

    /**
     * 开启netty客户端
     * @param config
     * @return
     */
    private static void open(AbstractConfig config, BrokerServerUrl brokerServerUrl) {
        EventLoopGroup eventExecutors = null;
        Bootstrap bootstrap = null;
        NettyClient nettyClient = null;
        try {
            eventExecutors = new NioEventLoopGroup(config.getNettyThreads());
            bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            // 添加协议编码器
                            socketChannel.pipeline().addLast(new ZeroProtocolEncoder());
                            // 添加协议解码器
                            socketChannel.pipeline().addLast(new ZeroProtocalDecoder());
                            // 添加注册到全局配置中的所有处理器
                            config.getChannelHandlers().forEach(
                                    channelHandler -> socketChannel.pipeline().addLast(channelHandler.clone()));
                        }
                    });
            nettyClient = new NettyClient(eventExecutors, config, bootstrap);
        } catch (Exception e) {
            if (eventExecutors != null) {
                eventExecutors.shutdownGracefully();
            }
            throw new RemotingException("An exception occurred while initializing the Netty client", e);
        }

        nettyClient.connect(brokerServerUrl);
        NETTY_CLIENT_MAP.put(brokerServerUrl, nettyClient);
    }

    private void connect(BrokerServerUrl brokerServerUrl) {
        try {
            // 与对应服务器建立连接
            ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(brokerServerUrl.getHost(), brokerServerUrl.getPort())).sync();
            // 缓存IO通道
            channelCache = channelFuture.channel();
        } catch (InterruptedException e) {
            throw new RemotingException(String.format("An exception occurred while connect to server: %s:%s", brokerServerUrl.getHost(), brokerServerUrl.getPort()));
        }
    }

    /**
     * 根据服务器信息，获取IO通道
     *
     * @param config
     * @param brokerServerUrl 服务器信息
     * @return
     */
    public static Channel getChannel(AbstractConfig config, BrokerServerUrl brokerServerUrl) {
        Channel channel = null;
        // 如果不存在服务器对应的客户端，加双重检测锁，并开启客户端
        if (!NETTY_CLIENT_MAP.containsKey(brokerServerUrl)) {
            synchronized (CLIENT_LOCK_OBJ) {
                if (!NETTY_CLIENT_MAP.containsKey(brokerServerUrl)) {
                    NettyClient.open(config, brokerServerUrl);
                    channel = NETTY_CLIENT_MAP.get(brokerServerUrl).channelCache;
                }
            }
        } else {
            NettyClient nettyClient = NETTY_CLIENT_MAP.get(brokerServerUrl);
            channel = nettyClient.channelCache;
            if (channel == null) {
                synchronized (nettyClient) {
                    channel = nettyClient.channelCache;
                    if (channel == null) {
                        nettyClient.connect(brokerServerUrl);
                        channel = nettyClient.channelCache;
                    }
                }
            }
        }
        return channel;
    }

    /**
     * 关闭netty客户端
     */
    private void close() {
        this.channelCache.close();
        eventLoopGroup.shutdownGracefully();
        BrokerServerUrl selfBrokerServerUrl = null;
        for (Map.Entry<BrokerServerUrl, NettyClient> entry : NETTY_CLIENT_MAP.entrySet()) {
            if (entry.getValue() == this) {
                selfBrokerServerUrl = entry.getKey();
            }
        }
        NETTY_CLIENT_MAP.remove(selfBrokerServerUrl);
    }

    public static void shutDown(List<BrokerServerUrl> downBrokerServerUrlList) {
        downBrokerServerUrlList.forEach(brokerServerUrl -> {
            NettyClient nettyClient = NETTY_CLIENT_MAP.get(brokerServerUrl);
            if (nettyClient != null) {
                nettyClient.close();
            }
        });
    }
}
