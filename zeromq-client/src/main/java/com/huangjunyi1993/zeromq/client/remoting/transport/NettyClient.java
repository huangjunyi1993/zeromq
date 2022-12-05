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
import java.util.Map;

/**
 * Netty客户端
 * Created by huangjunyi on 2022/8/11.
 */
public class NettyClient {

    // IO通道缓存，服务器信息 => Channel
    private volatile Map<BrokerServerUrl, Channel> CHANNEL_MAP = new HashMap<>();

    // NIO事件循环组，相当于线程池
    private EventLoopGroup eventLoopGroup;

    // 全局配置
    private AbstractConfig config;

    // netty启动引导器
    private Bootstrap bootstrap;

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
    public static NettyClient open(AbstractConfig config) {
        EventLoopGroup eventExecutors = null;
        try {
            eventExecutors = new NioEventLoopGroup();
            Bootstrap bootstrap = new Bootstrap();
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
                                    channelHandler -> socketChannel.pipeline().addLast(channelHandler));
                        }
                    });
            return new NettyClient(eventExecutors, config, bootstrap);
        } catch (Exception e) {
            if (eventExecutors != null) {
                eventExecutors.shutdownGracefully();
            }
            throw new RemotingException("An exception occurred while initializing the Netty client", e);
        }
    }

    /**
     * 根据服务器信息，获取一条IO通道
     * @param brokerServerUrl 服务器信息
     * @return
     */
    public Channel getChannel(BrokerServerUrl brokerServerUrl) {
        Channel channel = null;
        // 如果不存在服务器对应的通道，加双重检测锁，并创建IO通道
        if (!CHANNEL_MAP.containsKey(brokerServerUrl)) {
            synchronized (CHANNEL_MAP) {
                if (!CHANNEL_MAP.containsKey(brokerServerUrl)) {
                    ChannelFuture channelFuture = null;
                    try {
                        // 与对应服务器建立连接
                        channelFuture = bootstrap.connect(new InetSocketAddress(brokerServerUrl.getHost(), brokerServerUrl.getPort())).sync();
                        channel = channelFuture.channel();
                        // 缓存IO通道
                        CHANNEL_MAP.put(brokerServerUrl, channel);
                    } catch (InterruptedException e) {
                        throw new RemotingException("An exception occurred while connect to broker server", e);
                    }
                }
            }
        } else {
            channel = CHANNEL_MAP.get(brokerServerUrl);
        }
        return channel;
    }

    /**
     * 关闭netty客户端
     */
    public void close() {
        eventLoopGroup.shutdownGracefully();
    }

}
