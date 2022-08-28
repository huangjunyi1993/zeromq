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

    private volatile Map<BrokerServerUrl, Channel> CHANNEL_MAP = new HashMap<>();

    private EventLoopGroup eventLoopGroup;

    private AbstractConfig config;

    private Bootstrap bootstrap;

    private NettyClient(EventLoopGroup eventLoopGroup, AbstractConfig config, Bootstrap bootstrap) {
        this.eventLoopGroup = eventLoopGroup;
        this.config = config;
        this.bootstrap = bootstrap;
    }

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
                            socketChannel.pipeline().addLast(new ZeroProtocolEncoder());
                            socketChannel.pipeline().addLast(new ZeroProtocalDecoder());
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

    public Channel getChannel(BrokerServerUrl brokerServerUrl) {
        Channel channel = null;
        if (!CHANNEL_MAP.containsKey(brokerServerUrl)) {
            synchronized (CHANNEL_MAP) {
                if (!CHANNEL_MAP.containsKey(brokerServerUrl)) {
                    ChannelFuture channelFuture = null;
                    try {
                        channelFuture = bootstrap.connect(new InetSocketAddress(brokerServerUrl.getHost(), brokerServerUrl.getPort())).sync();
                        channel = channelFuture.channel();
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

    public void close() {
        eventLoopGroup.shutdownGracefully();
    }

}
