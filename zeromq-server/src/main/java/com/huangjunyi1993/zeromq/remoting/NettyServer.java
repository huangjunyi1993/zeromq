package com.huangjunyi1993.zeromq.remoting;

import com.huangjunyi1993.zeromq.base.coder.ZeroProtocalDecoder;
import com.huangjunyi1993.zeromq.base.coder.ZeroProtocolEncoder;
import com.huangjunyi1993.zeromq.base.exception.RemotingException;
import com.huangjunyi1993.zeromq.base.exception.ServerBootstrapException;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty服务端
 * Created by huangjunyi on 2022/8/20.
 */
public class NettyServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    public static void open() {
        NioEventLoopGroup bossGroup = null;
        NioEventLoopGroup workerGroup = null;
        try {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            // 协议编码器
                            socketChannel.pipeline().addLast(new ZeroProtocolEncoder());
                            // 协议解码器
                            socketChannel.pipeline().addLast(new ZeroProtocalDecoder());
                            // 服务端处理器
                            socketChannel.pipeline().addLast(new ZeroServerHandler());
                        }
                    });
            serverBootstrap.bind(GlobalConfiguration.get().getPort()).sync();
        } catch (Exception e) {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            LOGGER.info("netty server open happen error: ", e.getMessage());
            throw new ServerBootstrapException("netty server open happen error: ", e);
        }
    }

}
