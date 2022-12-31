package com.huangjunyi1993.zeromq.client.remoting.transport;

import com.huangjunyi1993.zeromq.base.exception.RemotingException;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 可复制处理器
 * Created by huangjunyi on 2022/12/31.
 */
public abstract class CloneableChannelHandler<T> extends SimpleChannelInboundHandler<T> implements Cloneable {
    @Override
    public CloneableChannelHandler<T> clone() {
        try {
            return (CloneableChannelHandler<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RemotingException("An exception occurred while clone the channal handler", e);
        }
    }
}
