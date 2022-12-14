package com.huangjunyi1993.zeromq.base.protocol;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 协议报文对象，客户端与服务端直接传递接收的是协议报文对象
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroProtocol implements Serializable{

    private static final long serialVersionUID = -2157682875468449249L;

    // 报文长度
    private int len;

    // 序列化类型
    private int serializationType;

    // 消息类型（消息发送，ACK，服务端响应，消息请求）
    private int messageType;

    // 消息id
    private long id;

    // 协议报文体
    private byte[] body;

    public ZeroProtocol(int len, int serializationType, int messageType, long id, byte[] body) {
        this.len = len;
        this.serializationType = serializationType;
        this.messageType = messageType;
        this.id = id;
        this.body = body;
    }

    public int getLen() {
        return len;
    }

    public int getSerializationType() {
        return serializationType;
    }

    public int getMessageType() {
        return messageType;
    }

    public byte[] getBody() {
        return body;
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "ZeroProtocol{" +
                "len=" + len +
                ", serializationType=" + serializationType +
                ", messageType=" + messageType +
                ", id=" + id +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
