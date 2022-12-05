package com.huangjunyi1993.zeromq.base.enums;

/**
 * 协议包包体类型枚举
 * Created by huangjunyi on 2022/8/14.
 */
public enum MessageTypeEnum {

    // 消息发送
    MESSAGE(0),

    // 消息请求
    REQUEST(1),

    // 服务端响应，
    RESPONSE(2),

    // ACK
    ACK(3)
    ;

    private final int type;

    MessageTypeEnum(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
