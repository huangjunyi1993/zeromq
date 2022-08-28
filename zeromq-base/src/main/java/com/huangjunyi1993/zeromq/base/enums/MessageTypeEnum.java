package com.huangjunyi1993.zeromq.base.enums;

/**
 * 协议包包体类型枚举
 * Created by huangjunyi on 2022/8/14.
 */
public enum MessageTypeEnum {
    MESSAGE(0),
    REQUEST(1),
    RESPONSE(2),
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
