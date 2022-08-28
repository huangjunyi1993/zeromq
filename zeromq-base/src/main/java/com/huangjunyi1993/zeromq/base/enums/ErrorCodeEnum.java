package com.huangjunyi1993.zeromq.base.enums;

/**
 * 服务端错误码枚举
 * Created by huangjunyi on 2022/8/21.
 */
public enum  ErrorCodeEnum {
    SUCCESS(0),
    PROTOCAL_BODY_EMPTY(1),
    DESERIALIZE_FAILED(2),
    ID_NOT_CONSISTENT(3),
    SERIALIZATION_TYPE_NOT_CONSISTENT(4),
    MESSAGE_TOPIC_IS_NULL(5),
    IO_EXCEPTION(6),
    REQUEST_TOPIC_IS_NULL(7),
    BATCH_ILLEGAL(8),
    ACK_TOPIC_IS_NULL(9),
    NO_CONSUMABLE_NEWS(10),
    ;


    private int code;

    ErrorCodeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
