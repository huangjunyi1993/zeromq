package com.huangjunyi1993.zeromq.base.enums;

/**
 * 序列化枚举
 * Created by huangjunyi on 2022/8/13.
 */
public enum SerializationTypeEnum {

    /**
     * JDK native
     */
    JDK_NATIVE_SERIALIZATION(0),

    /**
     * hessian2
     */
    HESSIAN2_SERIALIZATION(1),

    ;

    private final int type;

    SerializationTypeEnum(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
