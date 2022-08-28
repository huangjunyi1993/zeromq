package com.huangjunyi1993.zeromq.base.exception;

/**
 * 序列化异常
 * Created by huangjunyi on 2022/8/13.
 */
public class SerializerException extends AbstractException {
    public SerializerException(String message, Exception e) {
        super(message, e);
    }

    public SerializerException(String message) {
        super(message);
    }
}
