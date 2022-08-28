package com.huangjunyi1993.zeromq.base.exception;

/**
 * 协议包编码异常
 * Created by huangjunyi on 2022/8/13.
 */
public class EncoderException extends AbstractException {
    public EncoderException(String message, Exception e) {
        super(message, e);
    }

    public EncoderException(String message) {
        super(message);
    }
}