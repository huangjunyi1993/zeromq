package com.huangjunyi1993.zeromq.base.exception;

/**
 * 处理器初始化异常
 * Created by huangjunyi on 2022/8/20.
 */
public class HandlerInitException extends AbstractException {
    public HandlerInitException(String message, Exception e) {
        super(message, e);
    }

    public HandlerInitException(String message) {
        super(message);
    }
}
