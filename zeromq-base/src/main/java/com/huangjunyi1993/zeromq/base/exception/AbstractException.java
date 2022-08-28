package com.huangjunyi1993.zeromq.base.exception;

/**
 * 抽象异常类
 * Created by huangjunyi on 2022/8/13.
 */
public class AbstractException extends RuntimeException {

    public AbstractException(String message, Exception e) {
        super(message);
        this.initCause(e);
    }

    public AbstractException(String message) {
        super(message);
    }
}