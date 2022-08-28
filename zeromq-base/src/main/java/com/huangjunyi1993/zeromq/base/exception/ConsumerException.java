package com.huangjunyi1993.zeromq.base.exception;

/**
 * 消费者异常
 * Created by huangjunyi on 2022/8/14.
 */
public class ConsumerException extends AbstractException {
    public ConsumerException(String message, Exception e) {
        super(message, e);
    }

    public ConsumerException(String message) {
        super(message);
    }
}
