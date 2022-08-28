package com.huangjunyi1993.zeromq.base.exception;

/**
 * 生产者异常
 * Created by huangjunyi on 2022/8/12.
 */
public class ProducerException extends AbstractException {
    public ProducerException(String message) {
        super(message);
    }

    public ProducerException(String message, Exception e) {
        super(message, e);
    }
}
