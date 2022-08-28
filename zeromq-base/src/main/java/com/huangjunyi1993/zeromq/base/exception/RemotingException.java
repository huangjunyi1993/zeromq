package com.huangjunyi1993.zeromq.base.exception;

/**
 * 远程调用异常
 * Created by huangjunyi on 2022/8/14.
 */
public class RemotingException extends AbstractException {
    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Exception e) {
        super(message, e);
    }
}
