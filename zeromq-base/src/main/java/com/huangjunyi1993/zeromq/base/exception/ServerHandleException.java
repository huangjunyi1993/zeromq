package com.huangjunyi1993.zeromq.base.exception;

/**
 * 服务端处理异常
 * Created by huangjunyi on 2022/8/21.
 */
public class ServerHandleException extends AbstractException {
    public ServerHandleException(String message, Exception e) {
        super(message, e);
    }

    public ServerHandleException(String message) {
        super(message);
    }
}
