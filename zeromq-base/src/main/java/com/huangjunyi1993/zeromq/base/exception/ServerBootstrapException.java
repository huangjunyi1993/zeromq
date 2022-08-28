package com.huangjunyi1993.zeromq.base.exception;

/**
 * 服务端启动异常
 * Created by huangjunyi on 2022/8/20.
 */
public class ServerBootstrapException extends AbstractException {
    public ServerBootstrapException(String message, Exception e) {
        super(message, e);
    }

    public ServerBootstrapException(String message) {
        super(message);
    }
}
