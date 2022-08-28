package com.huangjunyi1993.zeromq.core.handler;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.exception.HandlerInitException;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.HandlerChain;
import com.huangjunyi1993.zeromq.core.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

/**
 * 处理器链
 * Created by huangjunyi on 2022/8/20.
 */
public class ZeroHandlerChain implements HandlerChain {

    private Handler handler;

    private List<Interceptor> interceptors;

    @Override
    public void handle(Context context) {

        interceptors.forEach(interceptor -> interceptor.pre(context));

        handler.handle(context);

        interceptors.forEach(interceptor -> interceptor.after(context));

    }

    @Override
    public int messageType() {
        return this.handler.messageType();
    }

    @Override
    public void setHandler(Handler handler) {
        if (handler == null) {
            throw new HandlerInitException("handler is null");
        }
        this.handler = handler;
    }

    @Override
    public void setInterceptors(List<Interceptor> interceptors) {
        if (interceptors == null || interceptors.size() == 0) {
            throw new HandlerInitException("interceptors is empty");
        }
        this.interceptors = interceptors;
        this.interceptors.sort(Comparator.comparing(Interceptor::order));
    }
}
