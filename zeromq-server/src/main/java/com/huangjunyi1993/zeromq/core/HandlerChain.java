package com.huangjunyi1993.zeromq.core;

import java.util.List;

/**
 * 处理器链
 * Created by huangjunyi on 2022/8/10.
 */
public interface HandlerChain extends Handler {

    void setHandler(Handler handler);

    void setInterceptors(List<Interceptor> interceptors);

}
