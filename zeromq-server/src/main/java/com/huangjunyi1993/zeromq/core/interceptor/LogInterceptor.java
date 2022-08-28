package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 拦截器：参数打印
 * Created by huangjunyi on 2022/8/20.
 */
public class LogInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogInterceptor.class);

    @Override
    public boolean support(Handler handler) {
        return true;
    }

    @Override
    public void pre(Context context) {
        LOGGER.info("handler the client request: context={}", context.toString());
    }

    @Override
    public void after(Context context) {
        LOGGER.info("handler the client request over: context={}", context.toString());
    }

    @Override
    public int order() {
        return 0;
    }
}
