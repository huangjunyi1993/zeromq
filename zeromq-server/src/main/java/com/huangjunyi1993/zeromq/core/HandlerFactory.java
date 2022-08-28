package com.huangjunyi1993.zeromq.core;

import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;

import java.lang.reflect.InvocationTargetException;

/**
 * 处理器工作
 * Created by huangjunyi on 2022/8/20.
 */
public interface HandlerFactory {

    /**
     * 构建处理器
     * @param protocol
     * @return
     */
    Handler build(ZeroProtocol protocol);

    /**
     * 注册拦截器
     * @param interceptor
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    void regiserInterceptor(Interceptor interceptor) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException;

    /**
     * 注册处理器
     * @param handler
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    void regiserHandler(Handler handler) throws IllegalAccessException, InstantiationException;

}
