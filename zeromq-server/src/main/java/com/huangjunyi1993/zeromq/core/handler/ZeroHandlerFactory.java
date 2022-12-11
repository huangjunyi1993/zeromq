package com.huangjunyi1993.zeromq.core.handler;

import com.huangjunyi1993.zeromq.base.exception.HandlerInitException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.HandlerChain;
import com.huangjunyi1993.zeromq.core.HandlerFactory;
import com.huangjunyi1993.zeromq.core.Interceptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 处理器工厂：构建处理器链
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroHandlerFactory implements HandlerFactory {

    // 处理器注册表
    private static final List<Class<? extends Handler>> HANDLER_CLASS_LIST = new ArrayList<>();

    // 所有处理器
    private static final List<Handler> HANDLER_LIST = new ArrayList<>();

    // 拦截器注册表
    private static final List<Class<? extends Interceptor>> INTERCEPTOR_CLASS_LIST = new ArrayList<>();

    // 所有拦截器
    private static final List<Interceptor> INTERCEPTOR_LIST = new ArrayList<>();

    // 处理器缓存表： 消息类型 => 处理器
    private static final Map<Integer, HandlerChain> HANDLER_CHAIN_MAP = new HashMap<>();

    // 工厂单例
    private volatile static ZeroHandlerFactory factory;

    private static final Object LOCK_OBJECT = new Object();

    private ZeroHandlerFactory() {

    }

    @Override
    public Handler build(ZeroProtocol protocol) {
        // 根据消息类型获取处理器
        if (HANDLER_CHAIN_MAP.containsKey(protocol.getMessageType())) {
            return HANDLER_CHAIN_MAP.get(protocol.getMessageType());
        }

        // 处理器不存在 创建 根据消息类型过滤出对应的处理器
        Optional<Handler> handlerOptional = HANDLER_LIST.stream().filter(handler -> handler.messageType() == protocol.getMessageType()).findFirst();
        // 构建处理器链
        ZeroHandlerChain handlerChain = handlerOptional.map(handler -> {
            ZeroHandlerChain zeroHandlerChain = new ZeroHandlerChain();
            zeroHandlerChain.setHandler(handler);
            // 根据消息类型，过滤出匹配的拦截器
            List<Interceptor> interceptors = INTERCEPTOR_LIST.stream().filter(interceptor -> interceptor.support(handler)).collect(Collectors.toList());
            if (interceptors.size() > 0) {
                // 添加到处理器链中
                zeroHandlerChain.setInterceptors(interceptors);
            }
            return zeroHandlerChain;
        }).orElse(null);

        // 缓存处理器到处理器缓存表
        HANDLER_CHAIN_MAP.put(protocol.getMessageType(), handlerChain);
        return handlerChain;
    }

    @Override
    public void regiserInterceptor(Interceptor interceptor) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if (interceptor == null) {
            throw new HandlerInitException("Interceptor is null");
        }
        if (INTERCEPTOR_CLASS_LIST.contains(interceptor.getClass())) {
            throw new HandlerInitException("Interceptor class is already exists");
        }
        INTERCEPTOR_CLASS_LIST.add(interceptor.getClass());
        INTERCEPTOR_LIST.add(interceptor);
    }

    @Override
    public void regiserHandler(Handler handler) throws IllegalAccessException, InstantiationException {
        if (handler == null) {
            throw new HandlerInitException("Handler is null");
        }
        if (HANDLER_CLASS_LIST.contains(handler.getClass())) {
            throw new HandlerInitException("Handler class is already exists");
        }
        HANDLER_CLASS_LIST.add(handler.getClass());
        HANDLER_LIST.add(handler);
    }

    /**
     * 获取处理器工厂单例实体
     * @return
     */
    public static HandlerFactory getInstance() {
        if (factory == null) {
            synchronized (LOCK_OBJECT) {
                if (factory == null) {
                    factory = new ZeroHandlerFactory();
                }
            }
        }
        return factory;
    }
}
