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

    private static final List<Class<? extends Handler>> HANDLER_CLASS_LIST = new ArrayList<>();

    private static final List<Handler> HANDLER_LIST = new ArrayList<>();

    private static final List<Class<? extends Interceptor>> INTERCEPTOR_CLASS_LIST = new ArrayList<>();

    private static final List<Interceptor> INTERCEPTOR_LIST = new ArrayList<>();

    private static final Map<Integer, HandlerChain> HANDLER_CHAIN_MAP = new HashMap<>();

    private volatile static ZeroHandlerFactory factory;

    private ZeroHandlerFactory() {

    }

    @Override
    public Handler build(ZeroProtocol protocol) {
        if (HANDLER_CHAIN_MAP.containsKey(protocol.getMessageType())) {
            return HANDLER_CHAIN_MAP.get(protocol.getMessageType());
        }
        Optional<Handler> handlerOptional = HANDLER_LIST.stream().filter(handler -> handler.messageType() == protocol.getMessageType()).findFirst();
        ZeroHandlerChain handlerChain = handlerOptional.map(handler -> {
            ZeroHandlerChain zeroHandlerChain = new ZeroHandlerChain();
            zeroHandlerChain.setHandler(handler);
            List<Interceptor> interceptors = INTERCEPTOR_LIST.stream().filter(interceptor -> interceptor.support(handler)).collect(Collectors.toList());
            if (interceptors.size() > 0) {
                zeroHandlerChain.setInterceptors(interceptors);
            }
            return zeroHandlerChain;
        }).orElse(null);

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

    public static HandlerFactory getInstance() {
        if (factory == null) {
            synchronized (ZeroHandlerFactory.class) {
                if (factory == null) {
                    factory = new ZeroHandlerFactory();
                }
            }
        }
        return factory;
    }
}
