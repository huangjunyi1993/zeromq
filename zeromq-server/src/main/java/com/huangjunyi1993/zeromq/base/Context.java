package com.huangjunyi1993.zeromq.base;

import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;

import java.util.Map;

/**
 * 上下文对象，在处理器链中传递
 * Created by huangjunyi on 2022/8/10.
 */
public interface Context {

    ZeroProtocol getProtocal();

    void setProtocal(ZeroProtocol protocal);

    Map<String, Object> variables();

    void setVariable(String key, Object value);

    Object getVariable(String key);

}
