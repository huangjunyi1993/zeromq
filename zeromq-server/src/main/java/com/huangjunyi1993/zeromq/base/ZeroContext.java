package com.huangjunyi1993.zeromq.base;

import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 上下文对象，在处理器链中传递
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroContext implements Context {

    private Map<String, Object> variables = new ConcurrentHashMap<>();

    private ZeroProtocol protocol;

    @Override
    public ZeroProtocol getProtocal() {
        return this.protocol;
    }

    @Override
    public void setProtocal(ZeroProtocol protocal) {
        this.protocol = protocal;
    }

    @Override
    public Map<String, Object> variables() {
        return variables;
    }

    @Override
    public void setVariable(String key, Object value) {
        variables.put(key, value);
    }

    @Override
    public Object getVariable(String key) {
        return variables.get(key);
    }

    @Override
    public String toString() {
        return "ZeroContext{" +
                "variables=" + variables +
                ", protocol=" + protocol +
                '}';
    }
}
