package com.huangjunyi1993.zeromq.base.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * 消息对象
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroMessage implements Message {

    private static final long serialVersionUID = -2832236042734725280L;

    private Map<String, Object> head;

    private byte[] body;

    public ZeroMessage() {
        this.head = new HashMap<>();
    }

    public ZeroMessage(byte[] body, Map<String, Object> head) {
        this.body = body;
        this.head = head;
    }

    @Override
    public void putHead(String key, Object value) {
        this.head.put(key, value);
    }

    @Override
    public Object getHead(String key) {
        return this.head.get(key);
    }

    @Override
    public byte[] getBody() {
        return this.body;
    }

    @Override
    public void setBody(byte[] body) {
        this.body = body;
    }
}
