package com.huangjunyi1993.zeromq.base.entity;

import java.io.Serializable;

/**
 * 消息对象接口
 * Created by huangjunyi on 2022/8/10.
 */
public interface Message extends Serializable {

    void putHead(String key, Object value);

    Object getHead(String key);

    byte[] getBody();

    void setBody(byte[] body);

}
