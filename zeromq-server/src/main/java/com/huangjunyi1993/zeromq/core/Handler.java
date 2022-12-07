package com.huangjunyi1993.zeromq.core;

import com.huangjunyi1993.zeromq.base.Context;

/**
 * 处理器
 * Created by huangjunyi on 2022/8/10.
 */
public interface Handler {

    // 处理逻辑
    void handle(Context context);

    // 该处理器适配的消息类型
    int messageType();

}
