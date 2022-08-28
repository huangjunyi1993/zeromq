package com.huangjunyi1993.zeromq.core;

import com.huangjunyi1993.zeromq.base.Context;

/**
 * 处理器
 * Created by huangjunyi on 2022/8/10.
 */
public interface Handler {

    void handle(Context context);

    int messageType();

}
