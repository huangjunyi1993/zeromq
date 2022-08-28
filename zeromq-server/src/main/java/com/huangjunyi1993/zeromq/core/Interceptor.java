package com.huangjunyi1993.zeromq.core;

import com.huangjunyi1993.zeromq.base.Context;

/**
 * 拦截器
 * Created by huangjunyi on 2022/8/10.
 */
public interface Interceptor {

    boolean support(Handler handler);

    void pre(Context context);

    void after(Context context);

    int order();

}
