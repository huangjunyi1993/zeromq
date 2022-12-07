package com.huangjunyi1993.zeromq.core;

import com.huangjunyi1993.zeromq.base.Context;

/**
 * 拦截器
 * Created by huangjunyi on 2022/8/10.
 */
public interface Interceptor {

    // 该拦截器是否适配该处理器类型
    boolean support(Handler handler);

    // 前置拦截
    void pre(Context context);

    // 后置拦截
    void after(Context context);

    // 拦截器排序
    int order();

}
