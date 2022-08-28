package com.huangjunyi1993.zeromq.async;

/**
 * 服务端事件监听器
 * Created by huangjunyi on 2022/8/21.
 */
public interface Listener {

    boolean support(Event event);

    void onEvent(Event event);

}
