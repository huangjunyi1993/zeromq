package com.huangjunyi1993.zeromq.async;

/**
 * Created by huangjunyi on 2022/8/21.
 */
public interface Broadcaster extends Runnable {

    void registerListener(Listener listener);

    boolean broadcaster(Event event);

}
