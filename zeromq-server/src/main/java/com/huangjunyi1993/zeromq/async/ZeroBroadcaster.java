package com.huangjunyi1993.zeromq.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 服务端事件广播器
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroBroadcaster implements Broadcaster {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroBroadcaster.class);

    private static final List<Listener> LISTENERS = new CopyOnWriteArrayList<>();

    private volatile static ZeroBroadcaster broadcaster;

    private volatile LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();

    private ZeroBroadcaster() {}

    @Override
    public void registerListener(Listener listener) {
        LISTENERS.add(listener);
    }

    @Override
    public boolean broadcaster(Event event) {
        try {
            events.put(event);
            return true;
        } catch (InterruptedException e) {
            LOGGER.info("An exception occurred in the publishing event: ", e);
            return false;
        }
    }

    public static ZeroBroadcaster getBroadcaster() {
        if (broadcaster == null) {
            synchronized (ZeroBroadcaster.class) {
                if (broadcaster == null) {
                    broadcaster = new ZeroBroadcaster();
                    Thread thread = new Thread(broadcaster);
                    thread.setDaemon(true);
                    thread.start();
                }
            }
        }
        return broadcaster;
    }

    @Override
    public void run() {
        while (true) {
            Event event = events.poll();
            LISTENERS.forEach(listener -> {
                if (listener.support(event)) {
                    listener.onEvent(event);
                }
            });
        }
    }
}
