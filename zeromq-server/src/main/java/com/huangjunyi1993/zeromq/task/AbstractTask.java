package com.huangjunyi1993.zeromq.task;

import java.util.concurrent.locks.LockSupport;

/**
 * 定时任务抽象类
 * Created by huangjunyi on 2022/8/21.
 */
public abstract class AbstractTask implements Runnable {

    protected static final long DEFAULT_INTERVAL = 1000L;

    private long interval;

    public AbstractTask() {
        this(DEFAULT_INTERVAL);
    }

    public AbstractTask(long interval) {
        this.interval = interval > 0 ? interval : DEFAULT_INTERVAL;
    }

    @Override
    public void run() {
        while (true) {
            process();
            LockSupport.parkNanos(interval * 1000 * 1000);
        }
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    protected abstract void process();

}
