package com.huangjunyi1993.zeromq.client.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局id生成器
 * Created by huangjunyi on 2022/8/19.
 */
public class GlobalIdGenerator {

    private static AtomicLong atomicLong = new AtomicLong();

    private GlobalIdGenerator() {

    }

    public static long nextId() {
        return atomicLong.getAndIncrement();
    }

}
