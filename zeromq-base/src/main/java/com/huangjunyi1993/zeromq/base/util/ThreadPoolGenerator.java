package com.huangjunyi1993.zeromq.base.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池生成器
 * Created by huangjunyi on 2022/12/18.
 */
public class ThreadPoolGenerator {

    private static AtomicInteger threadSequenceNumberGenerator = new AtomicInteger(1);

    public static ExecutorService newThreadPool(int corePoolSize, int maxPoolSize,
                                                long keepAliveTime, int threadPoolQueueCapacity,
                                                String threadName) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(threadPoolQueueCapacity),
                r -> new Thread(r, threadName + "-" + threadSequenceNumberGenerator.getAndIncrement()),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newThreadPool(int corePoolSize, int maxPoolSize,
                                                int threadPoolQueueCapacity,
                                                String threadName) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(threadPoolQueueCapacity),
                r -> new Thread(r, threadName + "-" + threadSequenceNumberGenerator.getAndIncrement()),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newThreadPool(int corePoolSize, int maxPoolSize,
                                                long keepAliveTime,
                                                String threadName) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> new Thread(r, threadName + "-" + threadSequenceNumberGenerator.getAndIncrement()),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newThreadPool(int corePoolSize, int maxPoolSize, String threadName) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> new Thread(r, threadName + "-" + threadSequenceNumberGenerator.getAndIncrement()),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newThreadPoolDynamic(int corePoolSize, int maxPoolSize,
                                                       long keepAliveTime, int threadPoolQueueCapacity,
                                                       String threadName) {
        ExecutorService executorService;
        if (keepAliveTime != 0L && threadPoolQueueCapacity != 0) {
            executorService = newThreadPool(corePoolSize, maxPoolSize, keepAliveTime, threadPoolQueueCapacity, threadName);
        } else if (keepAliveTime != 0L) {
            executorService = newThreadPool(corePoolSize, maxPoolSize, keepAliveTime, threadName);
        } else if (threadPoolQueueCapacity != 0) {
            executorService = newThreadPool(corePoolSize, maxPoolSize, threadPoolQueueCapacity, threadName);
        } else {
            executorService = newThreadPool(corePoolSize, maxPoolSize, threadName);
        }
        return executorService;
    }

}
