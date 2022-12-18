package com.huangjunyi1993.zeromq.config;

import com.huangjunyi1993.zeromq.base.util.ThreadPoolGenerator;
import com.huangjunyi1993.zeromq.core.writer.MessageWriterProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.ServerConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.GlobalConfigurablePropertyConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.ServerConstant.DEFAULT_PORT;

/**
 * 服务端全局配置类
 * Created by huangjunyi on 2022/8/20.
 */
public class GlobalConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalConfiguration.class);

    // 服务器端口
    private int port;

    // zk集群地址
    private String zkUrl;

    // 消息日志存储路径
    private String logPath;

    // 消息索引存储路径
    private String indexPath;

    // 消费偏移量信息存储路径
    private String consumerOffsetPath;

    // 单个日志大小，默认1G
    private long maxLogFileSize = 1024 * 1024 * 1024L;

    // 单索引文件索引条数
    private long indexFileCapacity = 10000L;

    // 写入策略
    private static String writeStrategy;

    // 刷盘策略
    private static String flushStrategy;

    // 写请求线程池核心线程数
    private int writeCorePoolSize = Runtime.getRuntime().availableProcessors() * 2;

    // 写请求线程池最大线程数
    private int writeMaxPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    // 写请求非核心线程最大空闲时间
    private long writeKeepAliveTime;

    // 写请求线程池队列容量
    private int writeThreadPoolQueueCapacity;

    // 读请求线程池核心线程数
    private int readCorePoolSize = Runtime.getRuntime().availableProcessors() * 2;

    // 读请求线程池最大线程数
    private int readMaxPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    // 读请求非核心线程最大空闲时间
    private long readKeepAliveTime;

    // 读请求线程池队列容量
    private int readThreadPoolQueueCapacity;

    // netty连接处理线程数
    private int nettyBossGroupThreads = 1;

    // netty工作线程数
    private int nettyWorkerGroupThreads = 1;

    private long consumerOffsetSyncInterval = -1L;

    private ExecutorService writeExecutorService;

    private ExecutorService readExecutorService;

    // 单例
    private volatile static GlobalConfiguration globalConfiguration;

    private GlobalConfiguration(int port, String zkUrl, String logPath, String indexPath, String consumerOffsetPath){
        this.port = port;
        this.zkUrl = zkUrl;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.consumerOffsetPath = consumerOffsetPath;
    }

    static {
        writeStrategy = DEFAULT_WRITE_STRATEGY;
        flushStrategy = FLUSH_STRATEGY_ASYNC;
    }

    /**
     * 服务端全局配置初始化
     * @param properties 配置信息
     * @return
     */
    public static GlobalConfiguration init(Properties properties) {
        if (globalConfiguration == null) {
            synchronized (GlobalConfiguration.class) {
                if (globalConfiguration == null) {
                    if (properties == null) {
                        // 没有指定配置文件，创建默认全局配置对象
                        globalConfiguration = new GlobalConfiguration(DEFAULT_PORT, DEFAULT_ZK_URL, getDefaultLogPath(), getDefaultIndexPath(), getDefaultConsumerOffsetPath());
                    } else {
                        // 指定了配置文件
                        int port = DEFAULT_PORT;
                        String zkurl = DEFAULT_ZK_URL;
                        String logPath = properties.getProperty(LOG_PATH);
                        String consumerOffsetPath = properties.getProperty(CONSUMER_OFFSET_PATH);
                        String indexPath = properties.getProperty(INDEX_PATH);
                        String writeStrategy = properties.getProperty(WRITE_STRATEGY);
                        String flushStrategy = properties.getProperty(FLUSH_STRATEGY);
                        // 端口
                        if (properties.getProperty(PORT) != null) {
                            port = Integer.valueOf(properties.getProperty(PORT));
                        }
                        // zk地址
                        if (properties.getProperty(ZK_URL) != null) {
                            zkurl = properties.getProperty(ZK_URL);
                        }
                        // 消息日志存储路径
                        if (logPath == null) {
                            logPath = getDefaultLogPath();
                        }
                        // 消息索引存储路径
                        if (indexPath == null) {
                            indexPath = getDefaultIndexPath();
                        }
                        // 消费偏移量文件存储路径
                        if (consumerOffsetPath == null) {
                            consumerOffsetPath = getDefaultConsumerOffsetPath();
                        }
                        // 创建全局配置对象
                        globalConfiguration = new GlobalConfiguration(port, zkurl, logPath, indexPath, consumerOffsetPath);

                        if (writeStrategy != null && !"".equals(writeStrategy)) {
                            GlobalConfiguration.writeStrategy = writeStrategy;
                        }
                        if (flushStrategy != null && !"".equals(flushStrategy)) {
                            GlobalConfiguration.flushStrategy = flushStrategy;
                        }

                        // 最大日志大小
                        globalConfiguration.setLongProperty(properties, MAX_LOG_FILE_SIZE, maxLogFileSize -> globalConfiguration.setMaxLogFileSize(maxLogFileSize));

                        // 单索引文件索引条数
                        globalConfiguration.setLongProperty(properties, INDEX_FILE_CAPACITY, indexFileCapacity -> globalConfiguration.setIndexFileCapacity(indexFileCapacity));

                        // 写请求线程池核心数
                        globalConfiguration.setIntProperty(properties, WRITE_CORE_POOL_SIZE, corePoolSize -> globalConfiguration.setWriteCorePoolSize(corePoolSize));

                        // 写请求线程池最大数
                        globalConfiguration.setIntProperty(properties, WRITE_MAX_POOL_SIZE, maxPoolSize -> globalConfiguration.setWriteMaxPoolSize(maxPoolSize));

                        // 写请求非核心线程最大空闲时间
                        globalConfiguration.setLongProperty(properties, WRITE_KEEP_ALIVE_TIME, keepAliveTime -> globalConfiguration.setWriteKeepAliveTime(keepAliveTime));

                        // 写请求线程池队列容量
                        globalConfiguration.setIntProperty(properties, WRITE_THREAD_POOL_QUQUE_CAPACITY, threadPoolQueueCapacity -> globalConfiguration.setWriteThreadPoolQueueCapacity(threadPoolQueueCapacity));

                        // 读请求线程池核心数
                        globalConfiguration.setIntProperty(properties, READ_CORE_POOL_SIZE, corePoolSize -> globalConfiguration.setReadCorePoolSize(corePoolSize));

                        // 读请求线程池最大数
                        globalConfiguration.setIntProperty(properties, READ_MAX_POOL_SIZE, maxPoolSize -> globalConfiguration.setReadMaxPoolSize(maxPoolSize));

                        // 读请求非核心线程最大空闲时间
                        globalConfiguration.setLongProperty(properties, READ_KEEP_ALIVE_TIME, keepAliveTime -> globalConfiguration.setReadKeepAliveTime(keepAliveTime));

                        // 读请求线程池队列容量
                        globalConfiguration.setIntProperty(properties, READ_THREAD_POOL_QUQUE_CAPACITY, threadPoolQueueCapacity -> globalConfiguration.setReadThreadPoolQueueCapacity(threadPoolQueueCapacity));

                        // netty连接处理线程数
                        globalConfiguration.setIntProperty(properties, NETTY_BOSS_GROUP_THREADS, threads -> globalConfiguration.setNettyBossGroupThreads(threads));

                        // netty工作线程数
                        globalConfiguration.setIntProperty(properties, NETTY_WORKER_GROUP_THREADS, threads -> globalConfiguration.setNettyWorkerGroupThreads(threads));

                        globalConfiguration.setLongProperty(properties, CONSUMER_OFFSET_SYNC_INTERVAL, consumerOffsetSyncInterval -> globalConfiguration.setConsumerOffsetSyncInterval(consumerOffsetSyncInterval));
                    }
                    // 初始化线程池
                    globalConfiguration.initGlobalThreadPool();
                }
            }
        }
        return globalConfiguration;
    }

    private void setLongProperty(Properties properties, String propertyName, Consumer<Long> consumer) {
        if (properties.getProperty(propertyName) != null) {
            try {
                long property = Long.parseLong(properties.getProperty(propertyName));
                consumer.accept(property);
            } catch (NumberFormatException e) {
                LOGGER.info("property " + propertyName + " illegal");
            }
        }
    }

    private void setIntProperty(Properties properties, String propertyName, Consumer<Integer> consumer) {
        if (properties.getProperty(propertyName) != null) {
            try {
                int property = Integer.parseInt(properties.getProperty(propertyName));
                consumer.accept(property);
            } catch (NumberFormatException e) {
                LOGGER.info("property " + propertyName + " illegal");
            }
        }
    }

    /**
     * 初始化线程池
     */
    private void initGlobalThreadPool() {
        writeExecutorService = ThreadPoolGenerator.newThreadPoolDynamic(writeCorePoolSize, writeMaxPoolSize, writeKeepAliveTime, writeThreadPoolQueueCapacity, THREAD_NAME_WRITE);
        readExecutorService = ThreadPoolGenerator.newThreadPoolDynamic(readCorePoolSize, readMaxPoolSize, readKeepAliveTime, readThreadPoolQueueCapacity, THREAD_NAME_READ);
    }

    public static MessageWriterProxy getMessageWriterProxy() throws NoSuchMethodException {
        return MessageWriterProxy.getInstance(writeStrategy, flushStrategy);
    }

    private static String getDefaultIndexPath() {
        return getDefaultPath(PATH_INDEX);
    }

    private static String getDefaultLogPath() {
        return getDefaultPath(PATH_LOG);
    }

    public static String getDefaultConsumerOffsetPath() {
        return getDefaultPath(PATH_OFFSET);
    }

    private static String getDefaultPath(String path) {
        String os = System.getProperty("os.name");
        String defaultLogPath;
        if (os != null && os.toLowerCase().startsWith("windows")) {
            defaultLogPath = "D:" + File.separator + "zero" + File.separator + path;
        } else {
            defaultLogPath = File.separator + "var" + File.separator + "tmp" + File.separator + "zero" + File.separator + path;
        }
        File file =new File(defaultLogPath);
        if  (!file.exists()) {
            file.mkdirs();
        }
        return defaultLogPath;
    }

    private void setMaxLogFileSize(long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
    }

    private void setIndexFileCapacity(long indexFileCapacity) {
        this.indexFileCapacity = indexFileCapacity;
    }

    private void setWriteCorePoolSize(int writeCorePoolSize) {
        this.writeCorePoolSize = writeCorePoolSize;
    }

    private void setWriteMaxPoolSize(int writeMaxPoolSize) {
        this.writeMaxPoolSize = writeMaxPoolSize;
    }

    private void setWriteKeepAliveTime(long writeKeepAliveTime) {
        this.writeKeepAliveTime = writeKeepAliveTime;
    }

    private void setWriteThreadPoolQueueCapacity(int writeThreadPoolQueueCapacity) {
        this.writeThreadPoolQueueCapacity = writeThreadPoolQueueCapacity;
    }

    private void setReadCorePoolSize(int readCorePoolSize) {
        this.readCorePoolSize = readCorePoolSize;
    }

    private void setReadMaxPoolSize(int readMaxPoolSize) {
        this.readMaxPoolSize = readMaxPoolSize;
    }

    private void setReadKeepAliveTime(long readKeepAliveTime) {
        this.readKeepAliveTime = readKeepAliveTime;
    }

    private void setReadThreadPoolQueueCapacity(int readThreadPoolQueueCapacity) {
        this.readThreadPoolQueueCapacity = readThreadPoolQueueCapacity;
    }

    private void setNettyBossGroupThreads(int nettyBossGroupThreads) {
        this.nettyBossGroupThreads = nettyBossGroupThreads;
    }

    private void setNettyWorkerGroupThreads(int nettyWorkerGroupThreads) {
        this.nettyWorkerGroupThreads = nettyWorkerGroupThreads;
    }

    private void setConsumerOffsetSyncInterval(long consumerOffsetSyncInterval) {
        this.consumerOffsetSyncInterval = consumerOffsetSyncInterval;
    }

    public int getPort() {
        return port;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    public String getLogPath() {
        return logPath;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public long getMaxLogFileSize() {
        return maxLogFileSize;
    }

    public long getIndexFileCapacity() {
        return indexFileCapacity;
    }

    public int getNettyBossGroupThreads() {
        return nettyBossGroupThreads;
    }

    public int getNettyWorkerGroupThreads() {
        return nettyWorkerGroupThreads;
    }

    public long getConsumerOffsetSyncInterval() {
        return consumerOffsetSyncInterval;
    }

    public String getConsumerOffsetPath() {
        return consumerOffsetPath;
    }

    public static GlobalConfiguration get() {
        return globalConfiguration;
    }

    public ExecutorService getWriteExecutorService() {
        return writeExecutorService;
    }

    public ExecutorService getReadExecutorService() {
        return readExecutorService;
    }
}
