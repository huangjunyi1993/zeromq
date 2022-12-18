package com.huangjunyi1993.zeromq.base.constants;

/**
 * 服务端配置文件配置项
 * Created by huangjunyi on 2022/8/21.
 */
public interface GlobalConfigurablePropertyConstant {

    String PORT = "port";

    String ZK_URL = "zkurl";

    String LOG_PATH = "logPath";

    String INDEX_PATH = "indexPath";

    String CONSUMER_OFFSET_PATH = "consumerOffsetPath";

    String CONSUMER_OFFSET_SYNC_INTERVAL = "consumerOffsetSyncInterval";

    String MAX_LOG_FILE_SIZE = "maxLogFileSize";

    String INDEX_FILE_CAPACITY = "indexFileCapacity";

    String FLUSH_STRATEGY = "flushStrategy";

    String WRITE_STRATEGY = "writeStrategy";

    String WRITE_CORE_POOL_SIZE = "writeCorePoolSize";

    String WRITE_MAX_POOL_SIZE = "writeMaxPoolSize";

    String WRITE_KEEP_ALIVE_TIME = "writeKeepAliveTime";

    String WRITE_THREAD_POOL_QUQUE_CAPACITY = "writeThreadPoolQueueCapacity";

    String READ_CORE_POOL_SIZE = "readCorePoolSize";

    String READ_MAX_POOL_SIZE = "readMaxPoolSize";

    String READ_KEEP_ALIVE_TIME = "readKeepAliveTime";

    String READ_THREAD_POOL_QUQUE_CAPACITY = "readThreadPoolQueueCapacity";

    String NETTY_BOSS_GROUP_THREADS = "nettyBossGroupThreads";

    String NETTY_WORKER_GROUP_THREADS = "nettyWorkerGroupThreads";

}
