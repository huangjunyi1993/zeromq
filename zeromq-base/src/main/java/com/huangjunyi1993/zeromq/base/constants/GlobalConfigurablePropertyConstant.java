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

    String FLUSH_STRATEGY = "flushStrategy";

    String WRITE_STRATEGY = "writeStrategy";

}
