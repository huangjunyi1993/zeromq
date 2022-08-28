package com.huangjunyi1993.zeromq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

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

    private int port;

    private String zkUrl;

    private String logPath;

    private String indexPath;

    private String consumerOffsetPath;

    private long maxLogFileSize = 1024 * 1024L;

    private long consumerOffsetSyncInterval = -1L;

    private volatile static GlobalConfiguration globalConfiguration;

    private GlobalConfiguration(int port, String zkUrl, String logPath, String indexPath, String consumerOffsetPath){
        this.port = port;
        this.zkUrl = zkUrl;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.consumerOffsetPath = consumerOffsetPath;
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

    public long getConsumerOffsetSyncInterval() {
        return consumerOffsetSyncInterval;
    }

    public String getConsumerOffsetPath() {
        return consumerOffsetPath;
    }

    public static GlobalConfiguration init(Properties properties) {
        if (globalConfiguration == null) {
            synchronized (GlobalConfiguration.class) {
                if (globalConfiguration == null) {
                    if (properties == null) {
                        globalConfiguration = new GlobalConfiguration(DEFAULT_PORT, DEFAULT_ZK_URL, getDefaultLogPath(), getDefaultIndexPath(), getDefaultConsumerOffsetPath());
                    } else {
                        int port = DEFAULT_PORT;
                        String zkurl = DEFAULT_ZK_URL;
                        String logPath = properties.getProperty(LOG_PATH);
                        String consumerOffsetPath = properties.getProperty(CONSUMER_OFFSET_PATH);
                        String indexPath = properties.getProperty(INDEX_PATH);
                        if (properties.getProperty(PORT) != null) {
                            port = Integer.valueOf(properties.getProperty(PORT));
                        }
                        if (properties.getProperty(ZK_URL) != null) {
                            zkurl = properties.getProperty(ZK_URL);
                        }
                        if (logPath == null) {
                            logPath = getDefaultLogPath();
                        }
                        if (indexPath == null) {
                            indexPath = getDefaultIndexPath();
                        }
                        if (consumerOffsetPath == null) {
                            consumerOffsetPath = getDefaultConsumerOffsetPath();
                        }
                        globalConfiguration = new GlobalConfiguration(port, zkurl, logPath, indexPath, consumerOffsetPath);
                        if (properties.getProperty(MAX_LOG_FILE_SIZE) != null) {
                            try {
                                long maxLogFileSize = Long.parseLong(properties.getProperty(MAX_LOG_FILE_SIZE));
                                globalConfiguration.setMaxLogFileSize(maxLogFileSize);
                            } catch (NumberFormatException e) {
                                LOGGER.info("property maxLogFileSize illegal");
                            }
                        }
                        if (properties.get(CONSUMER_OFFSET_SYNC_INTERVAL) != null) {
                            try {
                                long consumerOffsetSyncInterval = Long.parseLong(properties.getProperty(CONSUMER_OFFSET_SYNC_INTERVAL));
                                globalConfiguration.setConsumerOffsetSyncInterval(consumerOffsetSyncInterval);
                            } catch (NumberFormatException e) {
                                LOGGER.info("property maxLogFileSize illegal");
                            }
                        }
                    }
                }
            }
        }
        return globalConfiguration;
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

    private void setConsumerOffsetSyncInterval(long consumerOffsetSyncInterval) {
        this.consumerOffsetSyncInterval = consumerOffsetSyncInterval;
    }

    public static GlobalConfiguration get() {
        return globalConfiguration;
    }

}
