package com.huangjunyi1993.zeromq.base.constants;

/**
 * 公共常量
 * Created by huangjunyi on 2022/8/13.
 */
public interface CommonConstant {

    String TOPIC_DEFAULT = "default";

    int ACK_SUCCESS = 1;

    String DEFAULT_ZK_URL = "127.0.0.1:2181";

    String SUFFIX_LOG = "log";

    String SUFFIX_INDEX = "index";

    String THREAD_NAME = "zeromq-thread";

    String ZK_PATH_BROKERS = "/zero/brokers";

    String ZK_PATH_CONSUMER_LOCK = "/zero/lock/consumer";

    String ZK_PATH_CONSUMER_TOPIC = "/zero/topic/%s/consumers/%s";

    String ZK_PATH_CONSUMER = "/zero/consumer/%s";

}
