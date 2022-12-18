package com.huangjunyi1993.zeromq.base.constants;

/**
 * 服务端常量
 * Created by huangjunyi on 2022/8/28.
 */
public interface ServerConstant {

    int DEFAULT_PORT = 8888;

    String PATH_LOG = "log";

    String PATH_INDEX = "index";

    String PATH_OFFSET = "offset";

    String FLUSH_STRATEGY_ASYNC = "async";

    String FLUSH_STRATEGY_SYNC = "sync";

    String DEFAULT_WRITE_STRATEGY = "spin";

    long INDEX_ITEM_LENGTH = 8L;

    long LOG_HEAD_LENGTH = 8L;

    String THREAD_NAME_WRITE = "zeromq-thread-write";

    String THREAD_NAME_READ = "zeromq-thread-read";

}
