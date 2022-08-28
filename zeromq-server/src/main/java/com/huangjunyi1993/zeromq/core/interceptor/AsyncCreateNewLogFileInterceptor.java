package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.async.Broadcaster;
import com.huangjunyi1993.zeromq.async.ZeroBroadcaster;
import com.huangjunyi1993.zeromq.async.event.CreateNewLogFileEvent;
import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.Interceptor;
import com.huangjunyi1993.zeromq.core.handler.ZeroProducerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_CURRENT_FILE_SIZE;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_TOPIC;

/**
 * 拦截器：异步构建新的消息日志文件（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class AsyncCreateNewLogFileInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncCreateNewLogFileInterceptor.class);

    private Broadcaster broadcaster;

    private long DEFAULT_MAX_LOG_FILE_SIZE = 1024 * 1024L;

    public AsyncCreateNewLogFileInterceptor() {
        this.broadcaster = ZeroBroadcaster.getBroadcaster();
    }

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroProducerHandler;
    }

    @Override
    public void pre(Context context) {

    }

    @Override
    public void after(Context context) {
        long currentFileSize = (long) context.getVariable(CONTEXT_VARIABLE_CURRENT_FILE_SIZE);
        GlobalConfiguration globalConfiguration = GlobalConfiguration.get();
        if (currentFileSize > (globalConfiguration.getMaxLogFileSize() > 0 ? globalConfiguration.getMaxLogFileSize() : DEFAULT_MAX_LOG_FILE_SIZE)) {
            String topic = (String) context.getVariable(CONTEXT_VARIABLE_TOPIC);
            String dir = GlobalConfiguration.get().getLogPath() + File.separator + topic;
            CreateNewLogFileEvent createNewLogFileEvent = CreateNewLogFileEvent.newEvent(dir);
            if (!this.broadcaster.broadcaster(createNewLogFileEvent)) {
                LOGGER.info("Publishing the event failed");
            }
        }
    }

    @Override
    public int order() {
        return 3;
    }
}
