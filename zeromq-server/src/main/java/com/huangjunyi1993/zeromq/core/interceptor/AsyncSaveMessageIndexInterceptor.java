package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.async.Broadcaster;
import com.huangjunyi1993.zeromq.async.ZeroBroadcaster;
import com.huangjunyi1993.zeromq.async.event.SaveMessageIndexEvent;
import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.Interceptor;
import com.huangjunyi1993.zeromq.core.handler.ZeroProducerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_CURRENT_MESSAGE_OFFSET;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_TOPIC;

/**
 * 拦截器：异步保存消息日志索引（暂时没有用到）
 * Created by huangjunyi on 2022/8/22.
 */
public class AsyncSaveMessageIndexInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSaveMessageIndexInterceptor.class);

    private Broadcaster broadcaster;

    public AsyncSaveMessageIndexInterceptor() {
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
        long currentMessageOffset = (long) context.getVariable(CONTEXT_VARIABLE_CURRENT_MESSAGE_OFFSET);
        String topic = (String) context.getVariable(CONTEXT_VARIABLE_TOPIC);
        SaveMessageIndexEvent saveMessageIndexEvent = SaveMessageIndexEvent.newEvent(currentMessageOffset, topic);
        if (!this.broadcaster.broadcaster(saveMessageIndexEvent)) {
            LOGGER.info("Publishing the event failed");
        }
    }

    @Override
    public int order() {
        return 4;
    }
}
