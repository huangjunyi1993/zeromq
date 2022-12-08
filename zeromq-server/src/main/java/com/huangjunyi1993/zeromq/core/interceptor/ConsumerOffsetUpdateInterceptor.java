package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Ack;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.Interceptor;
import com.huangjunyi1993.zeromq.core.handler.ZeroAckHandler;
import com.huangjunyi1993.zeromq.util.ConsumerOffsetCache;
import com.huangjunyi1993.zeromq.util.FileUtil;
import com.huangjunyi1993.zeromq.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_ACK;
import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.CONTEXT_VARIABLE_CONSUMER_OFFSET;

/**
 * 拦截器：消费者消费偏移量更新
 * Created by huangjunyi on 2022/8/21.
 */
public class ConsumerOffsetUpdateInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerOffsetUpdateInterceptor.class);

    private ConsumerOffsetCache consumerOffsetCache;

    private Map<String, MappedByteBuffer> mappedByteBufferMap = new ConcurrentHashMap<>();

    public ConsumerOffsetUpdateInterceptor() {
        this.consumerOffsetCache = ConsumerOffsetCache.get();
    }

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroAckHandler;
    }

    @Override
    public void pre(Context context) {
        Ack ack = (Ack) context.getVariable(CONTEXT_VARIABLE_ACK);
        try {
            // ack中包含topic和groupId，确定偏移量更新到哪个文件
            String file = GlobalConfiguration.get().getConsumerOffsetPath() + File.separator + ack.topic() + "_" + ack.consumerGroupId() + ".txt";
            MappedByteBuffer map;
            if (!mappedByteBufferMap.containsKey(file)) {
                map = IOUtil.getMappedByteBuffer(file);
                mappedByteBufferMap.put(file, map);
            } else {
                map = mappedByteBufferMap.get(file);
            }
            map.position(0);
            map.putLong(ack.offset());
            context.setVariable(CONTEXT_VARIABLE_CONSUMER_OFFSET, ack.offset());
        } catch (Exception e) {
            LOGGER.info("update conumer offset failed: ", e);
        }
    }

    @Override
    public void after(Context context) {

    }

    @Override
    public int order() {
        return 2;
    }
}
