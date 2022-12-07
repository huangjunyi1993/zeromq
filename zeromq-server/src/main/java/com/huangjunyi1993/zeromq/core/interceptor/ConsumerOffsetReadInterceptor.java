package com.huangjunyi1993.zeromq.core.interceptor;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.Interceptor;
import com.huangjunyi1993.zeromq.core.handler.ZeroConsumerHandler;
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

import static com.huangjunyi1993.zeromq.base.constants.ContextVariableConstant.*;

/**
 * 拦截器：消费者消费偏移量读取
 * Created by huangjunyi on 2022/8/21.
 */
public class ConsumerOffsetReadInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerOffsetReadInterceptor.class);

    private ConsumerOffsetCache consumerOffsetCache;

    // 消费者偏移量文件内存映射表 topic => buf
    private Map<String, MappedByteBuffer> mappedByteBufferMap = new ConcurrentHashMap<>();

    public ConsumerOffsetReadInterceptor() {
        consumerOffsetCache = ConsumerOffsetCache.get();
    }

    @Override
    public boolean support(Handler handler) {
        return handler instanceof ZeroConsumerHandler;
    }

    @Override
    public void pre(Context context) {
        // 读取对应消费者对应topic的最新偏移量（消费到第几条消息）
        Request request = (Request) context.getVariable(CONTEXT_VARIABLE_REQUEST);
        try {
            // 该topic对应的消费者偏移量文件名
            String file = GlobalConfiguration.get().getConsumerOffsetPath() + File.separator + request.getTopic() + "_" + request.getConsumerGroupId() + ".txt";
            // 根据文件名获取对应的内存映射
            MappedByteBuffer map;
            if (!mappedByteBufferMap.containsKey(file)) {
                map = IOUtil.getMappedByteBuffer(file);
                mappedByteBufferMap.put(file, map);
            } else {
                map = mappedByteBufferMap.get(file);
            }
            // 读取偏移量 设置到变量表
            map.position(0);
            long offset = map.getLong();
            context.setVariable(CONTEXT_VARIABLE_CONSUMER_OFFSET, offset);
        } catch (Exception e) {
            LOGGER.info("read conumer offset failed: ", e);
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
