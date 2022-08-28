package com.huangjunyi1993.zeromq.task;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.util.ConsumerOffsetCache;
import com.huangjunyi1993.zeromq.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 定时任务：定时同步消费者消费偏移量缓存到磁盘（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class ConsumerOffsetSyncTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerOffsetSyncTask.class);

    private ConsumerOffsetCache consumerOffsetCache;

    private String offsetFileName;

    public ConsumerOffsetSyncTask() {
        this(GlobalConfiguration.get().getConsumerOffsetSyncInterval(), GlobalConfiguration.get().getConsumerOffsetPath() + File.separator + "offset.json");
    }

    public ConsumerOffsetSyncTask(long interval, String offsetFileName) {
        super(interval);
        consumerOffsetCache = ConsumerOffsetCache.get();
        this.offsetFileName = offsetFileName;
    }

    @Override
    protected void process() {
        Map<String, Map<Integer, Long>> topicConsumerGroupOffsetMap = consumerOffsetCache.getTopicConsumerGroupOffsetMap();
        Gson gson = new Gson();
        String json = gson.toJson(topicConsumerGroupOffsetMap);
        try {
            IOUtil.writeFile(offsetFileName, json.getBytes());
        } catch (IOException e) {
            LOGGER.info("flush the consumer offset file failed: {}", e);
        }
    }


}
