package com.huangjunyi1993.zeromq.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者偏移量缓存（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class ConsumerOffsetCache {

    private Map<String, Map<Integer, Long>> topicConsumerGroupOffsetMap;

    private static ConsumerOffsetCache consumerOffsetCache;

    private ConsumerOffsetCache(String offsetFileName) throws IOException {
        topicConsumerGroupOffsetMap = new ConcurrentHashMap<>();

        // 读取消费者偏移量文件，并解析填充topicConsumerGroupOffsetMap
        Path offsetFilePath = Paths.get(offsetFileName);
        if (!Files.exists(offsetFilePath, LinkOption.NOFOLLOW_LINKS)) {
            Files.createFile(offsetFilePath);
            return;
        }
        byte[] data = IOUtil.readFile(offsetFileName);
        String consumerOffsetJsonStr = new String(data);
        JsonParser parser = new JsonParser();
        JsonElement topicConsumerOffsetJsonTree = parser.parse(consumerOffsetJsonStr);
        if (!topicConsumerOffsetJsonTree.isJsonNull()) {
            JsonObject topicConsumerOffsetJsonObj = topicConsumerOffsetJsonTree.getAsJsonObject();
            topicConsumerOffsetJsonObj.keySet().forEach(topic -> {
                JsonObject consumerOffsetJsonObj = topicConsumerOffsetJsonObj.getAsJsonObject(topic);
                consumerOffsetJsonObj.keySet().forEach(consumerGroupId -> {
                    long offset = consumerOffsetJsonObj.getAsJsonPrimitive(consumerGroupId).getAsLong();
                    topicConsumerGroupOffsetMap.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
                    Map<Integer, Long> consumerGroupOffsetMap = topicConsumerGroupOffsetMap.get(topic);
                    consumerGroupOffsetMap.put(Integer.valueOf(consumerGroupId), offset);
                });
            });
        }
    }

    public static void init(String offsetFileName) throws IOException {
        consumerOffsetCache = new ConsumerOffsetCache(offsetFileName);
    }

    public static ConsumerOffsetCache get() {
        return consumerOffsetCache;
    }

    public void update(String topic, int consumerGroupId, long offset) {
        topicConsumerGroupOffsetMap.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
        topicConsumerGroupOffsetMap.get(topic).put(consumerGroupId, offset);
    }

    public long getOffset(String topic, int consumerGroupId) {
        if (topicConsumerGroupOffsetMap.containsKey(topic)) {
            Map<Integer, Long> consumerGroupOffsetMap = topicConsumerGroupOffsetMap.get(topic);
            if (consumerGroupOffsetMap.containsKey(consumerGroupId)) {
                return consumerGroupOffsetMap.get(consumerGroupId);
            } else {
                consumerGroupOffsetMap.put(consumerGroupId, 0L);
                return 0L;
            }
        } else {
            Map<Integer, Long> consumerGroupOffsetMap = new ConcurrentHashMap<>();
            consumerGroupOffsetMap.put(consumerGroupId, 0L);
            topicConsumerGroupOffsetMap.put(topic, consumerGroupOffsetMap);
            return 0L;
        }
    }

    public Map<String, Map<Integer, Long>> getTopicConsumerGroupOffsetMap() {
        return topicConsumerGroupOffsetMap;
    }

}
