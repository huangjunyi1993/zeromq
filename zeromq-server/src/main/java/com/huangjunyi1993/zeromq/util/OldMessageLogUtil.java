package com.huangjunyi1993.zeromq.util;

import com.huangjunyi1993.zeromq.base.Context;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息日志工具类V1
 * 处理并发读写通过 synchronized + 内存映射
 * Created by huangjunyi on 2022/8/27.
 */
public class OldMessageLogUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(OldMessageLogUtil.class);

    private Object writeLogLock = new Object();

    // 日志工具类实例表 topic => 工具类实例
    private static final Map<String, OldMessageLogUtil> TOPIC_MESSAGE_LOG_UTIL_MAP = new ConcurrentHashMap<>();

    private OldMessageLogUtil() {}

    public static OldMessageLogUtil getMessageLogUtil(String topic) {
        TOPIC_MESSAGE_LOG_UTIL_MAP.computeIfAbsent(topic, key -> new OldMessageLogUtil());
        return TOPIC_MESSAGE_LOG_UTIL_MAP.get(topic);
    }

    public void writeMessageLog(String topic, byte[] bytes, int serializationType, Context context) throws IOException, InterruptedException {

        // 该topic的消息日志存储路径 ${logPath}/${topic}
        String dir = GlobalConfiguration.get().getLogPath() + File.separator + topic;
        // 本次写入的长度 本次写入的长度 加8字节长度是用于在前面存长度和序列化类型
        int writeLen = bytes.length + 8;
        // 该topic对应的消息日志索引文件存储路径
        String currentTopicIndexFilePath = GlobalConfiguration.get().getIndexPath() + File.separator + topic;

        String lastFile;
        String lastIndexFile;
        long writePosition;
        FileChannel logChannel = null;
        FileChannel indexChannel = null;
        MappedByteBuffer logBuf;
        MappedByteBuffer indexBuf;

        try {
            // 加锁，同步吸入日志和索引
            synchronized (writeLogLock) {
                // 找到最新的日志文件
                lastFile = FileUtil.findLastLOGFile(dir);
                if (lastFile == null) {
                    lastFile = FileUtil.createTopicDirAndNewLogFIle(GlobalConfiguration.get().getLogPath(), topic);
                }
                logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                writePosition = logChannel.size();
                // 如果日志文件写满了，新建一个
                if (writePosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                    lastFile = FileUtil.createNewLogFile(dir);
                    logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                    writePosition = logChannel.size();
                }
                // 日志文件的内存映射
                logBuf = logChannel.map(FileChannel.MapMode.READ_WRITE, writePosition, writeLen);

                // 找到最新的索引文件
                lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                if (lastIndexFile == null) {
                    lastIndexFile = FileUtil.createTopicDirAndNewIndexFIle(GlobalConfiguration.get().getIndexPath(), topic);
                }
                indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                long indexPosition = indexChannel.size();
                // 如果索引文件写满了，新建一个
                if (indexPosition + 8L > 1000 * 8L) {
                    lastIndexFile = FileUtil.createNewIndexFile(lastIndexFile);
                    indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                    indexPosition = indexChannel.size();
                }
                // 索引文件的内存映射
                indexBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, indexPosition, 8L);
            }

            if (logBuf != null && indexBuf != null) {
                // 写入日志和索引
                IOUtil.writeLogAndIndex(bytes, serializationType, lastFile, lastIndexFile, logBuf, indexBuf, (int) (FileUtil.getOffsetOfFileName(lastFile) + writePosition));
            }
        } finally {
            close(logChannel, indexChannel);
        }
    }

    private static void close(Closeable... closeables) throws IOException {
        if (closeables == null || closeables.length == 0) {
            return;
        }

        for (Closeable closeable : closeables) {
            if (closeable != null) {
                closeable.close();
            }
        }
    }

}
