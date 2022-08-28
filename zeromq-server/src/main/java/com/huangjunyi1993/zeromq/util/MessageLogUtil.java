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
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 消息日志工具类V2
 * 处理并发读写通过 CAS + 内存映射
 * Created by huangjunyi on 2022/8/27.
 */
public class MessageLogUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageLogUtil.class);

    private Object writeLogLock = new Object();

    private Object writeIndexLock = new Object();

    private static final Map<String, MessageLogUtil> TOPIC_MESSAGE_LOG_UTIL_MAP = new ConcurrentHashMap<>();

    private AtomicReference<PositionCounter> positionCounterAtomicReference;

    private static class PositionCounter {
        private int logPosition;
        private int indexPosition;
    }

    private MessageLogUtil() {}

    /**
     * 获取对应topic的日志工具实例
     * @param topic
     * @return
     */
    public static MessageLogUtil getMessageLogUtil(String topic) {
        TOPIC_MESSAGE_LOG_UTIL_MAP.computeIfAbsent(topic, key -> new MessageLogUtil());
        return TOPIC_MESSAGE_LOG_UTIL_MAP.get(topic);
    }

    /**
     * 持久化消息到日志文件，并构建消息索引
     * @param topic
     * @param bytes
     * @param serializationType
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void writeMessageLog(String topic, byte[] bytes, int serializationType, Context context) throws IOException, InterruptedException {

        String dir = GlobalConfiguration.get().getLogPath() + File.separator + topic;
        int writeLen = bytes.length + 8;
        String currentTopicIndexFilePath = GlobalConfiguration.get().getIndexPath() + File.separator + topic;

        String lastFile;
        String lastIndexFile;
        FileChannel logChannel = null;
        FileChannel indexChannel = null;
        MappedByteBuffer logBuf;
        MappedByteBuffer indexBuf;

        try {
            if (positionCounterAtomicReference == null) {
                synchronized (this) {
                    if (positionCounterAtomicReference == null) {
                        PositionCounter positionCounter = new PositionCounter();
                        lastFile = FileUtil.findLastLOGFile(dir);
                        if (lastFile == null) {
                            FileUtil.createTopicDirAndNewLogFIle(GlobalConfiguration.get().getLogPath(), topic);
                            positionCounter.logPosition = 0;
                        } else {
                            FileChannel channel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
                            int position = map.position();
                            int temp;
                            while (position < channel.size()) {
                                temp = map.getInt();
                                if (temp == 0) {
                                    positionCounter.logPosition = position;
                                    break;
                                }
                                map.getInt();
                                map.get(new byte[temp]);
                                position = map.position();
                            }
                        }

                        lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                        if (lastIndexFile == null) {
                            FileUtil.createTopicDirAndNewIndexFIle(GlobalConfiguration.get().getIndexPath(), topic);
                            positionCounter.indexPosition = 0;
                        } else {
                            FileChannel channel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
                            int position = map.position();
                            while (position < channel.size()) {
                                if (map.getLong() == 0 && (FileUtil.getOffsetOfFileName(lastIndexFile) != 0 || position != 0)) {
                                    positionCounter.indexPosition = position;
                                    break;
                                }
                                position = map.position();
                            }
                        }
                        positionCounterAtomicReference = new AtomicReference<>(positionCounter);
                    }
                }
            }

            int logPosition;
            int indexPosition;
            PositionCounter expect;
            PositionCounter update;
            do {
                if (positionCounterAtomicReference.get().logPosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                    synchronized (writeLogLock) {
                        if (positionCounterAtomicReference.get().logPosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                            FileUtil.createNewLogFile(dir);
                            update = new PositionCounter();
                            update.logPosition = 0;
                            update.indexPosition = positionCounterAtomicReference.get().indexPosition;
                            positionCounterAtomicReference.set(update);
                        }
                    }
                }
                if (positionCounterAtomicReference.get().indexPosition + 8L > 1000 * 8L) {
                    synchronized (writeIndexLock) {
                        if (positionCounterAtomicReference.get().indexPosition + 8L > 1000 * 8L) {
                            FileUtil.createNewIndexFile(FileUtil.findLastIndexFile(currentTopicIndexFilePath));
                            update = new PositionCounter();
                            update.logPosition = positionCounterAtomicReference.get().logPosition;
                            update.indexPosition = 0;
                            positionCounterAtomicReference.set(update);
                        }
                    }
                }
                lastFile = FileUtil.findLastLOGFile(dir);
                lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                expect = positionCounterAtomicReference.get();
                logPosition = expect.logPosition;
                indexPosition = expect.indexPosition;
                update = new PositionCounter();
                update.logPosition = logPosition + writeLen;
                update.indexPosition = indexPosition + 8;
            } while (!positionCounterAtomicReference.compareAndSet(expect, update));

            logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            logBuf = logChannel.map(FileChannel.MapMode.READ_WRITE, logPosition, writeLen);
            indexBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, indexPosition, 8L);

            if (logBuf != null && indexBuf != null) {
                IOUtil.writeLogAndIndex(bytes, serializationType, lastFile, lastIndexFile, logBuf, indexBuf, logPosition);
            }
        } finally {
            close(logChannel, indexChannel);
        }
    }

    /**
     * 关闭IO通道
     * @param closeables
     * @throws IOException
     */
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
