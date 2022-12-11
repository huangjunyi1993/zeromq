package com.huangjunyi1993.zeromq.core.writer;

import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.util.FileUtil;
import com.huangjunyi1993.zeromq.util.IOUtil;
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
 * 消息日志写入器v2
 * 处理并发读写通过 CAS + 内存映射
 * Created by huangjunyi on 2022/8/27.
 */
@WriteStrategy("spin")
public class SpinMessageWriter extends AbstractMessageWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpinMessageWriter.class);

    private Object writeLogLock = new Object();

    private Object writeIndexLock = new Object();

    private AtomicReference<PositionCounter> positionCounterAtomicReference;

    private static class PositionCounter {
        private int logPosition;
        private int indexPosition;
    }

    /**
     * 持久化消息到日志文件，并构建消息索引
     * @param topic
     * @param bytes
     * @param serializationType
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void writeMessageLog(String topic, byte[] bytes, int serializationType, String flushStrategy) throws IOException, InterruptedException {

        // 该topic的消息日志存储路径 ${logPath}/${topic}
        String dir = GlobalConfiguration.get().getLogPath() + File.separator + topic;
        // 本次写入的长度 加8字节长度是用于在前面存长度和序列化类型
        int writeLen = bytes.length + 8;
        // 该topic对应的消息日志索引文件存储路径
        String currentTopicIndexFilePath = GlobalConfiguration.get().getIndexPath() + File.separator + topic;

        // 最新的日志文件
        String lastFile;
        // 最新的索引文件
        String lastIndexFile;
        // 日志文件channel
        FileChannel logChannel = null;
        // 索引文件channel
        FileChannel indexChannel = null;
        // 日志文件buf
        MappedByteBuffer logBuf;
        // 索引文件buf
        MappedByteBuffer indexBuf;

        try {
            // 双重检测锁，如果日志写入位置计数器原子引用为空，则初始化
            if (positionCounterAtomicReference == null) {
                synchronized (this) {
                    if (positionCounterAtomicReference == null) {
                        // 日志写入位置计数器
                        PositionCounter positionCounter = new PositionCounter();
                        // 找到最新的日志文件 根据文件名判断 文件名是前面所有文件写入的日志大小偏移量
                        lastFile = FileUtil.findLastLOGFile(dir);
                        if (lastFile == null) {
                            // 不存在任何日志文件，代表第一次启动，同时创建文件夹
                            FileUtil.createTopicDirAndNewLogFIle(GlobalConfiguration.get().getLogPath(), topic);
                            // 设置初始日志写入位置为0
                            positionCounter.logPosition = 0;
                        } else {
                            // 找到最新的日志文件
                            FileChannel channel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
                            // 定位日志最新写入位置，并设置
                            positionCounter.logPosition = locateLogWritePosition(channel.size(), map);
                        }

                        // 找到最新的索引文件 依然是根据偏移量判断 文件名是前面所有文件写入的索引大小偏移量
                        lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                        if (lastIndexFile == null) {
                            // 不存在索引文件，代表第一次启动，同时创建文件夹
                            FileUtil.createTopicDirAndNewIndexFIle(GlobalConfiguration.get().getIndexPath(), topic);
                            // 设置初始索引写入位置为0
                            positionCounter.indexPosition = 0;
                        } else {
                            // 找到最新的索引文件
                            FileChannel channel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
                            // 定位索引最新写入位置，并设置
                            positionCounter.indexPosition = locateIndexWritePosition(channel.size(), map, lastIndexFile);
                        }

                        // 创建日志写入位置计数器的原子引用
                        positionCounterAtomicReference = new AtomicReference<>(positionCounter);
                    }
                }
            }

            // 日志写入位置
            int logPosition;
            // 索引写入位置
            int indexPosition;
            // 计数器比较值
            PositionCounter expect;
            // 计数器更新值
            PositionCounter update;
            do {
                // 双重检测锁，日志文件写满了，再开一个，同时新开一个计数器
                if (positionCounterAtomicReference.get().logPosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                    synchronized (writeLogLock) {
                        if (positionCounterAtomicReference.get().logPosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                            FileUtil.createNewLogFile(dir);
                            update = new PositionCounter();
                            // 新的计数器的日志写入位置设为0
                            update.logPosition = 0;
                            // 新的计数器的索引写入位置不变
                            update.indexPosition = positionCounterAtomicReference.get().indexPosition;
                            positionCounterAtomicReference.set(update);
                        }
                    }
                }

                // 双重检测锁，索引文件写满了，再开一个，同时新开一个计数器
                if (positionCounterAtomicReference.get().indexPosition + 8L > 1000 * 8L) {
                    synchronized (writeIndexLock) {
                        if (positionCounterAtomicReference.get().indexPosition + 8L > 1000 * 8L) {
                            FileUtil.createNewIndexFile(FileUtil.findLastIndexFile(currentTopicIndexFilePath));
                            update = new PositionCounter();
                            // 新的计数器的日志写入位置不变
                            update.logPosition = positionCounterAtomicReference.get().logPosition;
                            // 新的计数器的索引写入位置设为0
                            update.indexPosition = 0;
                            positionCounterAtomicReference.set(update);
                        }
                    }
                }

                // 找到最新的日志文件和索引文件
                lastFile = FileUtil.findLastLOGFile(dir);
                lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                // 当前的计数器
                expect = positionCounterAtomicReference.get();
                // 日志写入位置
                logPosition = expect.logPosition;
                // 索引写入位置
                indexPosition = expect.indexPosition;
                // 更新后的计数器
                update = new PositionCounter();
                // 写入后的日志位置
                update.logPosition = logPosition + writeLen;
                // 写入后的索引位置
                update.indexPosition = indexPosition + 8;
                // cas 更新
            } while (!positionCounterAtomicReference.compareAndSet(expect, update));

            // 内存映射
            logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            logBuf = logChannel.map(FileChannel.MapMode.READ_WRITE, logPosition, writeLen);
            indexBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, indexPosition, 8L);
            if (logBuf != null && indexBuf != null) {
                // 写入日志和索引
                IOUtil.writeLogAndIndex(bytes, serializationType, lastFile, lastIndexFile, logBuf, indexBuf, logPosition, flushStrategy);
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
