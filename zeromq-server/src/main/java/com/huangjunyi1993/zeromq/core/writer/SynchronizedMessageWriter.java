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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息日志写入器V1
 * 处理并发读写通过 synchronized + 内存映射
 * Created by huangjunyi on 2022/8/27.
 */
@WriteStrategy("sync")
public class SynchronizedMessageWriter extends AbstractMessageWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedMessageWriter.class);

    private Object writeLogLock = new Object();

    private volatile long logPosition = 0L;
    private volatile long indexPosition = 0L;
    private volatile boolean logWritePositionUninitialized = true;
    private volatile boolean indexWritePositionUninitialized = true;

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
        // 本次写入的长度 本次写入的长度 加8字节长度是用于在前面存长度和序列化类型
        int writeLen = bytes.length + 8;
        // 该topic对应的消息日志索引文件存储路径
        String currentTopicIndexFilePath = GlobalConfiguration.get().getIndexPath() + File.separator + topic;

        String lastFile;
        String lastIndexFile;
        long currentLogWritePosition;
        long currentIndexWritePosition;
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
                    logPosition = 0;
                }
                currentLogWritePosition = logPosition;

                // logWritePositionUninitialized为true，写入位置未初始化，先初始化写入位置
                if (logWritePositionUninitialized) {
                    logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                    logBuf = logChannel.map(FileChannel.MapMode.READ_WRITE, currentLogWritePosition, logChannel.size());
                    currentLogWritePosition = locateLogWritePosition(logChannel.size(), logBuf);
                    logPosition = currentLogWritePosition;
                    logWritePositionUninitialized = false;
                }

                // 如果日志文件写满了，新建一个
                if (currentLogWritePosition + writeLen > GlobalConfiguration.get().getMaxLogFileSize()) {
                    currentLogWritePosition = 0;
                    logPosition = currentLogWritePosition;
                    lastFile = FileUtil.createNewLogFile(dir);
                }
                // 更新下一次的写入位置
                logPosition += writeLen;

                // 找到最新的索引文件
                lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
                if (lastIndexFile == null) {
                    lastIndexFile = FileUtil.createTopicDirAndNewIndexFIle(GlobalConfiguration.get().getIndexPath(), topic);
                    indexPosition = 0;
                }
                currentIndexWritePosition = indexPosition;

                // indexWritePositionUninitialized为true，写入位置未初始化，先初始化写入位置
                if (indexWritePositionUninitialized) {
                    indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
                    indexBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, currentIndexWritePosition, indexChannel.size());
                    currentIndexWritePosition = locateIndexWritePosition(indexChannel.size(), indexBuf, lastIndexFile);
                    indexPosition = currentIndexWritePosition;
                    indexWritePositionUninitialized = false;
                }

                // 如果索引文件写满了，新建一个
                if (currentIndexWritePosition + 8L > 1000 * 8L) {
                    currentIndexWritePosition = 0;
                    indexPosition = currentIndexWritePosition;
                    lastIndexFile = FileUtil.createNewIndexFile(lastIndexFile);
                }
                indexPosition += 8L;
            }

            // 内存映射
            logChannel = FileChannel.open(Paths.get(lastFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            indexChannel = FileChannel.open(Paths.get(lastIndexFile), StandardOpenOption.WRITE, StandardOpenOption.READ);
            logBuf = logChannel.map(FileChannel.MapMode.READ_WRITE, currentLogWritePosition, writeLen);
            indexBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, currentIndexWritePosition, 8L);
            // 写入日志和索引到
            IOUtil.writeLogAndIndex(bytes, serializationType, lastFile, lastIndexFile, logBuf, indexBuf, (int) (FileUtil.getOffsetOfFileName(lastFile) + currentLogWritePosition), flushStrategy);
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
