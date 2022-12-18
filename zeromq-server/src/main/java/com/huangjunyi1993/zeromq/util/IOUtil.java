package com.huangjunyi1993.zeromq.util;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.core.writer.command.IndexCommand;
import com.huangjunyi1993.zeromq.core.writer.command.WriteCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static com.huangjunyi1993.zeromq.base.constants.ServerConstant.FLUSH_STRATEGY_SYNC;

/**
 * IO工具类
 * Created by huangjunyi on 2022/8/21.
 */
public class IOUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);

    /**
     * 同时写入消息日志和日志索引
     * @param bytes
     * @param serializationType
     * @param lastFile
     * @param lastIndexFile
     * @param logBuf
     * @param indexBuf
     * @param logPosition
     */
    public static void writeLogAndIndex(byte[] bytes, int serializationType,
                                        String lastFile, String lastIndexFile,
                                        MappedByteBuffer logBuf, MappedByteBuffer indexBuf,
                                        long logPosition, String flushStrategy) {
        LOGGER.info("==================> {} write the message log {} <==================", Thread.currentThread().getName(), lastFile);
        // 写入消息日志长度
        logBuf.putInt(bytes.length);
        // 写入序列化类型
        logBuf.putInt(serializationType);
        // 写入消息日志数据
        logBuf.put(bytes);
        if (FLUSH_STRATEGY_SYNC.equals(flushStrategy)) {
            logBuf.force();
        }

        LOGGER.info("==================> {} write the message index {} <==================", Thread.currentThread().getName(), lastIndexFile);
        // 写入索引记录 日志总偏移量=日志文件名记录的偏移量+日志写入位置
        indexBuf.putLong(FileUtil.getOffsetOfFileName(lastFile) + logPosition);
        if (FLUSH_STRATEGY_SYNC.equals(flushStrategy)) {
            indexBuf.force();
        }
    }

    /**
     *  顺序写日志
     * @param writeCommand
     * @return
     */
    public static void writeLog(WriteCommand writeCommand) throws IOException {
        LOGGER.info("==================> {} write the message log {} <==================", Thread.currentThread().getName(), writeCommand);
        MappedByteBuffer mappedByteBuffer = writeCommand.getMappedByteBuffer();
        mappedByteBuffer.position(writeCommand.getWritePosition());
        mappedByteBuffer.putInt(writeCommand.getBytes().length);
        mappedByteBuffer.putInt(writeCommand.getSerializationType());
        mappedByteBuffer.put(writeCommand.getBytes());
        mappedByteBuffer.force();
    }

    /**
     * 顺序写索引
     * @param indexCommand
     * @return
     * @throws IOException
     */
    public static void writeIndex(IndexCommand indexCommand) throws IOException {
        MappedByteBuffer mappedByteBuffer = indexCommand.getMappedByteBuffer();
        mappedByteBuffer.position(indexCommand.getWritePosition());
        mappedByteBuffer.putLong(indexCommand.getOffset());
        mappedByteBuffer.force();
    }

    /**
     * 顺序读日志
     * @param filePath
     * @param offset
     * @param batch
     * @return
     */
    public static List<Message> readLog(String filePath, long offset, int batch) throws IOException, ClassNotFoundException {
        List<Message> messages = new ArrayList<>(batch);
        if (filePath == null || "".equals(filePath)) {
            return messages;
        }
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
        LOGGER.info("==================> read message from {}, offset={} <==================", fileChannel, offset);
        for (int i = 0; i < batch; i++) {

            // 读取长度和序列化类型用的buf
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, 8);
            // 长度
            int len = mappedByteBuffer.getInt();
            // 凑不够一个批次
            if (len == 0) {
                return messages;
            }
            // 序列化类型
            int serializationType = mappedByteBuffer.getInt();

            // 读取消息用的buf
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset + 8, len);
            byte[] bytes = new byte[len];
            mappedByteBuffer.get(bytes);
            // 根据日志记录的序列化类型 获取对应的序列化器 进行序列化
            Serializer serializer = SerializerFactory.getSerializer(serializationType);
            Message message = serializer.deserialize(bytes);
            // 加入批次
            messages.add(message);
            // 更新文件内偏移量
            offset = offset + 8 + len;

            // 读到文件尾了
            if (fileChannel.size() <= offset) {
                // 找下一个文件
                long offsetOfNextFileName = FileUtil.getOffsetOfFileName(filePath);
                // 下一个日志文件名 = 当前文件名的偏移量 + 当前文件内偏移量
                String nextLogFileName = FileUtil.determineTargetLogFile(filePath.substring(0, filePath.lastIndexOf(File.separator)), offsetOfNextFileName + offset);
                if (nextLogFileName == null || "".equals(nextLogFileName) || filePath.equals(nextLogFileName)) {
                    break;
                }
                // 递归读取该批次剩下的
                messages.addAll(readLog(nextLogFileName, 0, batch - i - 1));
                break;
            }
        }
        fileChannel.close();
        return messages;
    }

    /**
     * 顺序读索引
     * @param filePath
     * @param offset
     * @return
     * @throws IOException
     */
    public static long readIndex(String filePath, long offset) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset * 8L, 8L);
        fileChannel.close();
        long messageLogOffset = mappedByteBuffer.getLong();
        return messageLogOffset;
    }



    public static byte[] readFile(String filePath) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
                StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        byte[] bytes = new byte[(int) fileChannel.size()];
        fileChannel.close();
        mappedByteBuffer.get(bytes);
        return bytes;
    }

    public static void writeFile(String filePath, byte[] bytes) throws IOException {
        if (filePath == null || "".equals(filePath)) {
            return;
        }
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.WRITE, StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Math.max(bytes.length, fileChannel.size()));
        fileChannel.close();
        mappedByteBuffer.clear();
        mappedByteBuffer.put(bytes);
        mappedByteBuffer.force();
    }

    public static MappedByteBuffer getMappedByteBuffer(String file) throws IOException {
        FileUtil.createIfNotExists(file);
        FileChannel open = FileChannel.open(Paths.get(file), StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer map = open.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        open.close();
        return map;
    }
}
