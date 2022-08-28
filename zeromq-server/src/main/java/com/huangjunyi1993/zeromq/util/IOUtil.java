package com.huangjunyi1993.zeromq.util;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * IO工具类
 * Created by huangjunyi on 2022/8/21.
 */
public class IOUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);

    /**
     * 同时吸入消息日志和日志索引
     * @param bytes
     * @param serializationType
     * @param lastFile
     * @param lastIndexFile
     * @param logBuf
     * @param indexBuf
     * @param logPosition
     */
    public static void writeLogAndIndex(byte[] bytes, int serializationType, String lastFile, String lastIndexFile, MappedByteBuffer logBuf, MappedByteBuffer indexBuf, int logPosition) {
        LOGGER.info("==================> {} write the message log {} <==================", Thread.currentThread().getName(), lastFile);
        logBuf.putInt(bytes.length);
        logBuf.putInt(serializationType);
        logBuf.put(bytes);
        logBuf.force();

        LOGGER.info("==================> {} write the message index {} <==================", Thread.currentThread().getName(), lastIndexFile);
        indexBuf.putLong(FileUtil.getOffsetOfFileName(lastFile) + logPosition);
        indexBuf.force();
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

        FileWriter writer = new FileWriter("D:/zero/write_index.txt",true);
        writer.write(indexCommand.getOffset() + "\n");
        writer.close();
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
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
                StandardOpenOption.READ);
        LOGGER.info("==================> read message from {}, offset={} <==================", fileChannel, offset);
        for (int i = 0; i < batch; i++) {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, 8);
            int len = mappedByteBuffer.getInt();
            if (len == 0) {
                return messages;
            }
            int serializationType = mappedByteBuffer.getInt();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset + 8, len);
            byte[] bytes = new byte[len];
            mappedByteBuffer.get(bytes);
            Serializer serializer = SerializerFactory.getSerializer(serializationType);
            Message message = serializer.deserialize(bytes);
            messages.add(message);
            offset = offset + 8 + len;
            if (fileChannel.size() <= offset) {
                long offsetOfNextFileName = FileUtil.getOffsetOfFileName(filePath);
                String nextLogFileName = FileUtil.determineTargetLogFile(filePath.substring(0, filePath.lastIndexOf(File.separator)), offsetOfNextFileName + offset);
                if (nextLogFileName == null || "".equals(nextLogFileName) || filePath.equals(nextLogFileName)) {
                    break;
                }
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
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
                StandardOpenOption.READ);
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
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
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
