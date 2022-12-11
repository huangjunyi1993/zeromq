package com.huangjunyi1993.zeromq.core.writer.command;


import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * 写文件命令（暂时没有用到）
 * Created by huangjunyi on 2022/8/27.
 */
public class WriteCommand {

    private int writePosition;

    private String targetFile;

    private FileChannel fileChannel;

    private long writeLen;

    private MappedByteBuffer mappedByteBuffer;

    private byte[] bytes;

    private int serializationType;

    private String topic;

    public WriteCommand(int writePosition, String targetFile, FileChannel fileChannel, long writeLen, MappedByteBuffer mappedByteBuffer, byte[] bytes, int serializationType, String topic) {
        this.writePosition = writePosition;
        this.targetFile = targetFile;
        this.fileChannel = fileChannel;
        this.writeLen = writeLen;
        this.mappedByteBuffer = mappedByteBuffer;
        this.bytes = bytes;
        this.serializationType = serializationType;
        this.topic = topic;
    }

    public int getWritePosition() {
        return writePosition;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public long getWriteLen() {
        return writeLen;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getSerializationType() {
        return serializationType;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "WriteCommand{" +
                "writePosition=" + writePosition +
                ", targetFile='" + targetFile + '\'' +
                ", fileChannel=" + fileChannel +
                ", writeLen=" + writeLen +
                ", mappedByteBuffer=" + mappedByteBuffer +
                ", bytes=" + Arrays.toString(bytes) +
                ", serializationType=" + serializationType +
                ", topic='" + topic + '\'' +
                '}';
    }
}
