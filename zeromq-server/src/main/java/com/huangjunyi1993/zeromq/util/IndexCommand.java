package com.huangjunyi1993.zeromq.util;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 写索引命令（暂时没有用到）
 * Created by huangjunyi on 2022/8/28.
 */
public class IndexCommand {

    private String targetFile;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private int writePosition;

    private long offset;

    public IndexCommand(String targetFile, FileChannel fileChannel, MappedByteBuffer mappedByteBuffer, int writePosition, long offset) {
        this.targetFile = targetFile;
        this.fileChannel = fileChannel;
        this.mappedByteBuffer = mappedByteBuffer;
        this.writePosition = writePosition;
        this.offset = offset;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public int getWritePosition() {
        return writePosition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "IndexCommand{" +
                "targetFile='" + targetFile + '\'' +
                ", fileChannel=" + fileChannel +
                ", mappedByteBuffer=" + mappedByteBuffer +
                ", writePosition=" + writePosition +
                ", offset=" + offset +
                '}';
    }
}
