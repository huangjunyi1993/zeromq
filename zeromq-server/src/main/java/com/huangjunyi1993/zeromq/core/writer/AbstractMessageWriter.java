package com.huangjunyi1993.zeromq.core.writer;

import com.huangjunyi1993.zeromq.util.FileUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.MappedByteBuffer;

/**
 * 消息日志写入器抽象类
 * Created by huangjunyi on 2022/12/11.
 */
public abstract class AbstractMessageWriter implements MessageWriter {

    @Override
    public void writeMessageLog(String topic, byte[] bytes, int serializationType) throws IOException, InterruptedException, IllegalAccessException, InvocationTargetException, InstantiationException {

    }

    abstract void writeMessageLog(String topic, byte[] bytes, int serializationType, String flushStrategy) throws IOException, InterruptedException, IllegalAccessException, InvocationTargetException, InstantiationException;

    /**
     * 定位日志最新写入点位
     * @param channelSize
     * @param map
     * @return
     */
    protected int locateLogWritePosition(long channelSize, MappedByteBuffer map) {
        int position = map.position();
        int temp;
        // 定位到日志最新的未写入位置
        while (position < channelSize) {
            temp = map.getInt();
            if (temp == 0) {
                // 最新的日志写入位置
                return position;
            }
            map.getInt();
            map.get(new byte[temp]);
            position = map.position();
        }
        return position;
    }

    /**
     * 定位索引最新写入点位
     * @param channelSize
     * @param map
     * @return
     */
    protected int locateIndexWritePosition(long channelSize, MappedByteBuffer map, String lastIndexFile) {
        int position = map.position();
        while (position < channelSize) {
            if (map.getLong() == 0 && (FileUtil.getOffsetOfFileName(lastIndexFile) != 0 || position != 0)) {
                return position;
            }
            position = map.position();
        }
        return position;
    }

}
