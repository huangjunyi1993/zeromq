package com.huangjunyi1993.zeromq.async.listener;

import com.huangjunyi1993.zeromq.async.Event;
import com.huangjunyi1993.zeromq.async.Listener;
import com.huangjunyi1993.zeromq.async.event.SaveMessageIndexEvent;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.util.FileUtil;
import com.huangjunyi1993.zeromq.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 服务端事件监听器：异步构建消息日志索引（暂时没有用到）
 * Created by huangjunyi on 2022/8/22.
 */
public class SaveMessageIndexListener implements Listener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveMessageIndexListener.class);

    private String indexPath;

    public SaveMessageIndexListener() {
        this.indexPath = GlobalConfiguration.get().getIndexPath();
    }

    @Override
    public boolean support(Event event) {
        return event instanceof SaveMessageIndexEvent;
    }

    @Override
    public void onEvent(Event event) {
        SaveMessageIndexEvent saveMessageIndexEvent = (SaveMessageIndexEvent) event;
        long currentMessageOffset = saveMessageIndexEvent.getCurrentMessageOffset();
        String topic = saveMessageIndexEvent.getTopic();
        String currentTopicIndexFilePath = indexPath + File.separator + topic;
        try {
            String lastIndexFile = FileUtil.findLastIndexFile(currentTopicIndexFilePath);
            if (lastIndexFile == null) {
                lastIndexFile = FileUtil.createTopicDirAndNewIndexFIle(GlobalConfiguration.get().getIndexPath(), topic);
            }
            long fileSize = FileUtil.getFileSize(lastIndexFile);
            if (fileSize >= 1000 * 8) {
                lastIndexFile = FileUtil.createNewIndexFile(lastIndexFile).toString();
            }
            //IOUtil.writeIndex(lastIndexFile, currentMessageOffset);
        } catch (IOException e) {
            LOGGER.info("Failed to save current message offset index: ", e);
        }
    }

}
