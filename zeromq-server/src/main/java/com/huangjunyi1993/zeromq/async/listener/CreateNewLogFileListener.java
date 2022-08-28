package com.huangjunyi1993.zeromq.async.listener;

import com.huangjunyi1993.zeromq.async.Event;
import com.huangjunyi1993.zeromq.async.Listener;
import com.huangjunyi1993.zeromq.async.event.CreateNewLogFileEvent;
import com.huangjunyi1993.zeromq.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Path;

/**
 * 服务端事件监听器：异步创建新的消息日志文件（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class CreateNewLogFileListener implements Listener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateNewLogFileListener.class);

    @Override
    public boolean support(Event event) {
        return event instanceof CreateNewLogFileEvent;
    }

    @Override
    public void onEvent(Event event) {
        if (!(event instanceof CreateNewLogFileEvent)) {
            LOGGER.info("This listener does not support this type of event {}", event.getClass().getName());
            return;
        }

        String dir = ((CreateNewLogFileEvent) event).getDir();

        try {
            String file = FileUtil.createNewLogFile(dir);
            LOGGER.info("The new log file was created successfully: {}", file);
        } catch (IOException e) {
            LOGGER.info("Failed to create a new file: ", e);
        }

    }

}
