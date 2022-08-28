package com.huangjunyi1993.zeromq.task;

import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.SUFFIX_LOG;

/**
 * 定时任务：异步创建新消息日志文件（暂时没有用到）
 * Created by huangjunyi on 2022/8/27.
 */
public class CreateNewLogFileTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateNewLogFileTask.class);

    @Override
    protected void process() {
        String dir = GlobalConfiguration.get().getLogPath();

        try {
            List<Path> topicDirs = Files.list(Paths.get(dir)).collect(Collectors.toList());
            for (Path topicDir : topicDirs) {
                String logFileName = FileUtil.findLastFile(topicDir.toString(), SUFFIX_LOG);
                if (FileUtil.getFileSize(logFileName) > GlobalConfiguration.get().getMaxLogFileSize()) {
                    String file = FileUtil.createNewLogFile(topicDir.toString());
                    LOGGER.info("The new log file was created successfully: {}", file);
                }
            }
        } catch (IOException e) {
            LOGGER.info("Failed to create a new file {}", e);
        }
    }

}
