package com.huangjunyi1993.zeromq.async.event;

import com.huangjunyi1993.zeromq.async.Event;

/**
 * 事件：异步创建新的消息日志文件（暂时没有用到）
 * Created by huangjunyi on 2022/8/21.
 */
public class CreateNewLogFileEvent implements Event {

    private String dir;

    public String getDir() {
        return dir;
    }

    private CreateNewLogFileEvent(String dir) {
        this.dir = dir;
    }

    public static CreateNewLogFileEvent newEvent(String dir) {
        return new CreateNewLogFileEvent(dir);
    }
}
