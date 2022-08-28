package com.huangjunyi1993.zeromq.async.event;

import com.huangjunyi1993.zeromq.async.Event;

/**
 * 事件：异步构建消息日志索引（暂时没有用到）
 * Created by huangjunyi on 2022/8/22.
 */
public class SaveMessageIndexEvent implements Event {

    private long currentMessageOffset;

    private String topic;

    private SaveMessageIndexEvent(long currentMessageOffset, String topic) {
        this.currentMessageOffset = currentMessageOffset;
        this.topic = topic;
    }

    public long getCurrentMessageOffset() {
        return currentMessageOffset;
    }

    public String getTopic() {
        return topic;
    }

    public static SaveMessageIndexEvent newEvent(long currentMessageOffset, String topic) {
        return new SaveMessageIndexEvent(currentMessageOffset, topic);
    }
}
