package com.huangjunyi1993.zeromq.client.remoting.support;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Request;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.exception.RemotingException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_ID;

/**
 * 参考dubbo的操作：
 * 生成者每发送一次消息，或者消费者发送一次请求，就会参数一个与之对应的ZeroFuture
 * 一个ZeroFuture对应一个id，服务端处理完成后，返回的协议包写到对应的id
 * 客户端接收到服务端返回后，唤醒在对应id的ZeroFuture上等待的线程
 * Created by huangjunyi on 2022/8/14.
 */
public class ZeroFuture extends CompletableFuture<Object> {

    // 全局回执表
    private static final Map<Long, ZeroFuture> FUTURES = new ConcurrentHashMap<>();

    // 全局请求信息表
    private static final Map<Long, Request> REQUESTS = new ConcurrentHashMap<>();

    private ZeroFuture(long id) {
        FUTURES.put(id, this);
    }

    public static ZeroFuture newProducerFuture(Message message) {
        if (message.getHead(MESSAGE_HEAD_ID) == null) {
            throw new RemotingException("The message ID cannot be empty");
        }

        // 创建一个回执，并缓存到全局回执表
        return new ZeroFuture((Long) message.getHead(MESSAGE_HEAD_ID));
    }

    public static void received(Response response) {
        try {
            // 根据服务器响应中的id，取出对应的回执
            ZeroFuture zeroFuture = FUTURES.get(response.getId());
            // 设置服务端响应到回执中，唤醒阻塞等待的线程
            zeroFuture.complete(response);
        } finally {
            // 从全局回执表中删除回执
            FUTURES.remove(response.getId());
        }
    }

    public static ZeroFuture newProducerFuture(Request request) {
        REQUESTS.put(request.getId(), request);
        return new ZeroFuture(request.getId());
    }

    public static Request removeRequest(long id) {
        return REQUESTS.remove(id);
    }
}
