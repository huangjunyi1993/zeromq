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

    private static final Map<Long, ZeroFuture> FUTURES = new ConcurrentHashMap<>();

    private static final Map<Long, Request> REQUESTS = new ConcurrentHashMap<>();

    private ZeroFuture(long id) {
        FUTURES.put(id, this);
    }

    public static ZeroFuture newProducerFuture(Message message) {
        if (message.getHead(MESSAGE_HEAD_ID) == null) {
            throw new RemotingException("The message ID cannot be empty");
        }

        return new ZeroFuture((Long) message.getHead(MESSAGE_HEAD_ID));
    }

    public static void received(Response response) {
        try {
            ZeroFuture zeroFuture = FUTURES.get(response.getId());
            zeroFuture.complete(response);
        } finally {
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
