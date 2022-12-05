package com.huangjunyi1993.zeromq.client.listener;

import com.huangjunyi1993.zeromq.base.entity.Response;

/**
 * 生产者异步监听器（发送消息收到服务端返回时触发回调）
 * Created by huangjunyi on 2022/8/11.
 */
public interface ProducerListener {

    void onResponse(Response response);

}
