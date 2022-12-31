package com.huangjunyi1993.zeromq.client.producer;

import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.entity.Response;
import com.huangjunyi1993.zeromq.base.exception.ProducerException;
import com.huangjunyi1993.zeromq.base.protocol.ZeroProtocol;
import com.huangjunyi1993.zeromq.base.serializer.Serializer;
import com.huangjunyi1993.zeromq.base.serializer.SerializerFactory;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.config.BrokerServerUrl;
import com.huangjunyi1993.zeromq.client.listener.ProducerListener;
import com.huangjunyi1993.zeromq.client.remoting.support.ZeroFuture;
import com.huangjunyi1993.zeromq.client.remoting.transport.NettyClient;
import com.huangjunyi1993.zeromq.client.util.ClientUtil;
import io.netty.channel.Channel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.TOPIC_DEFAULT;
import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.ZK_PATH_BROKERS;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.MESSAGE;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * 生成者抽象：定义了发送消息者等待消息（同步或异步）返回的流程
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class AbstractProducer implements Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProducer.class);

    // 消息id生成器
    private static AtomicLong MESSAGE_ID_GENERATOR = new AtomicLong();

    // 全局配置
    private final AbstractConfig config;

    // zk客户端
    private CuratorFramework zkCli;

    // 服务端url列表
    private List<BrokerServerUrl> brokerServerUrlList = new CopyOnWriteArrayList<>();

    // 计数器表，topic => 计数器
    private Map<String, AtomicInteger> TOPIC_COUNTER_MAP = new ConcurrentHashMap<>();

    private static final int DEFAULT_SEND_WAIT = 5;

    public AbstractProducer(AbstractConfig config) {
        // 保存全局配置
        this.config = config;
        // 创建zk客户端
        this.zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        // 开启key客户端
        this.zkCli.start();
    }

    @Override
    public boolean sendMessage(Message message) {
        return sendMessage(message, null, false);
    }

    @Override
    public boolean sendMessageAsync(Message message, ProducerListener producerListener) {
        return sendMessage(message, producerListener, true);
    }

    private boolean sendMessage(Message message, ProducerListener producerListener, boolean isAsync) {

        if (message.getHead(MESSAGE_HEAD_ID) == null) {
            // 生成默认的消息id
            message.putHead(MESSAGE_HEAD_ID, MESSAGE_ID_GENERATOR.getAndIncrement());
        }

        if (message.getHead(MESSAGE_HEAD_TOPIC) == null) {
            // 默认的topic
            message.putHead(MESSAGE_HEAD_TOPIC, TOPIC_DEFAULT);
        }

        // 发送消息前置处理
        preSendMessage(message);

        // 发送消息响应结果
        final Response[] response = new Response[1];

        try {
            // 根据topic和routingKey，进行消息路由，确定目标Broker
            BrokerServerUrl url = routing((String) message.getHead(MESSAGE_HEAD_TOPIC), (String) message.getHead(MESSAGE_HEAD_ROUTING_KEY));
            // 获取连接到对应服务器的IO通道
            Channel channel = NettyClient.getChannel(config, url);
            if (channel == null) {
                LOGGER.info("channel is null!");
                return false;
            }
            if (!channel.isActive()) {
                LOGGER.info("channel is not active!");
                return false;
            }
            // 创建一个回执，回执会缓存到全局回执表
            ZeroFuture zeroFuture = ZeroFuture.newProducerFuture(message);
            // 真正发送消息
            doSendMessage(channel, message);

            if (isAsync && producerListener != null) {
                // 异步发送
                zeroFuture.thenAccept(result -> {
                    response[0] = (Response) result;
                    producerListener.onResponse(response[0]);
                });
                return true;
            } else {
                // 阻塞式发送
                long timeout = this.config.getTimeout();
                Object result = timeout > 0L ? zeroFuture.get(timeout, TimeUnit.MILLISECONDS) : zeroFuture.get(DEFAULT_SEND_WAIT, TimeUnit.SECONDS);
                if (result != null) {
                    response[0] = (Response) result;
                    if (response[0].isSuccess()) {
                        return true;
                    }
                }
                LOGGER.info("send message failed!");
                return false;
            }
        } catch (Exception e) {
            // 发送消息发生错误
            LOGGER.error("send message failed!", e);
            postOnError(message, response[0], e);
            return false;
        } finally {
            // 消息发送完成
            postResponseReceived(message, response[0]);
        }
    }

    private void doSendMessage(Channel channel, Message message) throws IOException {

        // 确定要使用的序列化协议，默认是JDK原生序列化协议
        int serializationType = JDK_NATIVE_SERIALIZATION.getType();
        if (message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE) != null) {
            serializationType = (int) message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE);
        } else {
            message.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, serializationType);
        }

        // 从序列化器工厂获取一个序列化器
        Serializer serializer = SerializerFactory.getSerializer(serializationType);
        // 使用序列化器，进行消息序列化
        byte[] bytes = serializer.serialize(message);

        // 组装成协议对象
        ZeroProtocol protocol = new ZeroProtocol(bytes.length, serializationType, MESSAGE.getType(), (long)message.getHead(MESSAGE_HEAD_ID), bytes);
        // 发送数据到IO通道
        channel.writeAndFlush(protocol);
    }

    // 模板方法，消息发送错误后置处理
    protected abstract void postOnError(Message message, Response response, Exception e);

    // 模板方法，消息发送后置处理
    protected abstract void postResponseReceived(Message message, Response response);

    // 模板方法，消息发送前置处理
    protected abstract void preSendMessage(Message message);

    /**
     * 消息路由
     * @param topic 消息topic
     * @param routingKey 路由键
     * @return
     */
    protected BrokerServerUrl routing(String topic, String routingKey) {

        // 服务器Url列表为空，从zk中拉取
        if (this.brokerServerUrlList == null || this.brokerServerUrlList.size() == 0) {
            this.brokerServerUrlList = getBrokerServerUrlListFromZK();
        }

        // 路由键不为空，根据路由键hash取模，确定目标Broker
        if (routingKey != null && !"".equals(routingKey)) {
            return this.brokerServerUrlList.get(routingKey.hashCode() % brokerServerUrlList.size());
        }

        // 路由键为空，轮询
        return getBrokerServerUrlRoundRobin(topic, brokerServerUrlList);
    }

    protected BrokerServerUrl getBrokerServerUrlRoundRobin(String topic, List<BrokerServerUrl> brokerServerUrlList) {
        AtomicInteger counter = TOPIC_COUNTER_MAP.computeIfAbsent(topic, k -> new AtomicInteger());
        return brokerServerUrlList.get(counter.getAndIncrement() % brokerServerUrlList.size());
    }

    /**
     * 从zk拉取服务器列表
     * @return
     */
    private List<BrokerServerUrl> getBrokerServerUrlListFromZK() {
        try {

            // 从zk拉取服务器信息
            String path = ZK_PATH_BROKERS;
            List<String> brokers = null;
            while (brokers == null || brokers.size() == 0) {
                LockSupport.parkNanos(1000 * 1000 * 1000L);
                brokers = zkCli.getChildren().forPath(path);
            }

            // 更新服务器列表
            List<BrokerServerUrl> brokerServerUrlList = updateBrokerServerUrls(brokers);

            // 重新注册监听
            addWatcher(path);

            return brokerServerUrlList;
        } catch (Exception e) {
            throw new ProducerException("Failed to obtain broker server url");
        }
    }

    private void addWatcher(String path) throws Exception {
        PathChildrenCache cache = new PathChildrenCache(this.zkCli, path, true);
        cache.start();
        cache.getListenable().addListener((client, event) -> {
            // 监听节点发生变化，重新拉取服务器信息，进行更新操作
            List<String> brokers = client.getChildren().forPath(path);
            updateBrokerServerUrls(brokers);
        });
    }

    private List<BrokerServerUrl> updateBrokerServerUrls(List<String> brokers) {
        // 解析服务器列表
        List<BrokerServerUrl> newBrokerServerUrlList = ClientUtil.parseBrokerServerUrls(brokers);

        // 过滤出下线的brokers，关闭客户端
        List<BrokerServerUrl> downBrokerServerUrlList = this.brokerServerUrlList.stream().filter(brokerServerUrl -> !newBrokerServerUrlList.contains(brokerServerUrl))
                .collect(Collectors.toList());
        NettyClient.shutDown(downBrokerServerUrlList);

        // 过滤出当前的服务器列表中，需要保留的服务器
        List<BrokerServerUrl> oldBrokerServerUrlList = this.brokerServerUrlList.stream()
                .filter(newBrokerServerUrlList::contains).collect(Collectors.toList());

        // 先清空服务器列表
        this.brokerServerUrlList.clear();
        this.brokerServerUrlList.addAll(oldBrokerServerUrlList);
        this.brokerServerUrlList.addAll(newBrokerServerUrlList.stream()
                .filter(brokerServerUrl -> !this.brokerServerUrlList.contains(brokerServerUrl))
                .collect(Collectors.toList()));

        return this.brokerServerUrlList;
    }


}
