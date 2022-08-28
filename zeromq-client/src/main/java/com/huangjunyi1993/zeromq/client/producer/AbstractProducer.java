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
import org.apache.curator.retry.ExponentialBackoffRetry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.TOPIC_DEFAULT;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.*;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_SERIALIZATION_TYPE;
import static com.huangjunyi1993.zeromq.base.enums.MessageTypeEnum.MESSAGE;
import static com.huangjunyi1993.zeromq.base.enums.SerializationTypeEnum.JDK_NATIVE_SERIALIZATION;

/**
 * 生成者抽象：定义了发送消息者等待消息（同步或异步）返回的流程
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class AbstractProducer implements Producer {

    private static AtomicLong MESSAGE_ID_GENERATOR = new AtomicLong();

    private final AbstractConfig config;

    private CuratorFramework zkCli;

    private NettyClient nettyClient;

    private List<BrokerServerUrl> brokerServerUrlList = new CopyOnWriteArrayList<>();

    private Map<String, AtomicInteger> TOPIC_COUNTER_MAP = new ConcurrentHashMap<>();

    public AbstractProducer(AbstractConfig config) {
        this.config = config;
        this.zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        this.zkCli.start();
        this.nettyClient = NettyClient.open(config);
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
            message.putHead(MESSAGE_HEAD_ID, MESSAGE_ID_GENERATOR.getAndIncrement());
        }

        if (message.getHead(MESSAGE_HEAD_TOPIC) == null) {
            message.putHead(MESSAGE_HEAD_TOPIC, TOPIC_DEFAULT);
        }

        preSendMessage(message);

        final Response[] response = new Response[1];

        try {
            BrokerServerUrl url = routing((String) message.getHead(MESSAGE_HEAD_TOPIC), (String) message.getHead(MESSAGE_HEAD_ROUTING_KEY));
            Channel channel = nettyClient.getChannel(url);
            ZeroFuture zeroFuture = ZeroFuture.newProducerFuture(message);
            doSendMessage(channel, message);

            if (isAsync && producerListener != null) {
                zeroFuture.thenAccept(result -> {
                    response[0] = (Response) result;
                    producerListener.onResponse(response[0]);
                });
                return true;
            } else {
                long timeout = this.config.getTimeout();
                Object result = timeout > 0L ? zeroFuture.get(timeout, TimeUnit.MILLISECONDS) : zeroFuture.get();
                response[0] = (Response) result;
                if (response[0].isSuccess()) {
                    return true;
                }
                return false;
            }
        } catch (Exception e) {
            postOnError(message, response[0], e);
            return false;
        } finally {
            postResponseReceived(message, response[0]);
        }
    }

    private void doSendMessage(Channel channel, Message message) throws IOException {
        int serializationType = JDK_NATIVE_SERIALIZATION.getType();
        if (message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE) != null) {
            serializationType = (int) message.getHead(MESSAGE_HEAD_SERIALIZATION_TYPE);
        } else {
            message.putHead(MESSAGE_HEAD_SERIALIZATION_TYPE, serializationType);
        }

        Serializer serializer = SerializerFactory.getSerializer(serializationType);
        byte[] bytes = serializer.serialize(message);

        ZeroProtocol protocol = new ZeroProtocol(bytes.length, serializationType, MESSAGE.getType(), (long)message.getHead(MESSAGE_HEAD_ID), bytes);
        channel.writeAndFlush(protocol);
    }

    protected abstract void postOnError(Message message, Response response, Exception e);

    protected abstract void postResponseReceived(Message message, Response response);

    protected abstract void preSendMessage(Message message);

    protected BrokerServerUrl routing(String topic, String routingKey) {
        if (this.brokerServerUrlList == null || this.brokerServerUrlList.size() == 0) {
            this.brokerServerUrlList = getBrokerServerUrlListFromZK();
        }

        if (routingKey != null && !"".equals(routingKey)) {
            return this.brokerServerUrlList.get(routingKey.hashCode() % brokerServerUrlList.size());
        }

        return getBrokerServerUrlRoundRobin(topic, brokerServerUrlList);
    }

    protected BrokerServerUrl getBrokerServerUrlRoundRobin(String topic, List<BrokerServerUrl> brokerServerUrlList) {
        AtomicInteger counter = TOPIC_COUNTER_MAP.computeIfAbsent(topic, k -> new AtomicInteger());
        return brokerServerUrlList.get(counter.getAndIncrement() % brokerServerUrlList.size());
    }

    private List<BrokerServerUrl> getBrokerServerUrlListFromZK() {
        try {
            String path = "/brokers";
            byte[] bytes = zkCli.getData().forPath(path);
            List<BrokerServerUrl> brokerServerUrlList = updateBrokerServerUrls(bytes);
            addWatcher(path);
            return brokerServerUrlList;
        } catch (Exception e) {
            throw new ProducerException("Failed to obtain broker server url");
        }
    }

    private void addWatcher(String path) throws Exception {
        NodeCache nodeCache = new NodeCache(this.zkCli, path);
        nodeCache.start();
        nodeCache.getListenable().addListener(() -> {
            if (nodeCache.getCurrentData() != null) {
                byte[] bytes = nodeCache.getCurrentData().getData();
                updateBrokerServerUrls(bytes);
            }
        });
    }

    private List<BrokerServerUrl> updateBrokerServerUrls(byte[] bytes) {
        List<BrokerServerUrl> newBrokerServerUrlList = ClientUtil.parseBrokerServerUrls(bytes);

        List<BrokerServerUrl> oldBrokerServerUrlList = this.brokerServerUrlList.stream().filter(newBrokerServerUrlList::contains).collect(Collectors.toList());
        this.brokerServerUrlList.clear();
        this.brokerServerUrlList.addAll(oldBrokerServerUrlList);
        this.brokerServerUrlList.addAll(newBrokerServerUrlList.stream().filter(brokerServerUrl -> !this.brokerServerUrlList.contains(brokerServerUrl)).collect(Collectors.toList()));

        return this.brokerServerUrlList;
    }


}
