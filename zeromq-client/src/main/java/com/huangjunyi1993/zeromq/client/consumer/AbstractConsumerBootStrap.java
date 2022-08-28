package com.huangjunyi1993.zeromq.client.consumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.exception.ConsumerException;
import com.huangjunyi1993.zeromq.client.config.AbstractConfig;
import com.huangjunyi1993.zeromq.client.config.BrokerServerUrl;
import com.huangjunyi1993.zeromq.client.config.ConsumerConfig;
import com.huangjunyi1993.zeromq.client.remoting.transport.NettyClient;
import com.huangjunyi1993.zeromq.client.remoting.transport.ZeroConsumerHandler;
import com.huangjunyi1993.zeromq.client.task.ConsumerTask;
import com.huangjunyi1993.zeromq.client.util.ClientUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.TOPIC_DEFAULT;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_TOPIC;

/**
 * 消费者客户端抽象启动器：定义了消费者客户端的启动逻辑
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class AbstractConsumerBootStrap implements Consumer {

    private Map<String, Class<? extends Consumer>> TOPIC_CONSUMER_CLASS_MAP = new ConcurrentHashMap<>();

    private Map<String, Consumer> topicConsumerMap = new ConcurrentHashMap<>();

    private Map<String, Map<BrokerServerUrl, ConsumerTask>> consumerTaskMap = new ConcurrentHashMap<>();

    protected List<String> topicList = new CopyOnWriteArrayList<>();

    protected AbstractConfig config;

    protected CuratorFramework zkCli;

    private boolean isRunning;

    private Map<String, Map<String, List<BrokerServerUrl>>> topicConsumerIdUrlsMap = new ConcurrentHashMap<>();

    private NettyClient nettyClient;

    public AbstractConsumerBootStrap(AbstractConfig config) {
        this.config = config;
        ZeroConsumerHandler handler = new ZeroConsumerHandler(this);
        this.config.addChannelHandler(handler);
        this.zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        this.zkCli.start();
    }

    @Override
    public boolean onMessage(Message message) {
        Object headValue = message.getHead(MESSAGE_HEAD_TOPIC);
        String topic;
        if (headValue == null) {
            topic = TOPIC_DEFAULT;
        } else {
            topic = (String) headValue;
        }

        boolean handleSuccess;
        Consumer consumer = null;
        try {
            consumer = getConsumer(topic);
            preHandlerMessage(consumer, message, topic);
            handleSuccess = consumer.onMessage(message);
            return handleSuccess;
        } catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException|InstantiationException e) {
            throw new ConsumerException(String.format("An exception occurred while getting the consumer of the topic %s", topic));
        } catch (Exception e){
            return postOnError(consumer, message, topic, e, false);
        }finally {
            postHandlerMessage(consumer, message, topic);
        }
    }

    protected abstract boolean postOnError(Consumer consumer, Message message, String topic, Exception e, boolean result);

    protected abstract void postHandlerMessage(Consumer consumer, Message message, String topic);

    protected abstract void preHandlerMessage(Consumer consumer, Message message, String topic);

    public boolean start() throws Exception {

        List<String> topicLiSt = this.topicList;

        InterProcessLock lock = new InterProcessMutex(this.zkCli, "/lock/consumer");
        try {
            Stat brokersNodeStat = zkCli.checkExists().forPath("/brokers");
            while (brokersNodeStat == null) {
                LockSupport.parkNanos(1000 * 1000 * 1000);
                brokersNodeStat = zkCli.checkExists().forPath("/brokers");
            }

            while (!lock.acquire(10 * 1000, TimeUnit.SECONDS)) {}

            byte[] bytes = zkCli.getData().forPath("/brokers");
            List<BrokerServerUrl> brokerServerUrlList = ClientUtil.parseBrokerServerUrls(bytes);
            ConsumerConfig consumerConfig = (ConsumerConfig) this.config;

            Map<String, String> topicResultMap = new HashMap<>();
            topicLiSt.forEach(topic -> {
                String path = String.format("/topic/%s/consumers/%s", topic, consumerConfig.getGroupId());
                try {
                    List<String> consumerIdList = new ArrayList<>();
                    Stat stat = zkCli.checkExists().forPath(path);
                    if (stat != null) {
                        byte[] bytes1 = zkCli.getData().forPath(path);
                        String json = new String(bytes1, StandardCharsets.UTF_8);
                        JsonElement jsonElement = new JsonParser().parse(json);
                        JsonObject jsonObject = jsonElement.getAsJsonObject();
                        consumerIdList.addAll(jsonObject.keySet());
                    } else {
                        zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
                    }

                    if (!consumerIdList.contains(consumerConfig.getConsumerId())) {
                        consumerIdList.add(String.valueOf(consumerConfig.getConsumerId()));
                    }

                    Map<String, List<BrokerServerUrl>> consumerIdUrlsMap = new HashMap<>();
                    for (int i = 0; i < brokerServerUrlList.size(); i++) {
                        BrokerServerUrl url = brokerServerUrlList.get(i);
                        String consumerId = consumerIdList.get(i % consumerIdList.size());
                        if (consumerIdUrlsMap.containsKey(consumerId)) {
                            consumerIdUrlsMap.get(consumerId).add(url);
                        } else {
                            List<BrokerServerUrl> brokerServerUrls = new ArrayList<>();
                            brokerServerUrls.add(url);
                            consumerIdUrlsMap.put(consumerId, brokerServerUrls);
                        }
                    }

                    JsonObject result = new JsonObject();
                    consumerIdUrlsMap.forEach((comsumerId, urlList) -> {
                        result.addProperty(comsumerId, urlList.stream().map(BrokerServerUrl::toString).collect(Collectors.joining(",")));
                    });

                    topicResultMap.put(topic, result.toString());

                } catch (Exception e) {
                    throw new ConsumerException("An exception occurred while calculating the consumption allocation scheme");
                }
            });

            topicResultMap.forEach((topic, result) -> {
                String path = String.format("/topic/%s/consumers/%s", topic, consumerConfig.getGroupId());
                try {
                    zkCli.setData().forPath(path, result.getBytes(StandardCharsets.UTF_8));
                    updateTopicConsumerIdUrlsMap(topic, result);
                    addWatcher(topic, path);
                } catch (Exception e) {
                    throw new ConsumerException("Failed to update the consumer group node data", e);
                }
            });
        } catch (Exception e) {
            throw new ConsumerException("The consumer launch failed", e);
        } finally {
            lock.release();
        }

        LockSupport.parkNanos(5 * 1000 * 1000 * 1000L);

        runTask(topicList, topicConsumerIdUrlsMap, (ConsumerConfig) this.config);
        isRunning = true;
        return true;
    }

    private NettyClient getNettyClient() {
        if (nettyClient == null) {
            synchronized (this) {
                if (nettyClient == null) {
                    nettyClient = NettyClient.open(this.config);
                }
            }
        }
        return nettyClient;
    }

    protected void runTask(List<String> topicList, Map<String, Map<String, List<BrokerServerUrl>>> topicConsumerIdUrlsMap, ConsumerConfig config) {
        Map<String, Set<BrokerServerUrl>> currentConsumerTopicBrokerServerUrlsMap = new HashMap<>();
        topicList.forEach(topic -> {
            Map<String, List<BrokerServerUrl>> consumerIdUrsMap = topicConsumerIdUrlsMap.get(topic);
            if (consumerIdUrsMap != null) {
                List<BrokerServerUrl> serverUrls = consumerIdUrsMap.get(String.valueOf(config.getConsumerId()));
                if (serverUrls != null && serverUrls.size() > 0) {
                    currentConsumerTopicBrokerServerUrlsMap.computeIfAbsent(topic, k -> new HashSet<>());
                    currentConsumerTopicBrokerServerUrlsMap.get(topic).addAll(serverUrls);
                }
            }
        });

        List<ConsumerTask> consumerTasks = new ArrayList<>();
        currentConsumerTopicBrokerServerUrlsMap.forEach((topic, currentTopicBrokerServerUrls) -> {
            currentTopicBrokerServerUrls.forEach(brokerServerUrl -> {
                ConsumerTask consumerTask = new ConsumerTask(getNettyClient().getChannel(brokerServerUrl), brokerServerUrl, topic, config);
                consumerTaskMap.computeIfAbsent(topic, k -> new HashMap<>());
                consumerTaskMap.get(topic).put(brokerServerUrl, consumerTask);
                consumerTasks.add(consumerTask);
                consumerTask.start();
            });
        });
    }

    private void addWatcher(String topic, String path) throws Exception {
        NodeCache nodeCache = new NodeCache(this.zkCli, path);
        nodeCache.start();
        nodeCache.getListenable().addListener(() -> {
            if (nodeCache.getCurrentData() != null) {
                byte[] bytes = nodeCache.getCurrentData().getData();
                updateTopicConsumerIdUrlsMap(topic, new String(bytes, StandardCharsets.UTF_8));
            }
        });
    }

    private void updateTopicConsumerIdUrlsMap(String topic, String data) {
        topicConsumerIdUrlsMap.computeIfAbsent(topic, k -> new HashMap<>());
        Map<String, List<BrokerServerUrl>> consumerIdUrlsMap = topicConsumerIdUrlsMap.get(topic);
        JsonElement jsonElement = new JsonParser().parse(data);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        Set<String> consumerIds = jsonObject.keySet();
        consumerIds.forEach(consumerId -> {
            String urlsStr = jsonObject.get(consumerId).getAsString();
            if (urlsStr != null && !"".equals(urlsStr)) {
                String[] urls = urlsStr.split(",");
                List<BrokerServerUrl> newBrokerServerUrls = Arrays.stream(urls).map(url -> {
                    String[] hostPort = url.split(":");
                    BrokerServerUrl brokerServerUrl = new BrokerServerUrl(hostPort[0], Integer.valueOf(hostPort[1]));
                    return brokerServerUrl;
                }).collect(Collectors.toList());

                if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId) && consumerIdUrlsMap.get(consumerId) != null) {
                    List<BrokerServerUrl> diffBrokerServerUrls = new ArrayList<>();
                    List<BrokerServerUrl> brokerServerUrls = consumerIdUrlsMap.get(consumerId);
                    brokerServerUrls = brokerServerUrls.stream().filter(brokerServerUrl -> {
                        if (newBrokerServerUrls.contains(brokerServerUrl)) {
                            return true;
                        }
                        diffBrokerServerUrls.add(brokerServerUrl);
                        return false;
                    }).collect(Collectors.toList());
                    consumerIdUrlsMap.put(consumerId, brokerServerUrls);
                    //停止消费任务，并移除
                    Map<BrokerServerUrl, ConsumerTask> brokerServerUrlConsumerTaskMap = null;
                    if ((brokerServerUrlConsumerTaskMap = consumerTaskMap.get(topic)) != null) {
                        for (BrokerServerUrl diffBrokerServerUrl : diffBrokerServerUrls) {
                            ConsumerTask removeConsumerTask = brokerServerUrlConsumerTaskMap.remove(diffBrokerServerUrl);
                            if (removeConsumerTask != null) {
                                removeConsumerTask.shutdown();
                            }
                        }
                    }
                }

                newBrokerServerUrls.forEach(newBrokerServerUrl -> {
                    List<BrokerServerUrl> brokerServerUrls;
                    if (consumerIdUrlsMap.get(consumerId) == null) {
                        brokerServerUrls = new ArrayList<>();
                        brokerServerUrls.add(newBrokerServerUrl);
                        consumerIdUrlsMap.put(consumerId, brokerServerUrls);
                    } else if (!(brokerServerUrls = consumerIdUrlsMap.get(consumerId)).contains(newBrokerServerUrl)){
                        brokerServerUrls.add(newBrokerServerUrl);
                        if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId)) {
                            //添加新的消费任务
                            ConsumerTask consumerTask = new ConsumerTask(getNettyClient().getChannel(newBrokerServerUrl), newBrokerServerUrl, topic, (ConsumerConfig) config);
                            consumerTaskMap.computeIfAbsent(topic, k -> new HashMap<>());
                            consumerTaskMap.get(topic).put(newBrokerServerUrl, consumerTask);
                            consumerTask.start();
                        }
                    }
                });
            }
        });
    }

    public void register(String topic, Class<? extends Consumer> clazz) {
        if (!TOPIC_CONSUMER_CLASS_MAP.containsKey(topic)) {
            TOPIC_CONSUMER_CLASS_MAP.put(topic, clazz);
            topicList.add(topic);
        }
    }

    private Consumer getConsumer(String topic) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Consumer consumer = topicConsumerMap.get(topic);

        if (consumer == null) {
            Class<? extends Consumer> consumerClass = TOPIC_CONSUMER_CLASS_MAP.get(topic);
            if (consumerClass == null) {
                throw new ConsumerException(String.format("There is no registered consumer for %s topic", topic));
            }
            Constructor<? extends Consumer> constructor = consumerClass.getConstructor();
            return constructor.newInstance();
        }

        return consumer;
    }

}
