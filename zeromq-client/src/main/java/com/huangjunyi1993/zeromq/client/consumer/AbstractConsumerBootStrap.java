package com.huangjunyi1993.zeromq.client.consumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.huangjunyi1993.zeromq.base.entity.Message;
import com.huangjunyi1993.zeromq.base.exception.ConsumerException;
import com.huangjunyi1993.zeromq.base.util.ThreadPoolGenerator;
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

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.THREAD_NAME;
import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.TOPIC_DEFAULT;
import static com.huangjunyi1993.zeromq.base.constants.MessageHeadConstant.MESSAGE_HEAD_TOPIC;

/**
 * 消费者客户端抽象启动器：定义了消费者客户端的启动逻辑
 * Created by huangjunyi on 2022/8/14.
 */
public abstract class AbstractConsumerBootStrap implements Consumer {

    // 消费者注册表 topic => consumer class
    private Map<String, Class<? extends Consumer>> TOPIC_CONSUMER_CLASS_MAP = new ConcurrentHashMap<>();

    // 消费者表 topic => consumer
    private Map<String, Consumer> topicConsumerMap = new ConcurrentHashMap<>();

    // 消费者客户端定时任务表 topic => (brokerUrl => task)
    private Map<String, Map<BrokerServerUrl, ConsumerTask>> consumerTaskMap = new ConcurrentHashMap<>();

    // 订阅的主题列表
    protected List<String> topicList = new CopyOnWriteArrayList<>();

    // 消费者全局配置
    protected AbstractConfig config;

    // zk客户端
    protected CuratorFramework zkCli;

    // 是否正在运行
    private boolean isRunning;

    // 主题 消费者 服务器 双层映射表 topic => consumerId => brokerUrls
    private Map<String, Map<String, List<BrokerServerUrl>>> topicConsumerIdUrlsMap = new ConcurrentHashMap<>();

    // netty客户端
    private NettyClient nettyClient;

    private ExecutorService executorService;

    public AbstractConsumerBootStrap(AbstractConfig config) {
        // 保存全局配置
        this.config = config;

        // 创建消费者处理器，并保存到全局配置中
        ZeroConsumerHandler handler = new ZeroConsumerHandler(this);
        this.config.addChannelHandler(handler);

        // 创建并开启zk客户端
        this.zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        this.zkCli.start();

        this.executorService = ThreadPoolGenerator.newThreadPoolDynamic(config.getCorePoolSize(), config.getMaxPoolSize(), config.getKeepAliveTime(), config.getThreadPoolQueueCapacity(), THREAD_NAME);
    }

    /**
     * 消息消费，消费者启动器也继承了Consumer，属于顶层消费者，负责分发任务
     * @param message 消息
     * @return
     */
    @Override
    public boolean onMessage(Message message) {

        // 当前消息的主题 topic
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
            // 根据topic获取消费者
            consumer = getConsumer(topic);
            // 模板方法 消息消费前置处理
            preHandlerMessage(consumer, message, topic);
            // 处理消息，获取消费结果，并返回消费结果
            handleSuccess = consumer.onMessage(message);
            return handleSuccess;
        } catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException|InstantiationException e) {
            throw new ConsumerException(String.format("An exception occurred while getting the consumer of the topic %s", topic));
        } catch (Exception e){
            // 模板方法 消息消费异常后置处理
            return postOnError(consumer, message, topic, e, false);
        }finally {
            // 模板方法 消息消费后置处理
            postHandlerMessage(consumer, message, topic);
        }
    }

    /**
     * 模板方法，消息消费异常后置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     * @param e 异常
     * @param result 消费结果
     * @return
     */
    protected abstract boolean postOnError(Consumer consumer, Message message, String topic, Exception e, boolean result);

    /**
     * 模板方法，消息消费后置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     */
    protected abstract void postHandlerMessage(Consumer consumer, Message message, String topic);

    /**
     * 模板方法，消息消费前置处理
     * @param consumer 消费者
     * @param message 消息
     * @param topic 主题
     */
    protected abstract void preHandlerMessage(Consumer consumer, Message message, String topic);

    public boolean start() throws Exception {

        // 当前消费者客户端所有要订阅的主题
        List<String> topicLiSt = this.topicList;

        // 分布式锁，防止消费者客户端并发rebalance
        InterProcessLock lock = new InterProcessMutex(this.zkCli, "/lock/consumer");
        try {

            // 自旋检查/brokers节点是否存在
            Stat brokersNodeStat = zkCli.checkExists().forPath("/brokers");
            while (brokersNodeStat == null) {
                LockSupport.parkNanos(1000 * 1000 * 1000);
                brokersNodeStat = zkCli.checkExists().forPath("/brokers");
            }

            // 自旋获取锁
            while (!lock.acquire(10 * 1000, TimeUnit.SECONDS)) {}

            // 从zk获取服务器信息
            byte[] bytes = zkCli.getData().forPath("/brokers");
            // 解析服务器信息
            List<BrokerServerUrl> brokerServerUrlList = ClientUtil.parseBrokerServerUrls(bytes);
            ConsumerConfig consumerConfig = (ConsumerConfig) this.config;

            // 循环遍历所有订阅的topic，进行rebalance
            Map<String, String> topicResultMap = new HashMap<>();
            topicLiSt.forEach(topic -> {

                // zk：/topic/{topicName}/consumers/{groupId}
                String path = String.format("/topic/%s/consumers/%s", topic, consumerConfig.getGroupId());
                try {
                    // 注册到zk上当前topic当前group的所有消费者id列表
                    List<String> consumerIdList = new ArrayList<>();
                    Stat stat = zkCli.checkExists().forPath(path);
                    if (stat != null) {
                        byte[] bytes1 = zkCli.getData().forPath(path);
                        String json = new String(bytes1, StandardCharsets.UTF_8);
                        JsonElement jsonElement = new JsonParser().parse(json);
                        JsonObject jsonObject = jsonElement.getAsJsonObject();
                        consumerIdList.addAll(jsonObject.keySet());
                    } else {
                        // 节点路径不存在，创建
                        zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
                    }

                    // 消费者id列表不包含当前消费者，添加
                    if (!consumerIdList.contains(consumerConfig.getConsumerId())) {
                        consumerIdList.add(String.valueOf(consumerConfig.getConsumerId()));
                    }

                    // rebalance： consumerId => brokers 轮询
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

                    // rebalance解析成json {"consumerId1": "brokerUrl1,brokerUrl2", "consumerId2": "brokerUrl3,brokerUrl4"}
                    JsonObject result = new JsonObject();
                    consumerIdUrlsMap.forEach((comsumerId, urlList) -> {
                        result.addProperty(comsumerId, urlList.stream().map(BrokerServerUrl::toString).collect(Collectors.joining(",")));
                    });

                    // 保存rebalance结果：topic => json
                    topicResultMap.put(topic, result.toString());

                } catch (Exception e) {
                    throw new ConsumerException("An exception occurred while calculating the consumption allocation scheme");
                }
            });

            // rebalance结果处理
            topicResultMap.forEach((topic, result) -> {
                String path = String.format("/topic/%s/consumers/%s", topic, consumerConfig.getGroupId());
                try {
                    // 1、循环推到zk
                    zkCli.setData().forPath(path, result.getBytes(StandardCharsets.UTF_8));
                    // 2、更新双层映射表
                    updateTopicConsumerIdUrlsMap(topic, result);
                    // 3、添加监听，后面有其他消费者加入，触发更新双层映射表
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

        // 睡5秒，等待同时启动的其他消费者rebalance
        LockSupport.parkNanos(5 * 1000 * 1000 * 1000L);

        // 创建并运行消费任务
        runTask(topicList, topicConsumerIdUrlsMap, (ConsumerConfig) this.config);
        // 修复运行标志 当前正在运行
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

        // 当前消费者订阅的主题 => 服务器列表
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

        // 创建任务 建立连接 添加到任务表中 启动任务
        List<ConsumerTask> consumerTasks = new ArrayList<>();
        currentConsumerTopicBrokerServerUrlsMap.forEach((topic, currentTopicBrokerServerUrls) -> {
            currentTopicBrokerServerUrls.forEach(brokerServerUrl -> {
                ConsumerTask consumerTask = new ConsumerTask(getNettyClient().getChannel(brokerServerUrl), brokerServerUrl, topic, config, executorService);
                consumerTaskMap.computeIfAbsent(topic, k -> new HashMap<>());
                consumerTaskMap.get(topic).put(brokerServerUrl, consumerTask);
                consumerTasks.add(consumerTask);
                consumerTask.start();
            });
        });
    }

    /**
     * 堆指定路径添加监听
     * @param topic 订阅的主题
     * @param path zk上指定topic和group的路径
     * @throws Exception
     */
    private void addWatcher(String topic, String path) throws Exception {
        // 监听当前topic下当前group分配的变动，触发更新
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
        // 创建外围映射表
        topicConsumerIdUrlsMap.computeIfAbsent(topic, k -> new HashMap<>());
        // 内层映射表
        Map<String, List<BrokerServerUrl>> consumerIdUrlsMap = topicConsumerIdUrlsMap.get(topic);
        // 解析当前topic的rebalance结果
        JsonElement jsonElement = new JsonParser().parse(data);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        Set<String> consumerIds = jsonObject.keySet();
        // 遍历：{"consumerId1": "brokerUrl1,brokerUrl2", "consumerId2": "brokerUrl3,brokerUrl4"}
        consumerIds.forEach(consumerId -> {

            // 服务器地址字符串 brokerUrl1,brokerUrl2
            String urlsStr = jsonObject.get(consumerId).getAsString();
            if (urlsStr != null && !"".equals(urlsStr)) {
                String[] urls = urlsStr.split(",");
                // 解析成服务器信息列表newBrokerServerUrls
                List<BrokerServerUrl> newBrokerServerUrls = Arrays.stream(urls).map(url -> {
                    String[] hostPort = url.split(":");
                    BrokerServerUrl brokerServerUrl = new BrokerServerUrl(hostPort[0], Integer.valueOf(hostPort[1]));
                    return brokerServerUrl;
                }).collect(Collectors.toList());

                // 不再分配给自己的服务器，停止消费任务，不再向该broker拉取消息
                // 当前正在运行（如果当前正在启动 false） && 当前遍历到的consumerId是自己 && 内层映射表中有分配给自己的服务器
                if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId) && consumerIdUrlsMap.get(consumerId) != null) {
                    // 不再分配给自己的服务器
                    List<BrokerServerUrl> diffBrokerServerUrls = new ArrayList<>();
                    // 由自己负责的服务器
                    List<BrokerServerUrl> brokerServerUrls = consumerIdUrlsMap.get(consumerId);
                    brokerServerUrls = brokerServerUrls.stream().filter(brokerServerUrl -> {
                        if (newBrokerServerUrls.contains(brokerServerUrl)) {
                            return true;
                        }
                        diffBrokerServerUrls.add(brokerServerUrl);
                        return false;
                    }).collect(Collectors.toList());
                    // 由自己负责的服务器，放入内层映射表
                    consumerIdUrlsMap.put(consumerId, brokerServerUrls);

                    // 不再分配给自己的服务器，并移除消费任务，并停止
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

                // 由自己负责的服务器，添加新的消费任务，并启动
                newBrokerServerUrls.forEach(newBrokerServerUrl -> {
                    List<BrokerServerUrl> brokerServerUrls;
                    if (consumerIdUrlsMap.get(consumerId) == null) {
                        brokerServerUrls = new ArrayList<>();
                        brokerServerUrls.add(newBrokerServerUrl);
                        consumerIdUrlsMap.put(consumerId, brokerServerUrls);
                    } else if (!(brokerServerUrls = consumerIdUrlsMap.get(consumerId)).contains(newBrokerServerUrl)){
                        // 由自己负责的服务器，内层映射表没有，添加
                        brokerServerUrls.add(newBrokerServerUrl);
                        // 当前正在运行（如果当前正在启动 false） && 当前遍历到的consumerId是自己
                        if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId)) {
                            // 添加新的消费任务，建立连接，并启动消费任务
                            ConsumerTask consumerTask = new ConsumerTask(getNettyClient().getChannel(newBrokerServerUrl), newBrokerServerUrl, topic, (ConsumerConfig) config, executorService);
                            consumerTaskMap.computeIfAbsent(topic, k -> new HashMap<>());
                            consumerTaskMap.get(topic).put(newBrokerServerUrl, consumerTask);
                            consumerTask.start();
                        }
                    }
                });
            }
        });
    }

    /**
     * 注册消费者
     * @param topic 订阅主题
     * @param clazz 消费类
     */
    public void register(String topic, Class<? extends Consumer> clazz) {
        if (!TOPIC_CONSUMER_CLASS_MAP.containsKey(topic)) {
            // 保存到消费者注册表
            TOPIC_CONSUMER_CLASS_MAP.put(topic, clazz);
            // 保存订阅主题
            topicList.add(topic);
        }
    }

    /**
     * 根据主题获取对应的消费者
     * @param topic 主题
     * @return
     */
    private Consumer getConsumer(String topic) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Consumer consumer = topicConsumerMap.get(topic);

        // 不存在则创建，并缓存到消费者表
        if (consumer == null) {
            Class<? extends Consumer> consumerClass = TOPIC_CONSUMER_CLASS_MAP.get(topic);
            if (consumerClass == null) {
                throw new ConsumerException(String.format("There is no registered consumer for %s topic", topic));
            }
            Constructor<? extends Consumer> constructor = consumerClass.getConstructor();
            consumer = constructor.newInstance();
            topicConsumerMap.put(topic, consumer);
            return consumer;
        }

        return consumer;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
