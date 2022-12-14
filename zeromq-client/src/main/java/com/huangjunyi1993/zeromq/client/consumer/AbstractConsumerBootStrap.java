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
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.*;
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

    // 主题与服务器的映射表 topic => brokerUrls
    private Map<String, List<BrokerServerUrl>> topicBrokerUrlsMap = new ConcurrentHashMap<>();

    // 当前连接的brokers
    private Set<BrokerServerUrl> connectBrokers = new HashSet<>();

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

        this.executorService = ThreadPoolGenerator.newConsumerThreadPool(config.getCorePoolSize(), config.getKeepAliveTime(), THREAD_NAME);
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
        InterProcessLock lock = new InterProcessMutex(this.zkCli, ZK_PATH_CONSUMER_LOCK);
        try {

            // 在zk上消费者注册节点 /zero/consumer/{groupId}/{consumerId}
            registerConsumerNodeOnZK((ConsumerConfig) this.config);

            // 自旋检查/zero/brokers节点是否存在
            Stat brokersNodeStat = zkCli.checkExists().forPath(ZK_PATH_BROKERS);
            while (brokersNodeStat == null) {
                LockSupport.parkNanos(1000 * 1000 * 1000L);
                brokersNodeStat = zkCli.checkExists().forPath(ZK_PATH_BROKERS);
            }

            // 自旋获取锁
            while (!lock.acquire(10, TimeUnit.SECONDS)) {}

            // 从zk获取服务器信息
            List<String> brokers = null;
            while (brokers == null || brokers.size() == 0) {
                brokers = zkCli.getChildren().forPath(ZK_PATH_BROKERS);
            }

            // 解析服务器信息
            List<BrokerServerUrl> brokerServerUrlList = ClientUtil.parseBrokerServerUrls(brokers);
            ConsumerConfig consumerConfig = (ConsumerConfig) this.config;

            // 消费者rebalance（分配各自消费的broker）
            rebalance(topicLiSt, brokerServerUrlList, consumerConfig, true);

        } catch (Exception e) {
            throw new ConsumerException("The consumer launch failed", e);
        } finally {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        }

        // 睡5秒，等待同时启动的其他消费者rebalance
        LockSupport.parkNanos(5 * 1000 * 1000 * 1000L);

        // 监听/zero/brokers节点，触发更新
        addWatcherBrokers();

        // 监听/zero/consumers/{groupId}，触发更新
        addWatcherConsumers((ConsumerConfig) this.config);

        // 创建并运行消费任务
        runTask();

        // 修改运行标志 当前正在运行
        isRunning = true;

        // 启动定时任务，检查没有消费任务的连接
        startCheckTask();

        return true;
    }

    private void startCheckTask() {
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(this::closeClientIfNoTask, 5, 5, TimeUnit.SECONDS);
    }

    private void registerConsumerNodeOnZK(ConsumerConfig config) throws Exception {
        // /zero/consumer/{groupId}/{consumerId}
        String path = String.format(ZK_PATH_CONSUMER + "/%s", config.getGroupId(), config.getConsumerId());
        Stat stat = zkCli.checkExists().forPath(path);
        if (stat == null) {
            zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
        }
    }

    private List<String> getAllConsumerFromZK(ConsumerConfig config) throws Exception {
        List<String> consumers = new ArrayList<>();
        // /zero/consumer/{groupId}
        String path = String.format(ZK_PATH_CONSUMER, config.getGroupId());
        Stat stat = zkCli.checkExists().forPath(path);
        if (stat != null) {
            List<String> groupChildrens = zkCli.getChildren().forPath(path);
            consumers.addAll(groupChildrens);
        }
        return consumers;
    }

    private void rebalance(List<String> topicLiSt, List<BrokerServerUrl> brokerServerUrlList, ConsumerConfig consumerConfig, boolean addWatcher) {
        // 循环遍历所有订阅的topic，进行rebalance
        Map<String, String> topicResultMap = new HashMap<>();
        topicLiSt.forEach(topic -> {

            // zk：/zero/topic/{topicName}/consumers/{groupId}
            String path = String.format(ZK_PATH_CONSUMER_TOPIC, topic, consumerConfig.getGroupId());
            try {

                Stat stat = zkCli.checkExists().forPath(path);
                if (stat == null) {
                    // 节点路径不存在，创建
                    zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
                }

                // 注册到zk上的所有消费者id列表
                List<String> consumerIdList = getAllConsumerFromZK(consumerConfig);

                // 消费者id列表不包含当前消费者，添加
                if (!consumerIdList.contains(String.valueOf(consumerConfig.getConsumerId()))) {
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
            String path = String.format(ZK_PATH_CONSUMER_TOPIC, topic, consumerConfig.getGroupId());
            try {
                // 1、循环推到zk
                zkCli.setData().forPath(path, result.getBytes(StandardCharsets.UTF_8));
                // 2、更新双层映射表
                updateTopicConsumerIdUrlsMap(topic, result);
                // 3、添加监听，后面有其他消费者加入，触发更新双层映射表
                if (addWatcher) {
                    addWatcherTopic(topic, path);
                }
            } catch (Exception e) {
                throw new ConsumerException("Failed to update the consumer group node data", e);
            }
        });
    }

    protected void runTask() {
        // 创建任务 建立连接 添加到任务表中 启动任务
        topicBrokerUrlsMap.forEach((topic, brokerServerUrls) -> {
            brokerServerUrls.forEach(brokerServerUrl -> {
                createAndStartConsumerTask(topic, brokerServerUrl);
            });
        });

    }

    private void createAndStartConsumerTask(String topic, BrokerServerUrl brokerServerUrl) {
        ConsumerTask consumerTask = new ConsumerTask(NettyClient.getChannel(config, brokerServerUrl), brokerServerUrl, topic, (ConsumerConfig) config, executorService);
        consumerTaskMap.computeIfAbsent(topic, k -> new HashMap<>());
        consumerTaskMap.get(topic).put(brokerServerUrl, consumerTask);
        connectBrokers.add(brokerServerUrl);
        consumerTask.start();
    }

    /**
     * 对指定主题和group的路径添加监听
     * @param topic 订阅的主题
     * @param path zk上指定topic和group的路径
     * @throws Exception
     */
    private void addWatcherTopic(String topic, String path) throws Exception {
        // 监听当前topic下当前group分配的变动，触发更新
        NodeCache nodeCache = new NodeCache(this.zkCli, path);
        nodeCache.start();
        nodeCache.getListenable().addListener(() -> {
            if (nodeCache.getCurrentData() != null) {
                byte[] bytes = nodeCache.getCurrentData().getData();
                if (bytes != null && bytes.length != 0) {
                    updateTopicConsumerIdUrlsMap(topic, new String(bytes, StandardCharsets.UTF_8));
                }
            }
        });
    }

    /**
     * 对/brokers节点添加监听，当新的broker上线时，触发rebalance
     * @throws Exception
     */
    private void addWatcherBrokers() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(this.zkCli, ZK_PATH_BROKERS, true);
        cache.start();
        cache.getListenable().addListener((client, event) -> tryLockAndRebalance());
    }

    /**
     * 监听/zero/consumer/{groupId}，当组内有消费者上下线时，触发rebalance
     * @param config
     */
    private void addWatcherConsumers(ConsumerConfig config) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(this.zkCli, String.format(ZK_PATH_CONSUMER, config.getGroupId()), true);
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener((client, event) -> tryLockAndRebalance());
    }

    private void tryLockAndRebalance() throws Exception {
        // 分布式锁，防止消费者客户端并发rebalance
        InterProcessLock lock = new InterProcessMutex(this.zkCli, ZK_PATH_CONSUMER_LOCK);

        try {
            // 获取锁成功才操作，只有一个消费者成功获取锁，进行rebalance
            if (lock.acquire(1, TimeUnit.SECONDS)) {
                // 当前消费者客户端所有要订阅的主题
                List<String> topicLiSt = this.topicList;

                // 解析服务器信息
                List<String> brokers = zkCli.getChildren().forPath(ZK_PATH_BROKERS);
                List<BrokerServerUrl> brokerServerUrlList = ClientUtil.parseBrokerServerUrls(brokers);

                ConsumerConfig consumerConfig = (ConsumerConfig) this.config;

                rebalance(topicLiSt, brokerServerUrlList, consumerConfig, false);

            }
        } finally {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        }
    }

    /**
     * 关闭已经没有消费者任务的客户端
     */
    private void closeClientIfNoTask() {
        // 统计当前消费者连接的broker，各有多少任务数，记到brokerTaskCountMap中
        HashMap<BrokerServerUrl, Integer> brokerTaskCountMap = new HashMap<>();
        Map<String, Map<BrokerServerUrl, ConsumerTask>> consumerTaskMap = this.consumerTaskMap;
        for (Map<BrokerServerUrl, ConsumerTask> brokerTaskMap : consumerTaskMap.values()) {
            Set<BrokerServerUrl> brokerServerUrls = brokerTaskMap.keySet();
            for (BrokerServerUrl brokerServerUrl : brokerServerUrls) {
                if (brokerTaskCountMap.containsKey(brokerServerUrl)) {
                    brokerTaskCountMap.put(brokerServerUrl, brokerTaskCountMap.get(brokerServerUrl) + 1);
                } else {
                    brokerTaskCountMap.put(brokerServerUrl, 1);
                }
            }
        }

        // 已经没有消费任务的broker，关闭客户端
        Set<BrokerServerUrl> connectBrokers = this.connectBrokers;
        List<BrokerServerUrl> removeBrokers = new ArrayList<>();
        for (BrokerServerUrl connectBroker : connectBrokers) {
            if (!brokerTaskCountMap.containsKey(connectBroker)) {
                removeBrokers.add(connectBroker);
            }
        }
        connectBrokers.removeAll(removeBrokers);
        NettyClient.shutDown(removeBrokers);
    }


    private void updateTopicConsumerIdUrlsMap(String topic, String data) {
        // 创建外围映射表
        topicBrokerUrlsMap.computeIfAbsent(topic, k -> new ArrayList<>());

        // 解析当前topic的rebalance结果
        JsonElement jsonElement = new JsonParser().parse(data);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        Set<String> consumerIds = jsonObject.keySet();

        // 如果rebalance后的结果，没有分配到当前消费者，停止该消费者对该topic所有broker的消费
        if (consumerIds == null || consumerIds.size() == 0 || !consumerIds.contains(((ConsumerConfig)config).getConsumerId())) {
            Map<BrokerServerUrl, ConsumerTask> brokerServerUrlConsumerTaskMap = consumerTaskMap.get(topic);
            if (brokerServerUrlConsumerTaskMap != null) {
                for (Map.Entry<BrokerServerUrl, ConsumerTask> entry : brokerServerUrlConsumerTaskMap.entrySet()) {
                    ConsumerTask consumerTask = entry.getValue();
                    consumerTask.shutdown();
                }
                // 从消费者任务表删除
                consumerTaskMap.remove(topic);
                // 从映射表中删除
                topicBrokerUrlsMap.remove(topic);
            }
        }

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
                // 当前正在运行（如果当前正在启动 false） && 当前遍历到的consumerId是自己
                if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId)) {
                    // 不再分配给自己的服务器
                    List<BrokerServerUrl> diffBrokerServerUrls = new ArrayList<>();
                    // 原来由自己负责的服务器
                    List<BrokerServerUrl> brokerServerUrls = topicBrokerUrlsMap.get(topic);
                    if (brokerServerUrls == null) {
                        brokerServerUrls = new ArrayList<>();
                        topicBrokerUrlsMap.put(topic, brokerServerUrls);
                    }
                    brokerServerUrls = brokerServerUrls.stream().filter(brokerServerUrl -> {
                        if (newBrokerServerUrls.contains(brokerServerUrl)) {
                            return true;
                        }
                        diffBrokerServerUrls.add(brokerServerUrl);
                        return false;
                    }).collect(Collectors.toList());
                    // 依然由自己负责的服务器，放回内层映射表
                    topicBrokerUrlsMap.put(topic, brokerServerUrls);

                    // 不再分配给自己的服务器，移除消费任务，并停止
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
                    List<BrokerServerUrl> brokerServerUrls = topicBrokerUrlsMap.get(topic);
                    if (brokerServerUrls == null) {
                        brokerServerUrls = new ArrayList<>();
                        topicBrokerUrlsMap.put(topic, brokerServerUrls);
                    }
                    if (!brokerServerUrls.contains(newBrokerServerUrl)){
                        // 由自己负责的服务器，内层映射表没有，添加
                        brokerServerUrls.add(newBrokerServerUrl);
                    }

                    // 当前正在运行（如果当前正在启动 false） && 当前遍历到的consumerId是自己
                    if (isRunning && ((ConsumerConfig)config).getConsumerId() == Integer.valueOf(consumerId)) {
                        // 添加新的消费任务，建立连接，并启动消费任务
                        createAndStartConsumerTask(topic, newBrokerServerUrl);
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
