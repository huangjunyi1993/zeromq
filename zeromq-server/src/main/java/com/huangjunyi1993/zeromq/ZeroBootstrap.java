package com.huangjunyi1993.zeromq;

import com.huangjunyi1993.zeromq.async.Listener;
import com.huangjunyi1993.zeromq.async.ZeroBroadcaster;
import com.huangjunyi1993.zeromq.config.GlobalConfiguration;
import com.huangjunyi1993.zeromq.core.Handler;
import com.huangjunyi1993.zeromq.core.HandlerFactory;
import com.huangjunyi1993.zeromq.core.Interceptor;
import com.huangjunyi1993.zeromq.core.handler.ZeroHandlerFactory;
import com.huangjunyi1993.zeromq.remoting.NettyServer;
import com.huangjunyi1993.zeromq.task.AbstractTask;
import com.huangjunyi1993.zeromq.task.ConsumerOffsetSyncTask;
import com.huangjunyi1993.zeromq.util.ConsumerOffsetCache;
import com.huangjunyi1993.zeromq.util.FileUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 启动类
 * Created by huangjunyi on 2022/8/21.
 */
public class ZeroBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroBroadcaster.class);

    public static void main(String[] args) throws IOException {
        try {
            // 读取配置文件，初始化config
            GlobalConfiguration config = initConfiguration(args);

            // 注册handler和interceptor
            registerHandlerAndInterceptor();

            // 初始定时任务
            initTask();

            // 注册监听器
            registerListener();

            // 开启netty
            NettyServer.open();

            // 往zk注册该节点
            register(config);
        } catch (Exception e) {
            LOGGER.info("Startup failed:", e);
        }
    }

    private static void registerListener() {
        ServiceLoader<Listener> listenerServiceLoader = ServiceLoader.load(Listener.class);
        ZeroBroadcaster broadcaster = ZeroBroadcaster.getBroadcaster();
        for (Listener listener : listenerServiceLoader) {
            broadcaster.registerListener(listener);
        }
    }

    private static void initTask() {
        ServiceLoader<AbstractTask> taskServiceLoader = ServiceLoader.load(AbstractTask.class);
        for (AbstractTask task : taskServiceLoader) {
            task.start();
        }
    }

    private static GlobalConfiguration initConfiguration(String[] args) throws IOException {
        Properties properties = null;
        if (args.length > 0 && !"".equals(args[0])) {
            properties = new Properties();
            properties.load(Files.newInputStream(Paths.get(args[0])));
        }
        GlobalConfiguration config = GlobalConfiguration.init(properties);
        return config;
    }

    private static void registerHandlerAndInterceptor() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        HandlerFactory factory = ZeroHandlerFactory.getInstance();
        ServiceLoader<Handler> handlerServiceLoader = ServiceLoader.load(Handler.class);
        for (Handler handler : handlerServiceLoader) {
            factory.regiserHandler(handler);
        }
        ServiceLoader<Interceptor> interceptorServiceLoader = ServiceLoader.load(Interceptor.class);
        for (Interceptor interceptor : interceptorServiceLoader) {
            factory.regiserInterceptor(interceptor);
        }
    }

    private static void register(GlobalConfiguration config) throws Exception {
        CuratorFramework zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        zkCli.start();
        InterProcessLock lock = new InterProcessMutex(zkCli, "/lock/broker");
        try {
            while (!lock.acquire(10 * 1000, TimeUnit.SECONDS)) {}

            Stat stat = zkCli.checkExists().forPath("/brokers");
            if (stat == null) {
                zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/brokers", "".getBytes());
            }
            byte[] bytes = zkCli.getData().forPath("/brokers");
            Set<String> urlSet = new HashSet<>();
            String brokers = new String(bytes);
            if (!"".equals(brokers)) {
                if (brokers.contains(",")) {
                    Collections.addAll(urlSet, brokers.split(","));
                } else {
                    urlSet.add(brokers);
                }
            }
            urlSet.add(getHostIpAndPort(config));
            brokers = urlSet.stream().collect(Collectors.joining(","));
            zkCli.setData().forPath("/brokers", brokers.getBytes());
        } finally {
            lock.release();
        }
    }

    private static String getHostIpAndPort(GlobalConfiguration config) {
        return getHostIp() + ":" + config.getPort();
    }

    private static String getHostIp(){
        try{
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()){
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()){
                    InetAddress ip = (InetAddress) addresses.nextElement();
                    if (ip != null
                            && ip instanceof Inet4Address
                            && !ip.isLoopbackAddress() //loopback地址即本机地址，IPv4的loopback范围是127.0.0.0 ~ 127.255.255.255
                            && ip.getHostAddress().indexOf(":")==-1){
                        System.out.println("本机的IP = " + ip.getHostAddress());
                        return ip.getHostAddress();
                    }
                }
            }
        }catch(Exception e){
            LOGGER.info("get host ip failed: ", e);
        }
        return null;
    }

}
