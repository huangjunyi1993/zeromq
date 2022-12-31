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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.ZK_PATH_BROKERS;

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
        // SPI机制，注册监听器
        ServiceLoader<Listener> listenerServiceLoader = ServiceLoader.load(Listener.class);
        ZeroBroadcaster broadcaster = ZeroBroadcaster.getBroadcaster();
        for (Listener listener : listenerServiceLoader) {
            broadcaster.registerListener(listener);
        }
    }

    private static void initTask() {
        // SPI机制，注册定时任务
        ServiceLoader<AbstractTask> taskServiceLoader = ServiceLoader.load(AbstractTask.class);
        for (AbstractTask task : taskServiceLoader) {
            task.start();
        }
    }

    private static GlobalConfiguration initConfiguration(String[] args) throws IOException {
        Properties properties = null;
        if (args.length > 0 && !"".equals(args[0])) {
            properties = new Properties();
            // 加载配置文件
            properties.load(Files.newInputStream(Paths.get(args[0])));
        }
        // 初始化全局配置
        GlobalConfiguration config = GlobalConfiguration.init(properties);
        return config;
    }

    private static void registerHandlerAndInterceptor() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // 获取处理器工厂单例
        HandlerFactory factory = ZeroHandlerFactory.getInstance();
        // SPI机制，注册处理器
        ServiceLoader<Handler> handlerServiceLoader = ServiceLoader.load(Handler.class);
        for (Handler handler : handlerServiceLoader) {
            factory.regiserHandler(handler);
        }
        // SPI机制，注册拦截器链
        ServiceLoader<Interceptor> interceptorServiceLoader = ServiceLoader.load(Interceptor.class);
        for (Interceptor interceptor : interceptorServiceLoader) {
            factory.regiserInterceptor(interceptor);
        }
    }

    /**
     * 注册服务器节点到zk
     * @param config
     * @throws Exception
     */
    private static void register(GlobalConfiguration config) throws Exception {
        // 创建并启动zk客户端
        CuratorFramework zkCli = CuratorFrameworkFactory.newClient(config.getZkUrl(), new ExponentialBackoffRetry(5000, 30));
        zkCli.start();
        String path = String.format(ZK_PATH_BROKERS + "/%s", getHostIpAndPort(config));
        // 创建服务器注册路径是否存在，不存在则创建
        Stat stat = zkCli.checkExists().forPath(path);
        if (stat != null) {
            // 上次启动在zk上的缓存还在，先清除
            zkCli.delete().forPath(path);
            // 睡3秒，等待消费者清除旧的连接
            LockSupport.parkNanos(3 * 1000 * 1000 * 1000L);
        }
        zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
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
