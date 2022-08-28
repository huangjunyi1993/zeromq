1、通过Netty处理网络通信，进行消息的收发，以协议包protocal的形式序列化进行传输，通过序列化器工厂和默认的两个序列化器jdk和hessian2

2、通过CAS+内存映射处理并发读写，消息日志文件进行顺序写，构建消息日志索引

3、broker启动时先获取zk分布式锁后，更新zk上/brokers节点的数据（拼接上自己的host和port）

4、生产者发送消息时，先从Zookeeper读取/brokers节点的信息，缓存到本地，经过路由选择一个broker送到消息。并且会在/brokers注册监听器，监听节点变化更新自身缓存

5、消费者启动时，根据自身要消费的topic，获取zk分布式锁，然后读取/brokers节点和/topic/${topicName}/consumers/${groupId}节点的信息，根据算法进行重新分配，然后更新topic/${topicName}/consumers/${groupId}节点的信息，消费者归属某个消费者组，通过消费者组id区分

6、消费者消费完成，返回ack，Broker更新消费者消费偏移量文件，文件记录各消费者组topic对应的消费偏移量，文件名默认为${topic}_${groupId}.txt
