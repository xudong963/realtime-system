# realtime-system
我的毕业设计

### 数据集
- deal_date: 交易(进站)时间
- close_date: 结算时间
- card_no: 卡号
- deal_value: 交易金额
- deal_type: 交易类型 (地铁入站 or 地铁出站 or 巴士)
- company_name: 地铁线名
- car_no: 车号
- station: 站名
- conn_mark: 联程标记
- deal_money: 交易金额
- equ_no: 闸机号

### data flow
> json->redis->kafka->flink->clickhouse->fontend show

#### json2redis[done]
- 对数据进行预处理
- 暂定为去重+排序
#### redis2kafka[done]
- 将处理后的数据导入 kafka 中

#### kafka2flink[done]
- flink 从 kafka 中 消费数据，模拟实时数据流

#### flink2ck[done]
- 数据最终流向 clickhouse

#### ck2show
- 可视化备选： Datav, EasyV, FineReport

#### reference
深入理解kafka
flink 文档
clickhouse 文档

### 启动命令

#### redis
1. redis-server
2. redis-cli
3. hgetall pageJSON

#### kafka & kafka-eagle & zk
1. 启动 zk: zkServer.sh start
2. 查看 zk 的状态：zkServer.sh status
3. zk 的 port 是 2181
4. kafka server.properties 中需要关注 **broker.id**, **listeners**, **log.dirs**, **zookeeper.connect**
5. 启动 kafka: kafka-server-start.sh ../config/server.properties&   & 代表 kafka 后台进行
6. jps 可以查看kafka 进程是否启动
7. kafka-eagle 启动后 可以进行登陆，账号和密码分别是： admin 123456..
8. 清空kafka topic: kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name my-topic --add-config retention.ms=1000
9. 上面的清空操作是通过修改 retention 时间实现的，现在需要重新修改回来 retention: ./kafka-topics.sh --zookeeper localhost:2181 --alter --topic my-topic --delete-config retention.ms
10. 查看 retention 时间是多少：  grep -i 'log.retention.[hms].*\=' config/server.properties   
11. 查看 kafka topic 信息： kafka-topics.sh --zookeeper localhost:2181 --describe --topics-with-overrides
12. 启动redis2kafka

#### 腾讯云
1. ssh root@82.157.179.4
2. 密码： zsy123456..

#### clickhouse
1. config: sudo vi /etc/clickhouse-server/config.xml
2. 启动： sudo /etc/init.d/clickhouse-server start

### TODO
1. 论文去重 + 排版 + 添加参考文献
2. easyV
