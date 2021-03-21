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


