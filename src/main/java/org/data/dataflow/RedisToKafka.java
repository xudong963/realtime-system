package org.data.dataflow;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

// kafka read redis data
public class RedisToKafka {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment().setParallelism(1);
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "10.227.89.202:9092");
    SingleOutputStreamOperator<String> dataStreamSource = streamExecutionEnvironment.addSource(new FilterRedisSource()).map(
        (MapFunction<String, String>) s -> {
          //Simulate real-time state
          Thread.sleep(ThreadLocalRandom.current().nextInt(0, 10));
          return s;
        });
    FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
      "my-topic",
      new ProducerStringSerializationSchema("my-topic"),
      properties,
      Semantic.EXACTLY_ONCE);
    dataStreamSource.addSink(producer);
    streamExecutionEnvironment.execute("Redis To Kafka");
  }
}


// filter invalid data, then sink to kafka
class FilterRedisSource extends RichSourceFunction<String> {
  final int PAGECNT = 1337;
  final int ELESIZE = 11;
  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    // Jedis is the official recommended Java connection development tool for Redis
    jedis_ = getJedisClient();
    for(int i = 1; i <= PAGECNT; ++i) {
      String value = jedis_.hget("pageJSON", i + "");
      // System.out.println(value);
      JSONObject jsonVal = JSON.parseObject(value);
      // {"data": [{}, {}, {}]}
      JSONArray dataArray = jsonVal.getJSONArray("data");
      for (Object element: dataArray) {
        String val = element.toString();
        JSONObject jVal = JSON.parseObject(val);
        if(jVal.size() == ELESIZE) {
          sourceContext.collect(val);
        }
      }
    }
  }

  @Override
  public void cancel() {
    jedis_.close();
  }

  private Jedis getJedisClient() {
    JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);
    return jedisPool.getResource();
  }

  private Jedis jedis_;
}

class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {

  private final String topic_;

  public ProducerStringSerializationSchema(String topic) {
    super();
    this.topic_ = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
    return new ProducerRecord<byte[], byte[]>(topic_, element.getBytes(StandardCharsets.UTF_8));
  }

}