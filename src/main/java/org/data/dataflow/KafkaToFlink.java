package org.data.dataflow;


import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

// flink consume kafka data
public class KafkaToFlink {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment().setParallelism(1);
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "10.227.89.202:9092");
    properties.setProperty("group.id", "test-consumer-group");
    SingleOutputStreamOperator<String> dataStreamSource = streamExecutionEnvironment
        .addSource(new FlinkKafkaConsumer<>(
            "my-topic",
            new SimpleStringSchema(),
            properties).setStartFromEarliest()
        ).map((MapFunction<String, String>) s -> {
          Thread.sleep(ThreadLocalRandom.current().nextInt(0, 10));
          return s;
        });
    dataStreamSource.addSink(new FlinkToCK("82.157.179.4", 8123 + "", "default", "123"));
    streamExecutionEnvironment.execute("flink consume kafka topic");
  }
}

class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<String> {

  @Override
  public boolean isEndOfStream(String s) {
    return false;
  }

  @Override
  public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    return consumerRecord.toString();
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }
}