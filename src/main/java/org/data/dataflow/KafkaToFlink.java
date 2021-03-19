package org.data.dataflow;


import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

// flink consume kafka data
public class KafkaToFlink {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment().setParallelism(1);
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "10.227.89.202:9092");
    properties.setProperty("group.id", "test-consumer-group");
    SingleOutputStreamOperator<String> dataStreamSource = streamExecutionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(
            "my-topic",
            new SimpleStringSchema(),
            properties).setStartFromEarliest()
        ).map((MapFunction<String, String>) s -> {
          Thread.sleep(ThreadLocalRandom.current().nextInt(0, 500));
          return s;
        });
    dataStreamSource.addSink(new FlinkToCK());
    streamExecutionEnvironment.execute("flink consume kafka topic");
  }
}