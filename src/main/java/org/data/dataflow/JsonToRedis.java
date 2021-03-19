package org.data.dataflow;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//pass file data to redis
public class JsonToRedis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    // get flink data stream
    DataStreamSource<String> datastream = streamExecutionEnvironment
        .readTextFile("dataset/dataset.json").setParallelism(1);
    // for loop can not traverse datastream, it's unbound
    SingleOutputStreamOperator<JSONObject> parsed = datastream.map(
        (MapFunction<String, JSONObject>) JSON::parseObject);
    // get jedis conf
    FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
    //for sending data to Redis.
    parsed.addSink(new RedisSink<JSONObject>(conf, new RedisSinkFunc()));
    streamExecutionEnvironment.execute("JSON to Redis");
  }
}

class RedisSinkFunc implements RedisMapper<JSONObject> {

  @Override
  public RedisCommandDescription getCommandDescription() {
    return new RedisCommandDescription(RedisCommand.HSET, "pageJSON");
  }

  @Override
  public String getKeyFromData(JSONObject jsonObject) {
    return jsonObject.getInteger("page").toString();
  }

  @Override
  public String getValueFromData(JSONObject jsonObject) {
    return jsonObject.toString();
  }
}
