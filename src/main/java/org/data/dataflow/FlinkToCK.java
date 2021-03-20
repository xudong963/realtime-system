package org.data.dataflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

class FlinkToCK extends RichSinkFunction<String> {

  FlinkToCK(String host, String port, String user, String pwd) throws SQLException {
    super();
    this.connection_ = DriverManager.getConnection("jdbc:clickhouse://" + host + ":" + port, user, pwd);
    this.statement_ = connection_.createStatement();
    statement_.execute("CREATE DATABASE IF NOT EXISTS test");
  }

  @Override
  public void invoke(String value, SinkFunction.Context context) throws SQLException {
    statement_.execute("INSERT INTO test VALUES ('${value}')");
  }

  public void close() throws SQLException {
    connection_.close();
  }

  private final Connection connection_;
  private final Statement statement_;
}