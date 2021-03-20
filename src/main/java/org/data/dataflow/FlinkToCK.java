package org.data.dataflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

class FlinkToCK extends RichSinkFunction<String> {

  FlinkToCK(String host, String port, String user, String pwd) {
    super();
    host_ = host;
    port_ = port;
    user_ = user;
    pwd_ = pwd;
  }

  @Override
  public void invoke(String value, SinkFunction.Context context) throws SQLException {
    statement_.execute("INSERT INTO test VALUES ('${value}')");
  }

  public void close() throws SQLException {
    connection_.close();
  }

  @Override
  public void open(Configuration param) throws SQLException {
    this.connection_ = DriverManager.getConnection("jdbc:clickhouse://" + host_ + ":" + port_, user_, pwd_);
    this.statement_ = connection_.createStatement();
    statement_.execute("CREATE DATABASE IF NOT EXISTS test");
  }

  private Connection connection_;
  private Statement statement_;
  private final String host_;
  private final String port_;
  private final String user_;
  private final String pwd_;
}