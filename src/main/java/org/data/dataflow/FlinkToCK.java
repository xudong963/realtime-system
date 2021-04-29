package org.data.dataflow;

import com.alibaba.fastjson.JSONObject;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
  public void invoke(String value, Context context) throws SQLException {
    // ods table
    String ods_subway_data_sql = "INSERT INTO ods_subway_data (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement statement = connection_.prepareStatement(ods_subway_data_sql);
    Map<String, String> mapVal = new Gson().fromJson(
        value, new TypeToken<HashMap<String, String>>() {
        }.getType()
    );
    statement.setString(1, mapVal.get("deal_date"));
    statement.setString(2, mapVal.get("close_date"));
    statement.setString(3, mapVal.get("card_no"));
    statement.setString(4, mapVal.get("deal_value"));
    statement.setString(5, mapVal.get("deal_type"));
    statement.setString(6, mapVal.get("company_name"));
    statement.setString(7, mapVal.get("car_no"));
    statement.setString(8, mapVal.get("station"));
    statement.setString(9, mapVal.get("conn_mark"));
    statement.setString(10, mapVal.get("deal_money"));
    statement.setString(11, mapVal.get("equ_no"));
    statement.addBatch();
    statement.executeBatch();

    // dwd table (对不合理的数据进行过滤)
    String dwd_subway_data_sql =
        "INSERT INTO dwd_subway_data (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no) SELECT "
            + "deal_date,"
            + "close_date,"
            + "card_no,"
            + "deal_value,"
            + "deal_type,"
            + "company_name,"
            + "car_no,"
            + "station,"
            + "conn_mark,"
            + "deal_money,"
            + "equ_no "
            + "FROM ods_subway_data "
            + "WHERE deal_type != '巴士'";
    statement = connection_.prepareStatement(dwd_subway_data_sql);
    statement.execute();

    // dwd in station table
    String dwd_in_station_data_sql =
        "INSERT INTO dwd_in_station_data (deal_date, card_no, deal_type, company_name, car_no, station, equ_no) SELECT "
            + "deal_date,"
            + "card_no,"
            + "deal_type,"
            + "company_name,"
            + "car_no,"
            + "station,"
            + "equ_no "
            + "FROM dwd_subway_data "
            + "WHERE deal_type == '地铁入站'";
    statement = connection_.prepareStatement(dwd_in_station_data_sql);
    statement.execute();
    // dwd out station table
    String dwd_out_station_data_sql =
        "INSERT INTO dwd_out_station_data (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no) SELECT "
            + "deal_date,"
            + "close_date,"
            + "card_no,"
            + "deal_value,"
            + "deal_type,"
            + "company_name,"
            + "car_no,"
            + "station,"
            + "conn_mark,"
            + "deal_money,"
            + "equ_no "
            + "FROM dwd_subway_data "
            + "WHERE deal_type == '地铁出站'";
    statement = connection_.prepareStatement(dwd_out_station_data_sql);
    statement.execute();
  }

  public void close() throws SQLException {
    connection_.close();
  }

  @Override
  public void open(Configuration param) throws SQLException {
    this.connection_ = DriverManager
        .getConnection("jdbc:clickhouse://" + host_ + ":" + port_, user_, pwd_);
    // ods table
    this.connection_.createStatement().execute("DROP TABLE IF EXISTS ods_subway_data");
    connection_.createStatement().execute("CREATE TABLE IF NOT EXISTS ods_subway_data (" +
        "deal_date Nullable(String),"
        + "close_date Nullable(String),"
        + "card_no Nullable(String),"
        + "deal_value Nullable(String),"
        + "deal_type Nullable(String),"
        + "company_name Nullable(String),"
        + "car_no Nullable(String),"
        + "station Nullable(String),"
        + "conn_mark Nullable(String),"
        + "deal_money Nullable(String),"
        + "equ_no Nullable(String)"
        + ") ENGINE = Memory()"
    );
    // dwd table (对不合理的数据进行过滤)
    this.connection_.createStatement().execute("DROP TABLE IF EXISTS dwd_subway_data");
    connection_.createStatement().execute("CREATE TABLE IF NOT EXISTS dwd_subway_data (" +
        "deal_date Nullable(String),"
        + "close_date Nullable(String),"
        + "card_no Nullable(String),"
        + "deal_value Nullable(String),"
        + "deal_type Nullable(String),"
        + "company_name Nullable(String),"
        + "car_no Nullable(String),"
        + "station Nullable(String),"
        + "conn_mark Nullable(String),"
        + "deal_money Nullable(String),"
        + "equ_no Nullable(String)"
        + ") ENGINE = Memory()"
    );
    // dwd in station table
    this.connection_.createStatement().execute("DROP TABLE IF EXISTS dwd_in_station_data");
    connection_.createStatement().execute("CREATE TABLE IF NOT EXISTS dwd_in_station_data (" +
        "deal_date Nullable(String),"
        + "card_no Nullable(String),"
        + "deal_type Nullable(String),"
        + "company_name Nullable(String),"
        + "car_no Nullable(String),"
        + "station Nullable(String),"
        + "equ_no Nullable(String)"
        + ") ENGINE = Memory()"
    );
    // dwd out station table
    this.connection_.createStatement().execute("DROP TABLE IF EXISTS dwd_out_station_data");
    connection_.createStatement().execute("CREATE TABLE IF NOT EXISTS dwd_out_station_data (" +
        "deal_date Nullable(String),"
        + "close_date Nullable(String),"
        + "card_no Nullable(String),"
        + "deal_value Nullable(String),"
        + "deal_type Nullable(String),"
        + "company_name Nullable(String),"
        + "car_no Nullable(String),"
        + "station Nullable(String),"
        + "conn_mark Nullable(String),"
        + "deal_money Nullable(String),"
        + "equ_no Nullable(String)"
        + ") ENGINE = Memory()"
    );

    //

    //

    //

    //
  }

  private Connection connection_;
  private final String host_;
  private final String port_;
  private final String user_;
  private final String pwd_;
}