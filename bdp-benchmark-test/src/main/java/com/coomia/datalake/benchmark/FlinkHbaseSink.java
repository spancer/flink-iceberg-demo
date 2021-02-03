package com.coomia.datalake.benchmark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * Hbase sink
 **/
public class FlinkHbaseSink extends RichSinkFunction<Map<String, Object>> {
  private static final long serialVersionUID = 727797000700012640L;
  private String zkQuorum = "zookeeper";
  private String zkPort = "2181";
  private int hbasePort = 6000;
  private TableName hbaseTableName;
  private String columnFamily = "cf";
  private Connection connection;
  private Admin admin;
  private Table table;

  public FlinkHbaseSink(HBaseConfig hbaseConfig) {
    this.zkQuorum = hbaseConfig.getZkHost();
    this.zkPort = hbaseConfig.getZkPort();
    this.hbaseTableName = TableName.valueOf(hbaseConfig.getTable());
    this.columnFamily = hbaseConfig.getCf();
    this.hbasePort = hbaseConfig.getHbasePort();
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", zkQuorum);
    config.set("hbase.master", zkQuorum + ":" + hbasePort);
    config.set("hbase.zookeeper.property.clientPort", zkPort);
    config.setInt("hbase.rpc.timeout", 20000);
    config.setInt("hbase.client.operation.timeout", 30000);
    config.setInt("hbase.client.scanner.timeout.period", 200000);
    connection = ConnectionFactory.createConnection(config);
    admin = connection.getAdmin();
    boolean tableExists = admin.tableExists(hbaseTableName);
    if (!tableExists) {
      admin.createTable(TableDescriptorBuilder.newBuilder((hbaseTableName))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily)).build());
    }
    table = connection.getTable(hbaseTableName);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (connection != null) {
      connection.close();
    }
    if (table != null)
      table.close();
    if (admin != null)
      admin.close();
    connection.close();
  }

  @Override
  public void invoke(Map<String, Object> value, Context context) throws Exception {
    long timeMillis = System.currentTimeMillis();
    Put put = new Put(Bytes.toBytes(timeMillis));
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      String field = entry.getKey();
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field),
          Bytes.toBytes((String) entry.getValue()));
    }
    table.put(put);

  }
}
