/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.coomia.datalake.benchmark;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hbase sink
 **/
public class FlinkHbaseSink extends RichSinkFunction<Map<String, Object>> {
  private static final long serialVersionUID = 727797000700012640L;
  private String zkQuorum = "zookeeper";
  private String zkPort = "2181";
  private String hbaseTableName;
  private String hbaseMaster;
  private String columnFamily = "cf";
  private Connection connection;
  private Admin admin;
  private Table table;

  public FlinkHbaseSink(String zkHost, String zkPort, String hbaseMaster, String hbaseTableName, String cf) {
    this.zkQuorum = zkHost;
    this.zkPort = zkPort;
    this.hbaseMaster = hbaseMaster;
    this.hbaseTableName = hbaseTableName;
    this.columnFamily = cf;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", zkQuorum);
    config.set("hbase.master", hbaseMaster);
    config.set("hbase.zookeeper.property.clientPort", zkPort);
    config.setInt("hbase.rpc.timeout", 20000);
    config.setInt("hbase.client.operation.timeout", 30000);
    config.setInt("hbase.client.scanner.timeout.period", 200000);
    //config.set("zookeeper.znode.parent", "/hbase");
    config.set("zookeeper.znode.parent", "/hbase-unsecure");
    connection = ConnectionFactory.createConnection(config);
    
    admin = connection.getAdmin();
    TableName tableName = TableName.valueOf(hbaseTableName);
    boolean tableExists = admin.tableExists(tableName);
    if (!tableExists) {
      admin.createTable(TableDescriptorBuilder.newBuilder((tableName))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily)).build());
    }
    table = connection.getTable(tableName);
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
          Bytes.toBytes(entry.getValue().toString()));
    }
    table.put(put);

  }
}
