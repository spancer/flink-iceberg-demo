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
package com.coomia.datalake.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkSQLHiveWriter {


  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner() // 使用BlinkPlanner
        .inBatchMode() // Batch模式，默认为StreamingMode
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String catalogName = "myhive";
    String defaultDatabase = "tpcds_text_3"; // 默认数据库名称
    String hiveConfDir = "src/main/resources"; // hive-site.xml路径

    HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tableEnv.registerCatalog(catalogName, hive);
    tableEnv.useCatalog(catalogName);
    // set sql dialect as default, means using flink sql.
    tableEnv.useDatabase(defaultDatabase);
    long mill = System.currentTimeMillis();
    String tableName = "event_source_" + mill;
    // is generic = false, means we can execute query on hive too.
    tableEnv.executeSql("CREATE TABLE " + tableName
        + "  (    uid STRING,    eventid STRING,    uuid STRING, ts BIGINT) WITH (    'connector.type' = 'kafka',    'connector.version' = 'universal',    'connector.topic' = 'event',    'connector.startup-mode' = 'earliest-offset',    'connector.properties.zookeeper.connect' = 'itserver21:2181',    'connector.properties.bootstrap.servers' = 'itserver21:6667',    'format.type' = 'json', 'is_generic'='false')");

    String sql = "select count(*) from " + tableName;
    System.out.println(sql);
    tableEnv.executeSql(sql).print();

    String sinkTable = "event_sink_" + mill;
    // hive supported sql dialect, so as to query in hive client.
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tableEnv.executeSql("CREATE TABLE " + sinkTable
        + " (uid STRING,    eventid STRING,    uuid STRING,    eventTime BIGINT,proctime BIGINT) PARTITIONED BY (dt STRING, hr STRING) STORED AS ORC TBLPROPERTIES (  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',  'sink.partition-commit.trigger'='partition-time',  'sink.partition-commit.delay'='1 min',  'sink.partition-commit.policy.kind'='metastore,success-file')");

    tableEnv.executeSql(
        "INSERT INTO TABLE " + sinkTable + " SELECT uid,eventid, uuid,ts FROM " + tableName);

    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    sql = "select * from " + sinkTable + " limit 10";
    System.out.println(sql);
    tableEnv.executeSql(sql).print();

  }
}
