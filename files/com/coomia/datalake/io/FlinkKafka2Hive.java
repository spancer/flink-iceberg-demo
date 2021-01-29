/*******************************************************************************
 * Copyright (c) 2015-2021 Coomia Network Technology Co., Ltd. All Rights Reserved.
 * <p>
 * This software is licensed not sold. Use or reproduction of this software by any unauthorized
 * individual or entity is strictly prohibited. This software is the confidential and proprietary
 * information of Coomia Network Technology Co., Ltd. Disclosure of such confidential information
 * and shall use it only in accordance with the terms of the license agreement you entered into with
 * Coomia Network Technology Co., Ltd.
 * <p>
 * Coomia Network Technology Co., Ltd. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
 * OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. Coomia Network
 * Technology Co., Ltd. SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF
 * USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ANY DERIVATIVES THEREOF.
 *******************************************************************************/
// Created on 2021年1月14日

package com.coomia.flink.demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author spancer
 *
 */
public class FlinkKafka2Hive {

  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hive");

    /*
     * EnvironmentSettings settings =
     * EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
     * StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
     * StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, settings);
     */
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner() // 使用BlinkPlanner
        .inBatchMode() // Batch模式，默认为StreamingMode
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String catalogName = "myhive"; // Catalog名称，定义一个唯一的名称表示
    String defaultDatabase = "tpcds_text_10"; // 默认数据库名称
    String hiveConfDir =
        "E:\\Coomia\\eclipse-workspace\\workspace-2020-coomia\\flink-iceberg-demo\\src\\main\\resources"; // hive-site.xml路径

    HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog(catalogName, hive);
    tableEnv.useCatalog(catalogName);
    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tableEnv.useDatabase(defaultDatabase);
    String tableName = "event" + System.currentTimeMillis();
    // is generic = false, means we can execute query on hive too.
    tableEnv.executeSql("CREATE TABLE " + tableName
        + "  (    uid STRING,    eventid STRING,    uuid STRING,    eventTime BIGINT) WITH (    'connector.type' = 'kafka',    'connector.version' = 'universal',    'connector.topic' = 'event',    'connector.startup-mode' = 'earliest-offset',    'connector.properties.zookeeper.connect' = 'itserver21:2181',    'connector.properties.bootstrap.servers' = 'itserver21:6667',    'format.type' = 'json', 'is_generic'='false')");

    String sql = "select * from " + tableName;
    System.out.println(sql);
    TableResult res = tableEnv.executeSql(sql);
    res.print();

    String sinkTable = "event" + System.currentTimeMillis();
    // 设置为hive, 利用hive语法建表
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tableEnv.executeSql("CREATE TABLE " + sinkTable
        + " (uid STRING,    eventid STRING,    uuid STRING,    eventTime BIGINT,proctime BIGINT) PARTITIONED BY (dt STRING, hr STRING) STORED AS ORC TBLPROPERTIES (  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',  'sink.partition-commit.trigger'='partition-time',  'sink.partition-commit.delay'='1 min',  'sink.partition-commit.policy.kind'='metastore,success-file')");

    tableEnv.executeSql("INSERT INTO TABLE " + sinkTable
        + " SELECT uid,eventid, uuid, eventTime  FROM " + tableName);

    sql = "select * from " + sinkTable + " limit 10";
    System.out.println(sql);
    res = tableEnv.executeSql(sql);
    res.print();

  }



}
