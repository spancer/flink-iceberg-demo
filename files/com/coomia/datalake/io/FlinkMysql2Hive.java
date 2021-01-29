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
public class FlinkMysql2Hive {

  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hdfs");

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
    // create mysql datasource in flink.
    boolean flag = false;
    if (flag)
      tableEnv.executeSql(
          "CREATE TABLE a1 (id BIGINT,ds VARCHAR(60),ts TIMESTAMP, item BIGINT, cc BIGINT,PRIMARY KEY (id) NOT ENFORCED) WITH ('connector.type' = 'jdbc',    'connector.url' = 'jdbc:mysql://itserver21:3306/druid',    'connector.table' = 'aaa',    'connector.driver' = 'com.mysql.jdbc.Driver',    'connector.username' = 'root',    'connector.password' = 'youmei',    'connector.lookup.cache.max-rows' = '5000',    'connector.lookup.cache.ttl' = '10min')");

    String sql = "select * from a1 limit 10";
    System.out.println(sql);
    TableResult res = tableEnv.executeSql(sql);
    res.print();

    sql = "select * from hive_compatible_tbl limit 10";
    System.out.println(sql);
    res = tableEnv.executeSql(sql);
    res.print();

    // 设置为hive，hive语法建表。
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    if (flag)
      tableEnv.executeSql(
          "CREATE TABLE a2 (id BIGINT,ds VARCHAR(60),ts TIMESTAMP, item BIGINT, cc BIGINT) PARTITIONED BY (dt STRING, hr STRING) STORED AS ORC TBLPROPERTIES (  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',  'sink.partition-commit.trigger'='partition-time',  'sink.partition-commit.delay'='1 min',  'sink.partition-commit.policy.kind'='metastore,success-file')");

    tableEnv.executeSql(
        "INSERT INTO TABLE a2 SELECT id,ds, ts, item, cc, DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH') FROM a1");

    sql = "select * from a2 limit 10";
    System.out.println(sql);
    res = tableEnv.executeSql(sql);
    res.print();
  }



}
