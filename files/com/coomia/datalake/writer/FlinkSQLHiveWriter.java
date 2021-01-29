package com.coomia.datalake.writer;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkSQLHiveWriter {


  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner() // 使用BlinkPlanner
        .inBatchMode() // Batch模式，默认为StreamingMode
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String catalogName = "flinkhivecatalog";
    String defaultDatabase = "db20210120"; // 默认数据库名称
    String hiveConfDir = null; // hive-site.xml路径

    HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog(catalogName, hive);
    tableEnv.useCatalog(catalogName);
    // set sql dialect as default, means using flink sql.
    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tableEnv.useDatabase(defaultDatabase);
    String tableName = "event20210120";
    // is generic = false, means we can execute query on hive too.
    tableEnv.executeSql("CREATE TABLE " + tableName
        + "  (    uid STRING,    eventid STRING,    uuid STRING,    eventTime BIGINT) WITH (    'connector.type' = 'kafka',    'connector.version' = 'universal',    'connector.topic' = 'event',    'connector.startup-mode' = 'earliest-offset',    'connector.properties.zookeeper.connect' = 'itserver21:2181',    'connector.properties.bootstrap.servers' = 'itserver21:6667',    'format.type' = 'json', 'is_generic'='false')");

    String sql = "select * from " + tableName;
    System.out.println(sql);
    TableResult res = tableEnv.executeSql(sql);
    res.print();

    String sinkTable = "eventsink20210120";
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
