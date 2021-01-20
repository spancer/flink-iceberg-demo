package com.coomia.datalake.writer;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author spancer
 *
 */
public class FlinkSQLIcebergHadoopWriter {

  /**
   * @param args
   */
  public static void main(String[] args) {

    System.setProperty("HADOOP_USER_NAME", "hdfs");

    // use BlinkPlanner
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
    TableEnvironment tenv = TableEnvironment.create(settings);
    String createIcebergCatalog =
        "CREATE CATALOG iceberg WITH ( 'type'='iceberg', 'catalog-type'='hadoop', 'clients'='5', 'property-version'='1', 'warehouse'='hdfs://itserver21:8020/warehouse')";
    tenv.executeSql(createIcebergCatalog);

    boolean exists = true;
    tenv.executeSql("show catalogs").print();
    tenv.useCatalog("iceberg");
    if (!exists)
      tenv.executeSql("CREATE DATABASE hadoopdb");
    tenv.useDatabase("hadoopdb");
    if (!exists)
      tenv.executeSql("CREATE TABLE iceberg.hadoopdb.sample (\r\n"
          + "id BIGINT COMMENT 'unique id',\r\n" + "data STRING\r\n" + ")");
    tenv.executeSql("show tables").print();

    tenv.executeSql("INSERT INTO iceberg.hadoopdb.sample VALUES (2, 'b')");
    tenv.executeSql("select * from sample ").print();
    tenv.executeSql("select * from hadoopdb.sample ").print();

  }

}
