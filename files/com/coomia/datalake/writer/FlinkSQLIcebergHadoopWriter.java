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
