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
package com.coomia.datalake.reader;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * 
 * @author spancer
 *
 */
public class FlinkSQLIcebergHadoopReader {

  public static void main(String[] args) throws Exception {

    String warehouse = "hdfs://itserver21:8020/flink";

    System.setProperty("HADOOP_USER_NAME", "hdfs");

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner() // 使用BlinkPlanner
        .build();
    TableEnvironment tenv = TableEnvironment.create(settings);
    String createIcebergCatalog =
        "CREATE CATALOG iceberg WITH ( 'type'='iceberg', 'catalog-type'='hadoop', 'clients'='5', 'property-version'='1', 'warehouse'='hdfs://itserver21:8020/flink')";
    tenv.executeSql(createIcebergCatalog);
    tenv.executeSql("show catalogs").print();
    tenv.useCatalog("iceberg");
    tenv.executeSql("show tables").print();
    tenv.useDatabase("demo20210120");
    tenv.executeSql("show tables").print();
    tenv.executeSql("select * from demo1611026768511 limit 10 ").print();

  }

  

}
