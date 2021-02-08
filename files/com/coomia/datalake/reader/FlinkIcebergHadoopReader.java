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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * 
 * @author spancer
 *
 */
public class FlinkIcebergHadoopReader {

  public static void main(String[] args) throws Exception {

    // set hadoop user as hdfs.
    System.setProperty("HADOOP_USER_NAME", "hdfs");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    String warehouse = "hdfs://itserver21:8020/flink/";

    // load table.
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehouse);

    Table table = catalog.loadTable(TableIdentifier.of("demo20210120"));
    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());

    FlinkSource.forRowData().env(env).tableLoader(tableLoader).table(table).build()
        .map(new MapFunction<RowData, String>() {
          private static final long serialVersionUID = 1L;

          @Override
          public String map(RowData value) throws Exception {
            String val = value.getRawValue(0).toString();
            System.out.println(val);
            return val;
          }
        });
    catalog.close();
    env.execute("iceberg-reader-test");

  }



}
