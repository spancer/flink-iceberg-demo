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
package com.coomia.datalake.iceberg;

import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.coomia.datalake.kafka.KafkaUtils;

public class FlinkWriteIcebergTest {

  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "root");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    // iceberg catalog identification.
    Configuration conf = new Configuration();
    Catalog catalog = new HadoopCatalog(conf);

    // iceberg table identification.
    TableIdentifier name =
        TableIdentifier.of("default", "iceberg-tb-" + System.currentTimeMillis());

    // iceberg table schema identification.
    Schema schema = new Schema(Types.NestedField.required(1, "uid", Types.StringType.get()),
        Types.NestedField.required(2, "eventTime", Types.LongType.get()),
        Types.NestedField.required(3, "eventid", Types.StringType.get()),
        Types.NestedField.optional(4, "uuid", Types.StringType.get()));
        Types.NestedField.required(5, "ts", Types.TimestampType.withoutZone());


    // iceberg table partition identification.
    // PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("uid", 5).build();

    PartitionSpec spec = PartitionSpec.unpartitioned();
    // identify using orc format as storage.
    Map<String, String> props =
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());
    Table table = null;
    // create an iceberg table if not exists, otherwise, load it.
    if (!catalog.tableExists(name))
      table = catalog.createTable(name, schema, spec, props);
    else
      table = catalog.loadTable(name);

    String topic = "arkevent";
    String servers = "kafka:9092";

    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic,
        new SimpleStringSchema(), KafkaUtils.consumeProps(servers, "flink-consumer"));
    consumer.setStartFromEarliest();

    SingleOutputStreamOperator<RowData> dataStream =
        env.addSource(consumer).map(new MapFunction<String, RowData>() {

          @Override
          public RowData map(String value) throws Exception {
            JSONObject dataJson = JSON.parseObject(value);
            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromBytes(dataJson.getString("uid").getBytes()));
            row.setField(1, dataJson.getLong("eventTime"));
            row.setField(2, StringData.fromBytes(dataJson.getString("eventid").getBytes()));
            row.setField(3, StringData.fromBytes(dataJson.getString("uuid").getBytes()));
            row.setField(4, TimestampData.fromEpochMillis(dataJson.getLong("eventTime")));
            return row;
          }


        });
    // uid is used for job restart or something when using savepoint.
    dataStream.uid("flink-consumer");

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());

    // sink data to iceberg table
    FlinkSink.forRowData(dataStream).table(table).tableLoader(tableLoader).writeParallelism(1)
        .overwrite(true)
        .build();
    
    
    //read and write to file.
    DataStream<RowData> batchData = FlinkSource.forRowData().env(env).tableLoader(tableLoader).build();
    batchData.print();
    batchData.writeAsCsv(tableLoader.loadTable().location().concat("/out/out.csv"), WriteMode.OVERWRITE, "\n", " ");

    // Execute the program.
    env.execute("Test Iceberg DataStream");


  }

}
