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

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.impl.BeanAsArrayDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.orc.InMemoryKeystore;
import com.coomia.datalake.io.ProducerStringSerializationSchema;
import com.coomia.datalake.tools.EventSourceGenerator;

/**
 * 
 * @author spancer
 *
 */
public class FlinkIcebergHadoopWriter {
  public static void main(String[] args) throws Exception {
    // set hadoop user as hdfs.
    System.setProperty("HADOOP_USER_NAME", "hdfs");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);


    // produce some data to kafka
    String topic = "event";
    String servers = "itserver21:6667";
    DataStream<String> eventData = env.addSource(new EventSourceGenerator(1000000000, 10000000));
    eventData
        .addSink(new FlinkKafkaProducer<String>(topic, new ProducerStringSerializationSchema(topic),
            producerProps(servers), Semantic.EXACTLY_ONCE));

    Properties properties = consumeProps(servers, "iceberg-writer");

    FlinkKafkaConsumer<String> consumer =
        new FlinkKafkaConsumer<String>("event", new SimpleStringSchema(), properties);
    consumer.setStartFromLatest();

    DataStreamSource<String> dataStream = env.addSource(consumer);
    dataStream.uid("kafka-source");

    DataStream<RowData> input = eventData.map((v) -> {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, System.currentTimeMillis());
      row.setField(1, StringData.fromString(v));
      System.out.println(v);
      return row;
    });

    String basePath = "hdfs://itserver21:8020/warehouse/iceberg/";

    // create table schema.
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    // identify partition specification.
    PartitionSpec spec = PartitionSpec.unpartitioned();

    // table path
    String tablePath = basePath.concat("demo" + System.currentTimeMillis());

    // property settings, format as orc or parquet
    Map<String, String> props =
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());

    // create an iceberg table.
    Table table = new HadoopTables().create(schema, spec, props, tablePath);

    TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

    FlinkSink.forRowData(input).tableLoader(tableLoader).overwrite(false).build();

    env.execute("iceberg");
  }

  /**
   * kakfa consumer property settings.
   * 
   * @param servers
   * @param groupId
   * @return
   */
  public static Properties consumeProps(String servers, String groupId) {
    Properties prop = new Properties();
    // bootstrap server lists.
    prop.put("bootstrap.servers", servers);
    // groupId
    prop.put("group.id", groupId);
    // record the offset.
    prop.put("enable.auto.commit", "false");
    prop.put("auto.offset.reset", "earliest");
    prop.put("session.timeout.ms", "300000");
    prop.put("max.poll.interval.ms", "300000");
    // get 10 records per poll.
    prop.put("max.poll.records", 10);
    // Key deserializer
    prop.put("key.deserializer", StringDeserializer.class.getName());
    // value deserializer
    prop.put("value.deserializer", BeanAsArrayDeserializer.class.getName());
    return prop;
  }

  public static Properties producerProps(String servers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("acks", "all");
    /**
     * 设置大于零的值时，Producer会发送失败后会进行重试。
     */
    props.put("retries", 0);
    /**
     * Producer批量发送同一个partition消息以减少请求的数量从而提升客户端和服务端的性能，默认大小是16348 byte(16k).
     * 发送到broker的请求可以包含多个batch, 每个batch的数据属于同一个partition，太小的batch会降低吞吐.太大会浪费内存.
     */
    props.put("batch.size", 16384);
    /**
     * batch.size和liner.ms配合使用，前者限制大小后者限制时间。前者条件满足的时候，同一partition的消息会立即发送,
     * 此时linger.ms的设置无效，假如要发送的消息比较少, 则会等待指定的时间以获取更多的消息，此时linger.ms生效 默认设置为0ms(没有延迟).
     */
    props.put("linger.ms", 1);
    /**
     * Producer可以使用的最大内存来缓存等待发送到server端的消息.默认值33554432 byte(32m)
     */
    props.put("buffer.memory", 33554432);
    // props.put("compression.type", "snappy");
    props.put("max.request.size", 10485760);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return props;
  }
}
