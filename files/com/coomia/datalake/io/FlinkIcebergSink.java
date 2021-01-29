/*
 * Copyright (c) UMX Technology Co., Ltd. All Rights Reserved. This software is licensed not sold.
 * Use or reproduction of this software by any unauthorized individual or entity is strictly
 * prohibited. This software is the confidential and proprietary information of UMX Technology Co.,
 * Ltd. Disclosure of such confidential information and shall use it only in accordance with the
 * terms of the license agreement you entered into with UMX Technology Co., Ltd.
 * 
 * UMX Technology Co., Ltd. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. UMX Technology Co., Ltd.
 * SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ANY DERIVATIVES THEREOF.
 */
// Created on 2021年1月18日

package com.coomia.flink.demo;

import java.util.Properties;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.PropertiesUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/**
 * @author spancer
 *
 */
public class FlinkIcebergSink {
  public static void main(String[] args) {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    // Checkpoint配置.
    CheckpointConfig config = env.getCheckpointConfig();
    config.setCheckpointInterval(5 * 60 * 1000); // 5mins
    config.setMinPauseBetweenCheckpoints(5 * 60 * 1000);
    config.setCheckpointTimeout(10 * 60 * 1000);

    // 如果可以接受作业失败重启时发生数据重复的话，可以设置为AT_LEAST_ONCE，这样会加快Checkpoint速度。
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    // config.enableUnalignedCheckpoints(true);
    config.enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // env.setStateBackend(new MemoryStateBackend());
    // hdfs://itserver21:8020/warehouse
    env.setStateBackend(new RocksDBStateBackend("hdfs://itserver21:8020/warehouse/iceberg", true));

    Properties properties = PropertiesUtil.getKafkaProperties("iceberg-writer");
    KafkaProperties manager = new FlinkKafkaManager("iceberg-writer", properties);
    FlinkKafkaConsumer<String> consumer = manager.buildStringConsumer();


    FlinkKafkaConsumer<String> consumer =
        new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaConfig.consumeProps());
    // 从最近的消息开始处理
    consumer.setStartFromLatest();

    DataStreamSource<String> dataStream = env.addSource(consumer);
    // 指定UID可以更好地兼容版本升级.
    dataStream.uid("kafka-source");

    DataStream<RowData> input = dataStream.map((v) -> {
      Random random = new Random(System.currentTimeMillis());
      GenericRowData row = new GenericRowData(2);
      row.setField(0, random.nextLong());
      row.setField(1, StringData.fromString(v));
      return row;
    });

    Configuration configuration = new Configuration();
    TableLoader tableLoader =
        TableLoader.fromHadoopTable("hdfs://itserver21:8020/warehouse/iceberg/sample");

    FlinkSink.forRowData(input).tableLoader(tableLoader).overwrite(false).build();

    env.execute("iceberg");
  }
}
