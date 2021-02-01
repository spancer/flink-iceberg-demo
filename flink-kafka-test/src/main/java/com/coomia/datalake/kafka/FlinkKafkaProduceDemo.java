package com.coomia.datalake.kafka;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;

public class FlinkKafkaProduceDemo {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);
    env.enableCheckpointing(10 - 000);

    // produce some data to kafka
    String topic = "arkevent";
    String servers = "kafka:9092";
    DataStream<String> producer = env.addSource(new EventSourceGenerator(10000, 1000));
    producer
        .addSink(new FlinkKafkaProducer<String>(topic, new ProducerStringSerializationSchema(topic),
            KafkaUtils.producerProps(servers), Semantic.EXACTLY_ONCE));

    env.execute("flink-kafka-produce-test");
  }
 

}
