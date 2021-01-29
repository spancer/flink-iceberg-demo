package com.coomia.datalake.kafka;

import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.impl.BeanAsArrayDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer;
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
            producerProps(servers), Semantic.EXACTLY_ONCE));

    env.execute("flink-kafka-produce-test");
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
    props.put("retries", 1);
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
    //https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html
   
    props.put("transaction.timeout.ms",1000*60*5);//default is 15min,set to 5mins

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return props;
  }

}
