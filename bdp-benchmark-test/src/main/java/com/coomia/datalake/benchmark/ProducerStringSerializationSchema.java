package com.coomia.datalake.benchmark;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * 
 * @author spancer
 * @date 2019/03/19
 *
 */
public class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {
  private static final long serialVersionUID = 5556700683110420508L;
  private String topic;

  public ProducerStringSerializationSchema(String topic) {
    super();
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
    return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
  }
}
