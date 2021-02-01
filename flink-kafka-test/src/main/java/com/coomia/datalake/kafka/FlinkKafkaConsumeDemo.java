package com.coomia.datalake.kafka;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import com.coomia.datalake.es.ElasticsearchSinkFunctionWithConf;

/**
 * 
 * @author spancer
 *
 */
public class FlinkKafkaConsumeDemo {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    String topic = "arkevent";
    String servers = "kafka:9092";


    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
    ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
        new ElasticsearchSinkFunctionWithConf(topic, topic));
    esSinkBuilder.setBulkFlushMaxActions(1);

    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic,
        new SimpleStringSchema(), KafkaUtils.consumeProps(servers, "flink-consumer"));
    consumer.setStartFromLatest();

    DataStreamSource<String> dataStream = env.addSource(consumer);
    // uid is used for job restart or something when using savepoint.
    dataStream.uid("flink-consumer");
    dataStream.addSink(esSinkBuilder.build());
    env.execute("flink-consumer-demo");
  }



}
