package com.coomia.datalake.iceberg;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.table.data.RowData;
import org.apache.http.HttpHost;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import com.alibaba.fastjson.JSONObject;
import com.coomia.datalake.es.ElasticsearchSinkFunctionWithConf;

public class FlinkReadIcebergTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TableLoader tableLoader =
        TableLoader.fromHadoopTable("hdfs://namenode:9000/iceberg/warehouse/default/iceberg-tb");
    DataStream<RowData> batch = FlinkSource.forRowData().env(env).tableLoader(tableLoader).build();

    SingleOutputStreamOperator<String> dataStream = batch.map(new MapFunction<RowData, String>() {
      private static final long serialVersionUID = 3149495323490165536L;

      @Override
      public String map(RowData value) throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("uid", value.getString(0));
        jsonObject.put("eventTime", value.getLong(1));
        jsonObject.put("eventid", value.getString(2));
        jsonObject.put("uuid", value.getString(3));
        jsonObject.put("eventTime", value.getLong(4));
        System.out.println(jsonObject.toJSONString());
        return jsonObject.toJSONString();
      }
    });

    String topic = "arkevent";

    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
    ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
        new ElasticsearchSinkFunctionWithConf(topic, topic));
    esSinkBuilder.setBulkFlushMaxActions(1);

    dataStream.uid("iceberg-consumer");

    dataStream.addSink(esSinkBuilder.build());

    env.execute("Test Iceberg Batch Read");
  }

}
