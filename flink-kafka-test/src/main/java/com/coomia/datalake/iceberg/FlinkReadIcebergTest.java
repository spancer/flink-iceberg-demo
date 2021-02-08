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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.MultipleParameterTool;
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
    MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);

    String tableNameString = "iceberg-tb-1612252540237";
    if (params.has("table"))
      tableNameString = params.get("table");
    TableLoader tableLoader = TableLoader
        .fromHadoopTable("hdfs://namenode:9000/iceberg/warehouse/default/" + tableNameString);
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

    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
    ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
        new ElasticsearchSinkFunctionWithConf(tableNameString, tableNameString));
    esSinkBuilder.setBulkFlushMaxActions(1);

    dataStream.uid("iceberg-consumer");

    dataStream.addSink(esSinkBuilder.build());

    env.execute("Test Iceberg Batch Read");
  }

}
