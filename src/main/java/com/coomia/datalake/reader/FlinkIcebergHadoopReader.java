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
