package com.coomia.datalake.hive;

import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TableLoader.HadoopTableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import com.google.common.collect.Lists;

public class FlinkHiveIcebergWriter {

  public static void main(String[] args) throws Exception {

    System.setProperty("HADOOP_USER_NAME", "hdfs");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    // iceberg catalog identification.
    Configuration conf = new Configuration();
    Catalog catalog = new HiveCatalog(conf);

    // iceberg table identification.
    TableIdentifier name = TableIdentifier.of("default", "iceberg-tb");

    // iceberg table schema identification.
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "age", Types.IntegerType.get()),
        Types.NestedField.optional(4, "ts", Types.TimestampType.withZone()));

    // iceberg table partition identification.
    PartitionSpec spec = PartitionSpec.builderFor(schema).year("ts").bucket("id", 2).build();

    // create an iceberg table.
    Table table = catalog.createTable(name, schema, spec);

    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream = env.addSource(new SourceFunction<RowData>() {

      @Override
      public void run(SourceContext<RowData> ctx) throws Exception {
        while (true) {
          ctx.collect(new RowData);
        }
      }

      @Override
      public void cancel() {
        // TODO Auto-generated method stub

      }
    });

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
    
    FlinkSink.forRowData(dataStream).table(table).tableLoader(tableLoader)
        .writeParallelism(1).build();

    // Execute the program.
    env.execute("Test Iceberg DataStream");


  }

}
