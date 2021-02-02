package com.coomia.datalake.hive;

import java.util.Random;
import java.util.UUID;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

public class FlinkHiveIcebergWriter {

  public static void main(String[] args) throws Exception {

    //System.setProperty("HADOOP_USER_NAME", "root");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    // iceberg catalog identification.
    Configuration conf = new Configuration();
    Catalog catalog = new HiveCatalog(conf);

    // iceberg table identification.
    TableIdentifier name = TableIdentifier.of("default", "iceberg-hive-tb");

    // iceberg table schema identification.
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "age", Types.IntegerType.get()),
        Types.NestedField.optional(4, "ts", Types.TimestampType.withoutZone()));

    // iceberg table partition identification.
    PartitionSpec spec = PartitionSpec.builderFor(schema).year("ts").bucket("id", 2).build();

    // create an iceberg table.
    Table table = catalog.createTable(name, schema, spec);

    DataStream<RowData> dataStream = env.addSource(new SourceFunction<RowData>() {
      /**
       * 
       */
      private static final long serialVersionUID = -8786053745984140851L;
      boolean flag = true;

      @Override
      public void run(SourceContext<RowData> ctx) throws Exception {
        while (flag) {
          GenericRowData row = new GenericRowData(4);
          row.setField(0, new Random().nextInt());
          row.setField(1, StringData.fromBytes(UUID.randomUUID().toString().getBytes()));
          row.setField(2, new Random().nextInt());
          row.setField(3, TimestampData.fromEpochMillis(System.currentTimeMillis() - new Random().nextInt()));
          ctx.collect(row);
        }
      }

      @Override
      public void cancel() {
        flag = false;
      }
    });

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());

    FlinkSink.forRowData(dataStream).table(table).tableLoader(tableLoader).writeParallelism(1)
        .build();

    // Execute the program.
    env.execute("flink Iceberg hive catalog sink");


  }

}
