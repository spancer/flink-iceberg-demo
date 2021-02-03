package com.coomia.iceberg.test;

import java.util.Map;
import java.util.UUID;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import com.google.common.collect.ImmutableMap;

public class IcebergReadWriteTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(5000L);
    env.setParallelism(1);

    DataStream<RowData> inputStream = env.addSource(new RichSourceFunction<RowData>() {

      private static final long serialVersionUID = 1L;
      boolean flag = true;

      @Override
      public void run(SourceContext<RowData> ctx) throws Exception {
        while (flag) {
          GenericRowData row = new GenericRowData(2);
          row.setField(0, System.currentTimeMillis());
          row.setField(1, StringData.fromString(UUID.randomUUID().toString()));
          ctx.collect(row);
        }

      }

      @Override
      public void cancel() {
        flag = false;
      }
    });
    // define iceberg table schema.
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    // define iceberg partition specification.
    PartitionSpec spec = PartitionSpec.unpartitioned();

    // table path
    String basePath = "hdfs://namenode:9000/";

    String tablePath = basePath.concat("iceberg-table-01");

    // property settings, format as orc or parquet
    Map<String, String> props =
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());

    // create an iceberg table.
    Table table = new HadoopTables().create(schema, spec, props, tablePath);

    TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

    FlinkSink.forRowData(inputStream).table(table).tableLoader(tableLoader).writeParallelism(1)
        .build();
    
    //read and write to file.
    DataStream<RowData> batchData = FlinkSource.forRowData().env(env).tableLoader(tableLoader).build();
    batchData.print();
    batchData.writeAsCsv(basePath.concat("out"), WriteMode.OVERWRITE, "\n", " ");
    env.execute("iceberg write and read.");
    
  }

}
