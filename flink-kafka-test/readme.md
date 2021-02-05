# how to run
1. start yarn session.
2. test iceberg write using hadoop calalog.
	
	flink run -yid application_1612317134041_0001 -c  com.coomia.datalake.iceberg.FlinkWriteIcebergTest ../flink-kafka-test-0.0.1-SNAPSHOT.jar

3. test iceberg read and sink data to es.
	
	flink run -yid application_1612317134041_0001 -c  com.coomia.datalake.iceberg.FlinkReadIcebergTest  ../flink-kafka-test-0.0.1-SNAPSHOT.jar  --table "iceberg-tb-1612503648009" 

	