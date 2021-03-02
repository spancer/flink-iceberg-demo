# how to run

After build the job jar using maven install, you can upload to your server, and copy it to docker container using docker cp. e.g.
	
	docker cp flink-kafka-test-0.0.1-SNAPSHOT.jar jobmanager:/opt/flink
		
1. start yarn session.
	
	docker-compose exec jobmanager bash
	
	bin/yarn-session.sh &
	
2. log yid from console or get it from web ui via : http://resourcemanager:8088

3. produce some data to kafka topic [arkevent], this topic is auto-created after kafka started.

	flink run -yid application_1614215541327_0001 -c  com.coomia.datalake.kafka.FlinkKafkaProduceDemo ../flink-kafka-test-0.0.1-SNAPSHOT.jar

4. test iceberg write using hadoop calalog.
	
	flink run -yid application_1614215541327_0001 -c  com.coomia.datalake.iceberg.FlinkWriteIcebergTest ../flink-kafka-test-0.0.1-SNAPSHOT.jar

5. test iceberg read and sink data to es, iceberg table name can be found in hdfs: http://namenode:9870, table is under folder: /iceberg/warehouse/default/
	
	flink run -yid application_1614215541327_0001 -c  com.coomia.datalake.iceberg.FlinkReadIcebergTest  ../flink-kafka-test-0.0.1-SNAPSHOT.jar  --table "iceberg-tb-1614216179276" 

Alternative, you can consume kafka data and sink it to es. we can view data using kibana: http://kibana:5601 

	flink run -yid application_1614215541327_0001 -c  com.coomia.datalake.kafka.FlinkKafkaConsumeDemo ../flink-kafka-test-0.0.1-SNAPSHOT.jar

	