# Flink iceberg integration tests 
Testing flink jobs on yarn, jobs integrated with iceberg, hive, hbase, kafka, elasticsearch, etc.

## Used components

* Apache Hadoop 3.2
* Kafka 2.0.0
* Hbase 2.0.0
* Hive 3.1.2
* ELK 7.9.1
* Iceberg 0.10.0
* Flink 1.11.3

## How to Run

* Deploy a testing big data platform using docker-compose. [Download here](https://github.com/spancer/bigdata-docker-compose/tree/flink-yarn)

* Build each jobs and run it on yarn with yarn session or pre-job. 

## TODO
1. testing ozone and alluxio.
2. deep integration with iceberg and flink sql.