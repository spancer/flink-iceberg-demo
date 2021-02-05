# running this job on yarn.
1. start a yarn session.
2. running job with params.

	flink run -yid application_1612250026667_0001  ../jobs/bdp-benchmark-test-0.0.1-SNAPSHOT.jar  --topic "arkevent" --bootstrap-server "itserver21:6667,itserver22:6667,itserver23:6667" --esHost "itserver26"    --zkHost "itserver21" --hbaseMaster "itserver22:16000" --path "hdfs://umxhdfscluster/flink/" 
 
	  flink run -yid application_1612317134041_0001  ../bdp-benchmark-test-0.0.1-SNAPSHOT.jar
