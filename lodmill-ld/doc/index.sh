export HADOOP=/opt/hadoop/hadoop
$HADOOP/bin/hadoop jar lodmill-0.1.0-SNAPSHOT-jar-with-dependencies.jar org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch hdfs://10.1.2.111:8020/ json-ld-output 10.1.2.111 es-lod-hydra
