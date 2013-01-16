#!/bin/sh

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` INPUT"
  exit 65
fi
export HADOOP=/opt/hadoop/hadoop
#export HADOOP_CLIENT_OPTS="-XX:+UseConcMarkSweepGC -Xmx1024m $HADOOP_CLIENT_OPTS"
$HADOOP/bin/hadoop jar ../target/lodmill-0.1.0-SNAPSHOT-jar-with-dependencies.jar org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch hdfs://127.0.0.1:8020/ $1 127.0.0.1 es-lod-local

