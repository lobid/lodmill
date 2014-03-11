#!/bin/sh

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` INPUT ES_SERVER ES_CLUSTER INDEX_ALIAS"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop

INPUT=$1
ES_SERVER=$2
ES_CLUSTER=$3
INDEX_ALIAS=$4

$HADOOP/bin/hadoop jar ../../target/lodmill-ld-1.5.0-jar-with-dependencies.jar org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch hdfs://10.9.0.10:8020/ $INPUT $ES_SERVER $ES_CLUSTER $INDEX_ALIAS
