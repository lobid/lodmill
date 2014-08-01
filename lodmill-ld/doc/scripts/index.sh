#!/bin/sh

if [ $# -ne 5 ]
then
  printf "Usage: `basename $0` INPUT ES_SERVER ES_CLUSTER INDEX_ALIAS INDEX_NAME"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop

INPUT=$1
ES_SERVER=$2
ES_CLUSTER=$3
INDEX_ALIAS=$4
INDEX_NAME=$5

ES_URL="http://$ES_SERVER:9200/$INDEX_NAME/"
printf "create index in $ES_URL"
curl -XPUT "$ES_URL"

printf "stop index refreshing" 
curl -XPUT "$ES_URL"/_settings -d '{
    "index" : {
        "refresh_interval" : "-1"
    } }'

$HADOOP/bin/hadoop jar ../../target/lodmill-ld-1.8.1-jar-with-dependencies.jar org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch hdfs://10.9.0.10:8020/ $INPUT $ES_SERVER $ES_CLUSTER $INDEX_ALIAS

printf "start index refreshing"
curl -XPUT "$ES_URL"/_settings -d '{
    "index" : {
        "refresh_interval" : "1s"
    } }'
printf "optimize the index"
$ curl -XPOST "$ES_URL"/_optimize
