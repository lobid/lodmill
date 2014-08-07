#!/bin/sh

PRINT_TIME() {
 printf "\n$(date '+%y/%m/%d %H:%M:%S')"
}

if [ $# -ne 5 ]
then
  printf "Usage: `basename $0` INPUT ES_SERVER ES_CLUSTER INDEX_ALIAS INDEX_NAME\n"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop

INPUT=$1
ES_SERVER=$2
ES_CLUSTER=$3
INDEX_ALIAS=$4
INDEX_NAME=$5

ES_URL="http://$ES_SERVER:9200/$INDEX_NAME/"
PRINT_TIME ;printf "Create index in $ES_URL\n"
curl -XPUT "$ES_URL"

PRINT_TIME ; printf "Start stopping index refreshing ... \n" 
curl -XPUT "$ES_URL"/_settings -d '{
    "index" : {
        "refresh_interval" : "-1"
    } }'

$HADOOP/bin/hadoop jar ../../target/lodmill-ld-*-with-dependencies.jar org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch hdfs://10.9.0.10:8020/ $INPUT $ES_SERVER $ES_CLUSTER $INDEX_ALIAS

PRINT_TIME ;printf "Start index refreshing...\n"
curl -XPUT "$ES_URL"/_settings -d '{
    "index" : {
        "refresh_interval" : "1s"
    } }'
PRINT_TIME ;printf "Done index refreshing. Optimizing the index ...\n"
curl -XPOST "$ES_URL"/_optimize
PRINT_TIME ;printf "Done index optimizing.\n"
