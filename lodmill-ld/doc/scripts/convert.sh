#!/bin/sh

if [ $# -ne 8 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT SUBJECT INDEX TYPE INDEX_ALIAS_SUFFIX ES_SERVER ES_CLUSTER_NAME"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../../target/lodmill-ld-2.0.0-jar-with-dependencies.jar

TMP=output/json-ld-tmp

IN=$1
OUT=$2
SUBJ=$3
INDEX=$4
TYPE=$5
INDEX_ALIAS=$6
ES_SERVER=$7
ES_CLUSTER_NAME=$8

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $OUT
$HADOOP/bin/hadoop fs -rmr *.map
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $IN $TMP $SUBJ $INDEX
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $IN $TMP $OUT $INDEX $TYPE $SUBJ $INDEX_ALIAS $ES_SERVER $ES_CLUSTER_NAME
