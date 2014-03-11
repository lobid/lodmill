#!/bin/sh

if [ $# -ne 5 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT SUBJECT INDEX TYPE"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../../target/lodmill-ld-1.5.0-jar-with-dependencies.jar

TMP=output/json-ld-tmp

IN=$1
OUT=$2
SUBJ=$3
INDEX=$4
TYPE=$5

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $OUT
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $IN $TMP $SUBJ $INDEX
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $IN $TMP $OUT $INDEX $TYPE $SUBJ
