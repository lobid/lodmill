#!/bin/sh

if [ $# -lt 6 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT SUBJECT INDEX TYPE INDEX_ALIAS_SUFFIX [COLLECT]"
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
COLLECT=$7

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $OUT
$HADOOP/bin/hadoop fs -rmr *.map
if [ $COLLECT = "COLLECT" ]; then
    $HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $IN $TMP $SUBJ $INDEX
fi
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $IN $TMP $OUT $INDEX $TYPE $SUBJ $INDEX_ALIAS
