#!/bin/sh

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT SUBJECT COLLECTION"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../../target/lodmill-ld-0.2.0-SNAPSHOT-jar-with-dependencies.jar

TMP=output/json-ld-tmp

IN=$1
OUT=$2
SUBJ=$3
COLLECTION=$4

TIME=`date '+%Y%m%d-%H%M%S'`

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $OUT
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $IN $TMP $SUBJ
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $IN $TMP $OUT $COLLECTION-index-$TIME json-ld-$COLLECTION $SUBJ
