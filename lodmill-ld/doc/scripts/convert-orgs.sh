#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../../target/lodmill-ld-0.1.0-SNAPSHOT-jar-with-dependencies.jar

TMP=output/json-ld-tmp

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $2
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $1 $TMP
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $1 $TMP $2 lobid-orgs-index json-ld-lobid-orgs
