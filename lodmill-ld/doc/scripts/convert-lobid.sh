#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT"
  exit 65
fi

HADOOP=/opt/hadoop/hadoop
HADOOP_CLASSPATH=../../target/lodmill-ld-0.1.0-SNAPSHOT-jar-with-dependencies.jar
TMP=output/json-ld-tmp

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr $2
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriples $1 $TMP
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $TMP $2 lobid-index json-ld-lobid
