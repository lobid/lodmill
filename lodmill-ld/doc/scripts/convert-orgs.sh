#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` INPUT OUTPUT"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../target/lodmill-0.1.0-SNAPSHOT-jar-with-dependencies.jar

$HADOOP/bin/hadoop fs -rmr $2
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $1 $2 lobid-orgs-index json-ld-lobid-orgs
