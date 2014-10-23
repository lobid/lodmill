#!/bin/sh

if [ $# -lt 5 ]
then
  echo "Usage: `basename $0` INPUT SUBJECT INDEX TYPE INDEX_ALIAS_SUFFIX [COLLECT]"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../../target/lodmill-ld-2.0.1-jar-with-dependencies.jar

TMP=output/json-ld-tmp

IN=$1
SUBJ=$2
INDEX=$3
TYPE=$4
INDEX_ALIAS=$5
COLLECT=$6

$HADOOP/bin/hadoop fs -rmr $TMP
$HADOOP/bin/hadoop fs -rmr *.map
if [ $COLLECT = "COLLECT" ]; then
    $HADOOP/bin/hadoop org.lobid.lodmill.hadoop.CollectSubjects $IN $TMP $SUBJ $INDEX
fi
echo "<$IN> <$INDEX> <$TYPE> <$SUBJ> <$INDEX_ALIAS>"
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd $IN $INDEX $TYPE $SUBJ "$INDEX_ALIAS"
