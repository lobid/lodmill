#!/bin/sh

if [ ! $# -eq 2 ]
then
  echo "Usage: `basename $0` ALIAS {"All","Updates"}"
  exit 65
fi

export PATH="$PATH:/opt/hadoop/hadoop/bin/"
ALIAS=$1
SET=$2

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=lobid-resources-$TIME
if [ $SET = "Updates" ]; then
        INDEX_NAME=$ALIAS
fi

HDFS_FILE="hbzlod/lobid-resources-$SET/resources-dump.nt"
echo "Dumping to $HDFS_FILE ..."
hadoop fs -rm $HDFS_FILE
time bash -x mysqlDumpTable.sh $SET | hadoop dfs -put - $HDFS_FILE
echo "done dumping!"

time sh convert.sh hbzlod/lobid-resources-$SET/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS COLLECT
echo "done resources!"

time sh convert.sh hbzlod/lobid-resources-$SET/ http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS"
echo "done items!"
