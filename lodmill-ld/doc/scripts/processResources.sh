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

if [ $SET != "Updates" ]; then
	ENRICH=",enrich/"
fi

HDFS_FILE="hbzlod/lobid-resources-$SET/resources-dump.nt"
echo "Dumping to $HDFS_FILE ..."
hadoop fs -rm $HDFS_FILE
time bash -x mysqlDumpTable.sh $SET | hadoop dfs -put - $HDFS_FILE
echo "done dumping!"

time sh convert.sh hbzlod/lobid-resources-$SET/,extlod/gnd/,extlod/dewey_preprocessed.nt$ENRICH http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS COLLECT
echo "done resources!"

time sh convert.sh hbzlod/lobid-resources-$SET/ http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS"
echo "done items!"

curl -L 'http://api.lobid.org/resource?author=118580604&owner=DE-5,DE-6' > /dev/null
curl -L 'http://staging.api.lobid.org/resource?author=118580604&owner=DE-5,DE-6' > /dev/null
echo "done owner warm-up"
