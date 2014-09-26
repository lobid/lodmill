#!/bin/sh
# Index alias is hardcoded.

if [ ! $# -eq 1 ]
then
  echo "Usage: `basename $0` BRANCH"
  exit 65
fi

BRANCH=$1

THIS="$(pwd)"

cd ../../; git pull;
mvn assembly:assembly -DskipTests

# first, make the data
cd ../lodmill-rd/doc/scripts/hbz01/
bash startHbz012lobidUpdateMysql.sh $BRANCH Test

HDFS_FILE="hbzlod/lobid-resources-Test/resources-dump.nt"
echo "Dumping to $HDFS_FILE ..."
hadoop fs -rm $HDFS_FILE

cd  "$THIS"
time bash -x mysqlDumpTable.sh Test | hadoop dfs -put - $HDFS_FILE

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd-Test/ http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "-smallTest"

INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources-Test/,extlod/dewey_preprocessed.nt,extlod/gnd-Test/gndTest.nt http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS COLLECT # no alias, index not ready yet, needs items from below

sh convert.sh hbzlod/lobid-resources-Test/ http://lobid.org/item $INDEX_NAME json-ld-lobid-item "-smallTest"
