#!/bin/sh
# parameter 3 defines the branch.
# Index alias is hardcoded to "-staging".

if [ $# -lt 3 ]
then
  echo "Usage: `basename $0` ES-SERVER ES-CLUSTER BRANCH"
  exit 65
fi

ES_SERVER=$1
ES_CLUSTER=$2
BRANCH=$3

THIS="$(pwd)"

cd ../../; mvn assembly:assembly -DskipTests

# first, make the data
cd ../lodmill-rd/doc/scripts/hbz01/
bash startHbz012lobidUpdateMysqlInParallel.sh $BRANCH test
cd  "$THIS"

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=lobid-resources-$TIME

RESOURCES=output/json-ld-lobid-resources-test
sh convert.sh hbzlod/lobid-resourcestest/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS $INDEX_NAME # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items-test
sh convert.sh hbzlod/lobid-resourcestest/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER "-staging" $INDEX_NAME
