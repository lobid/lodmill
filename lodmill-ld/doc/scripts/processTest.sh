#!/bin/sh
# Index alias is hardcoded.

if [ ! $# -eq 3 ]
then
  echo "Usage: `basename $0` BRANCH ES_SERVER ES_CLUSTER_NAME"
  exit 65
fi

BRANCH=$1
ES_SERVER=$2
ES_CLUSTER_NAME=$3


THIS="$(pwd)"

cd ../../; git pull;
mvn assembly:assembly -DskipTests

# first, make the data
cd ../lodmill-rd/doc/scripts/hbz01/
bash startHbz012lobidUpdateMysqlInParallel.sh $BRANCH test
cd  "$THIS"

TIME=`date '+%Y%m%d-%H%M%S'`

GND=output/json-ld-gnd
INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd-test/ $GND http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "-smallTest" $ES_SERVER $ES_CLUSTER_NAME

INDEX_NAME=lobid-resources-$TIME
RESOURCES=output/json-ld-lobid-resources-test
sh convert.sh hbzlod/lobid-resourcestest/,extlod/dewey_preprocessed.nt,extlod/gnd-test/gndTest.nt $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS $ES_SERVER $ES_CLUSTER_NAME # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items-test
sh convert.sh hbzlod/lobid-resourcestest/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item "-smallTest" $ES_SERVER $ES_CLUSTER_NAME
