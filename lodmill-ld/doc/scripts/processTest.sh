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

cd ../../; git pull; mvn assembly:assembly -DskipTests

# first, make the data
cd ../lodmill-rd/doc/scripts/hbz01/
bash startHbz012lobidUpdateMysqlInParallel.sh $BRANCH test
cd  "$THIS"

TIME=`date '+%Y%m%d-%H%M%S'`

GND=output/json-ld-gnd
INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd-test/ $GND http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "-smallTest"

INDEX_NAME=lobid-resources-$TIME
RESOURCES=output/json-ld-lobid-resources-test
sh convert.sh hbzlod/lobid-resourcestest/,extlod/dewey_preprocessed.nt,extlod/gnd-test/gndTest.nt $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items-test
sh convert.sh hbzlod/lobid-resourcestest/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item "-smallTest"
