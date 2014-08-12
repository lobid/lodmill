#!/bin/sh
# parameter 3 defines the index alias. If not given resp. just as ""
# no suffix is appended. Usage example: "-staging".

if [ $# -lt 2 ]
then
  echo "Usage: `basename $0` ES-SERVER ES-CLUSTER [ALIAS]"
  exit 65
fi

ES_SERVER=$1
ES_CLUSTER=$2

TIME=`date '+%Y%m%d-%H%M%S'`

RESOURCES=output/json-ld-lobid-resources-test
sh convert.sh hbzlod/lobid-resourcestest/ $RESOURCES http://lobid.org/resource lobid-resources-$TIME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items-test
sh convert.sh hbzlod/lobid-resourcestest/ $ITEMS http://lobid.org/item lobid-resources-$TIME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER "-staging"
