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
ALIAS=$3

TIME=`date '+%Y%m%d-%H%M%S'`

RESOURCES=output/json-ld-lobid-resources
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS $INDEX_NAME & # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME
