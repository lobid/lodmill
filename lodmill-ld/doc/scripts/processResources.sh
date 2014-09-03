#!/bin/sh

if [ ! $# -eq 3 ]
then
  echo "Usage: `basename $0` ALIAS ES_SERVER ES_CLUSTER_NAME"
  exit 65
fi

ALIAS=$1
ES_SERVER=$2
ES_CLUSTER_NAME=$3

TIME=`date '+%Y%m%d-%H%M%S'`

RESOURCES=output/json-ld-lobid-resources
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS $ES_SERVER $ES_CLUSTER_NAME

ITEMS=output/json-ld-lobid-items
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME
