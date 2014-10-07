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

sh convert.sh hbzlod/lobid-team http://lobid.org/team lobid-team-$TIME json-ld-lobid-team "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

sh convert.sh hbzlod/lobid-collections http://lobid.org/resource lobid-collections-$TIME json-ld-lobid-collection "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME
