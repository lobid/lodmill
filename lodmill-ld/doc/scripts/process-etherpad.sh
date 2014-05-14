#!/bin/sh

ES_SERVER=$1 # 193.30.112.171 aka quaoar1
ES_CLUSTER=$2 # lobid-hbz

TIME=`date '+%Y%m%d-%H%M%S'`

LOBID_TEAM=output/lobid-team
sh convert.sh hbzlod/lobid-team $LOBID_TEAM http://lobid.org/team lobid-team-$TIME json-ld-lobid-team
sh index.sh $LOBID_TEAM $ES_SERVER $ES_CLUSTER ""

LOBID_COLLECTIONS=output/lobid-collections
sh convert.sh hbzlod/lobid-collections $LOBID_COLLECTIONS http://lobid.org/resource lobid-collections-$TIME json-ld-lobid-collection
sh index.sh $LOBID_COLLECTIONS $ES_SERVER $ES_CLUSTER ""
