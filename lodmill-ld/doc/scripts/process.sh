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

NWBIB=output/json-ld-nwbib
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib.nt $NWBIB http://purl.org/lobid/nwbib# $INDEX_NAME json-ld-nwbib "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

NWBIB_SPATIAL=output/json-ld-nwbib-spatial
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib-spatial.nt $NWBIB_SPATIAL http://purl.org/lobid/nwbib-spatial $INDEX_NAME json-ld-nwbib-spatial "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

ORGANISATIONS=output/json-ld-lobid-organisations
INDEX_NAME=lobid-organisations-$TIME
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation $INDEX_NAME json-ld-lobid-orgs "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

LOBID_TEAM=output/lobid-team
INDEX_NAME=lobid-team-$TIME
sh convert.sh hbzlod/lobid-team $LOBID_TEAM http://lobid.org/team $INDEX_NAME json-ld-lobid-team "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

LOBID_COLLECTIONS=output/lobid-collections
INDEX_NAME=lobid-collections-$TIME
sh convert.sh hbzlod/lobid-collections $LOBID_COLLECTIONS http://lobid.org/resource $INDEX_NAME json-ld-lobid-collection "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

GND=output/json-ld-gnd
INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd/ $GND http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME

RESOURCES=output/json-ld-lobid-resources
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS $ES_SERVER $ES_CLUSTER_NAME

ITEMS=output/json-ld-lobid-items
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS" $ES_SERVER $ES_CLUSTER_NAME
