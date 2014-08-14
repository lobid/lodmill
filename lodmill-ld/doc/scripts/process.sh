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

NWBIB=output/json-ld-nwbib
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib.nt $NWBIB http://purl.org/lobid/nwbib# $INDEX_NAME json-ld-nwbib
sh index.sh $NWBIB $ES_SERVER $ES_CLUSTER NOALIAS $INDEX_NAME &

NWBIB_SPATIAL=output/json-ld-nwbib-spatial
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib-spatial.nt $NWBIB_SPATIAL http://purl.org/lobid/nwbib-spatial $INDEX_NAME json-ld-nwbib-spatial
sh index.sh $NWBIB_SPATIAL $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME &

ORGANISATIONS=output/json-ld-lobid-organisations
INDEX_NAME=lobid-organisations-$TIME
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation $INDEX_NAME json-ld-lobid-orgs
sh index.sh $ORGANISATIONS $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME &

LOBID_TEAM=output/lobid-team
INDEX_NAME=lobid-team-$TIME
sh convert.sh hbzlod/lobid-team $LOBID_TEAM http://lobid.org/team $INDEX_NAME json-ld-lobid-team
sh index.sh $LOBID_TEAM $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME &

LOBID_COLLECTIONS=output/lobid-collections
INDEX_NAME=lobid-collections-$TIME
sh convert.sh hbzlod/lobid-collections $LOBID_COLLECTIONS http://lobid.org/resource $INDEX_NAME json-ld-lobid-collection
sh index.sh $LOBID_COLLECTIONS $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME &

GND=output/json-ld-gnd
INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd/ $GND http://d-nb.info/gnd $INDEX_NAME json-ld-gnd
sh index.sh $GND $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME &

RESOURCES=output/json-ld-lobid-resources
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS $INDEX_NAME & # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER "$ALIAS" $INDEX_NAME
