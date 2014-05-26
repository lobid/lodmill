#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` ES-SERVER ES-CLUSTER"
  exit 65
fi

ES_SERVER=$1
ES_CLUSTER=$2

TIME=`date '+%Y%m%d-%H%M%S'`

NWBIB=output/json-ld-nwbib
sh convert.sh hbzlod/nwbib/nwbib.nt $NWBIB http://purl.org/lobid/nwbib# nwbib-$TIME json-ld-nwbib
sh index.sh $NWBIB $ES_SERVER $ES_CLUSTER NOALIAS

NWBIB_SPATIAL=output/json-ld-nwbib-spatial
sh convert.sh hbzlod/nwbib/nwbib-spatial.nt $NWBIB_SPATIAL http://purl.org/lobid/nwbib-spatial nwbib-$TIME json-ld-nwbib-spatial
sh index.sh $NWBIB_SPATIAL $ES_SERVER $ES_CLUSTER ""

ORGANISATIONS=output/json-ld-lobid-organisations
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation lobid-organisations-$TIME json-ld-lobid-orgs
sh index.sh $ORGANISATIONS $ES_SERVER $ES_CLUSTER ""

LOBID_TEAM=output/lobid-team
sh convert.sh hbzlod/lobid-team $LOBID_TEAM http://lobid.org/team lobid-team-$TIME json-ld-lobid-team
sh index.sh $LOBID_TEAM $ES_SERVER $ES_CLUSTER ""

LOBID_COLLECTIONS=output/lobid-collections
sh convert.sh hbzlod/lobid-collections $LOBID_COLLECTIONS http://lobid.org/resource lobid-collections-$TIME json-ld-lobid-collection
sh index.sh $LOBID_COLLECTIONS $ES_SERVER $ES_CLUSTER ""

GND=output/json-ld-gnd
sh convert.sh extlod/gnd/ $GND http://d-nb.info/gnd gnd-$TIME json-ld-gnd
sh index.sh $GND $ES_SERVER $ES_CLUSTER ""

RESOURCES=output/json-ld-lobid-resources
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey.nt,enrich/ $RESOURCES http://lobid.org/resource lobid-resources-$TIME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item lobid-resources-$TIME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER ""
