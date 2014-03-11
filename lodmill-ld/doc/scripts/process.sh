#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` ES-SERVER ES-CLUSTER"
  exit 65
fi

ES_SERVER=$1
ES_CLUSTER=$2

TIME=`date '+%Y%m%d-%H%M%S'`

ORGANISATIONS=output/json-ld-lobid-organisations
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation lobid-organisations-$TIME json-ld-lobid-orgs
sh index.sh $ORGANISATIONS $ES_SERVER $ES_CLUSTER ""

GND=output/json-ld-gnd
sh convert.sh extlod/gnd/ $GND http://d-nb.info/gnd gnd-$TIME json-ld-gnd
sh index.sh $GND $ES_SERVER $ES_CLUSTER ""

RESOURCES=output/json-ld-lobid-resources
sh convert.sh hbzlod/lobid-resources/,hbzlod/owlSameAs/,hbzlod/orcaHasUrn/,extlod/gnd/,extlod/dewey.nt,enrich/ $RESOURCES http://lobid.org/resource lobid-resources-$TIME json-ld-lobid
sh index.sh $RESOURCES $ES_SERVER $ES_CLUSTER NOALIAS # no alias, index not ready yet, needs items from below

ITEMS=output/json-ld-lobid-items
sh convert.sh hbzlod/lobid-resources/ $ITEMS http://lobid.org/item lobid-resources-$TIME json-ld-lobid-item
sh index.sh $ITEMS $ES_SERVER $ES_CLUSTER ""
