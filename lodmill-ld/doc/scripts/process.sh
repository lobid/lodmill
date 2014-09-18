#!/bin/sh

if [ ! $# -eq 1 ]
then
  echo "Usage: `basename $0` ALIAS"
  exit 65
fi

ALIAS=$1

TIME=`date '+%Y%m%d-%H%M%S'`

NWBIB=output/json-ld-nwbib
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib.nt $NWBIB http://purl.org/lobid/nwbib# $INDEX_NAME json-ld-nwbib "$ALIAS"

NWBIB_SPATIAL=output/json-ld-nwbib-spatial
INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib-spatial.nt $NWBIB_SPATIAL http://purl.org/lobid/nwbib-spatial $INDEX_NAME json-ld-nwbib-spatial "$ALIAS"

ORGANISATIONS=output/json-ld-lobid-organisations
INDEX_NAME=lobid-organisations-$TIME
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation $INDEX_NAME json-ld-lobid-orgs "$ALIAS"

LOBID_TEAM=output/lobid-team
INDEX_NAME=lobid-team-$TIME
sh convert.sh hbzlod/lobid-team $LOBID_TEAM http://lobid.org/team $INDEX_NAME json-ld-lobid-team "$ALIAS"

LOBID_COLLECTIONS=output/lobid-collections
INDEX_NAME=lobid-collections-$TIME
sh convert.sh hbzlod/lobid-collections $LOBID_COLLECTIONS http://lobid.org/resource $INDEX_NAME json-ld-lobid-collection "$ALIAS"

GND=output/json-ld-gnd
INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd/ $GND http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "$ALIAS"

RESOURCES=output/json-ld-lobid-resources
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources-All/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ $RESOURCES http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS COLLECT

ITEMS=output/json-ld-lobid-items
INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources-All/ $ITEMS http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS"
