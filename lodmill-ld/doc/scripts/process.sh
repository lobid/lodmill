#!/bin/sh

if [ ! $# -eq 1 ]
then
  echo "Usage: `basename $0` ALIAS"
  exit 65
fi

ALIAS=$1

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib.nt http://purl.org/lobid/nwbib# $INDEX_NAME json-ld-nwbib "$ALIAS"

INDEX_NAME=nwbib-$TIME
sh convert.sh hbzlod/nwbib/nwbib-spatial.nt http://purl.org/lobid/nwbib-spatial $INDEX_NAME json-ld-nwbib-spatial "$ALIAS"

INDEX_NAME=lobid-organisations-$TIME
sh convert.sh hbzlod/lobid-organisations/ http://lobid.org/organisation $INDEX_NAME json-ld-lobid-orgs "$ALIAS"

INDEX_NAME=lobid-team-$TIME
sh convert.sh hbzlod/lobid-team http://lobid.org/team $INDEX_NAME json-ld-lobid-team "$ALIAS"

INDEX_NAME=lobid-collections-$TIME
sh convert.sh hbzlod/lobid-collections http://lobid.org/resource $INDEX_NAME json-ld-lobid-collection "$ALIAS"

INDEX_NAME=gnd-$TIME
sh convert.sh extlod/gnd/ http://d-nb.info/gnd $INDEX_NAME json-ld-gnd "$ALIAS"

sh processResources.sh $ALIAS All
