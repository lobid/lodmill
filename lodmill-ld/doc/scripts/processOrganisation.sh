#!/bin/sh

if [ ! $# -eq 1 ]
then
  echo "Usage: `basename $0` ALIAS"
  exit 65
fi

ALIAS=$1

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=lobid-organisations-$TIME
sh convert.sh hbzlod/lobid-organisations/ http://lobid.org/organisation $INDEX_NAME json-ld-lobid-orgs "$ALIAS" COLLECT
