#!/bin/sh

if [ ! $# -eq 1 ]
then
  echo "Usage: `basename $0` ALIAS"
  exit 65
fi

ALIAS=$1

TIME=`date '+%Y%m%d-%H%M%S'`

INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/,extlod/gnd/,extlod/dewey_preprocessed.nt,enrich/ http://lobid.org/resource $INDEX_NAME json-ld-lobid NOALIAS COLLECT

INDEX_NAME=lobid-resources-$TIME
sh convert.sh hbzlod/lobid-resources/ http://lobid.org/item $INDEX_NAME json-ld-lobid-item "$ALIAS"
