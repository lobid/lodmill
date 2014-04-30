#!/bin/sh

ES_SERVER=193.30.112.84
ES_CLUSTER=aither
          
TIME=`date '+%Y%m%d-%H%M%S'`
          
LOBID_PERSONS=output/hbzlod/lobid-persons
sh convert.sh hbzlod/lobid-persons $LOBID_PERSONS http://lobid.org/person lobid-persons-$TIME json-ld-lobid-person
sh index.sh $LOBID_PERSONS $ES_SERVER $ES_CLUSTER ""
      
LOBID_PROVENANCE=output/lobid-provenance
sh convert.sh hbzlod/lobid-provenance/ $LOBID_PROVENANCE http://lobid.org/resource lobid-provenance-$TIME json-ld-lobid-provenance 
sh index.sh $LOBID_PROVENANCE $ES_SERVER $ES_CLUSTER ""
