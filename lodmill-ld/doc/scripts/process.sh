#!/bin/sh

ORGANISATIONS=output/json-ld-lobid-organisations
sh convert.sh hbzlod/lobid-organisations/ $ORGANISATIONS http://lobid.org/organisation lobid-orgs
sh index.sh $ORGANISATIONS 193.30.112.170 quaoar

GND=output/json-ld-gnd
sh convert.sh extlod/GND.nt $GND http://d-nb.info/gnd gnd
sh index.sh $GND 193.30.112.170 quaoar

RESOURCES=output/json-ld-lobid-resources
sh convert.sh hbzlod/lobid-resources/,hbzlod/owlSameAs/,hbzlod/orcaHasUrn/,extlod/gnd/,extlod/dewey.nt,enrich/ $RESOURCES http://lobid.org/resource lobid
sh index.sh $RESOURCES 193.30.112.170 quaoar

ITEMS=output/json-ld-lobid-items
sh convert.sh hbzlod/lobid-resources/,enrich/ $ITEMS http://lobid.org/item lobid-items
sh index.sh $ITEMS 193.30.112.170 quaoar
