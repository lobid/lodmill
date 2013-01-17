#!/bin/sh

ORGANISATIONS=output/json-ld-lobid-organisations
sh convert-orgs.sh hbzlod/lobid-organisations.nt $ORGANISATIONS > logs/lobid-organisations-convert.log
sh index.sh $ORGANISATIONS > logs/lobid-organisations-index.log

GND=output/json-ld-gnd
sh convert-gnd.sh extlod/GND.nt $GND > logs/gnd-convert.log
sh index.sh $GND > logs/gnd-index.log

RESOURCES=output/json-ld-lobid-resources
sh convert-lobid.sh extlod,hbzlod $RESOURCES > logs/lobid-resources-convert.log
sh index.sh $RESOURCES > logs/lobid-resources.log

