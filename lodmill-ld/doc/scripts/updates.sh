#/bin/sh
OUT = "output/json-ld-update"
sh convert-lobid.sh extlod,update,enrich $OUT > logs/lobid-resources-updates-convert.log
sh index.sh $OUT > logs/lobid-resources-updates-index.log
/opt/hadoop/hadoop/bin/hadoop fs -mv update/* hbzlod/
