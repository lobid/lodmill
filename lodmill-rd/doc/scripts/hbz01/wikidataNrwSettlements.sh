#/bin/bash
# This script acts as documentation. Building a concordance
# for spatial NRW literals to get geo coordinates for them.
# Uses wikidata as source of data. Because wikidata API lacks
# some functionality we have to do this.

# to get all the settlements in NRW:
curl -g "https://wdq.wmflabs.org/api?q=tree[1198][150][131]%20AND%20claim[31:%28TREE[486972][][279]%29]" > wikidataNrwSettlements.json

# get all values of the array:
cat wikidataNrwSettlements.json | sed  's#.*\[\(.*\)].*#\1#g' > wikidataNrwSettlements.csv

# lookup all IDs and store them as single files
IFS=","; for i in $(cat wikidataNrwSettlements.csv) ; do echo $i ; curl "https://www.wikidata.org/wiki/Special:EntityData/Q$i.json" > wikidataEntities/Q$i.json ;done

# start java. First build it, something like:
mvn  clean install  -Dmysql.classifier=linux-x86 -Dmysql.port=33061
# This creates a rather incomplete and concordance with in part
# data which will not be used after all (e.g. pathes to jpegs).
# because of the way the json decoder works (emits just plain
# key-value) and the rather complex structure of the wikidata
# json data. Gains ~ 2 times more strings (~22k) of mapping as
# the size of the data set (~6,7k) thanks to the different labels
# (again, not ALL possible labels are taken into accout). i
# It's a start.
mvn exec:java -Dexec.mainClass="org.lobid.lodmill.WikidataGeoJson2Mysql" -Dexec.classpathScope="test"


