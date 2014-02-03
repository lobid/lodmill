#!/bin/bash
# description: when called, get an etherpad page (hopefully valid turtle)
# and transform that to ntriples. Copy that to hdfs and index into SPARQL endpoint.

ETHERPAD_PAGE=$1
DATASET=$2
echo "<html><head><title>index $2</title></head><body>"
echo "index http://etherpad.lobid.org/p/$ETHERPAD_PAGE/export/txt"
echo "<br>"
echo "<h2>"
rapper -i turtle http://etherpad.lobid.org/p/$ETHERPAD_PAGE/export/txt > $DATASET.nt 2>error.txt
if [ $(echo $?) -eq 0 ]; then
        echo "<br>On removing old file from hdfs ...</br>"
        hadoop fs -rm hdfs://weywot1.hbz-nrw.de:8020/user/hduser/hbzlod/lobid-$DATASET/$DATASET.nt
        echo "<br>On copying to hdfs ...</br>"
        hadoop fs -copyFromLocal $DATASET.nt hdfs://weywot1.hbz-nrw.de:8020/user/hduser/hbzlod/lobid-$DATASET 2>>error.txt
        if [ $(echo $?) -eq 0 ]; then
                echo "OK :) <br><br>"
                 curl -X DELETE "http://localhost:8000/data/http://lobid.org/graph/$DATASET"
                echo "<br> "
                curl --data-urlencode data@$DATASET.nt -d "graph=http%3A%2F%2Flobid.org%2Fgraph%2F$DATASET" -d 'mime-type=text/plain' http://localhost:8000/data/
                fi 
                else
                        echo ":( Something went wrong :<br><br>"
fi
cat error.txt

echo "</body></html>"
