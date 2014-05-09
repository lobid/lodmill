#!/bin/bash

# duration: 12 h alone for concat and copying

DIR="/files/open_data/closed/gnd/gnd_snapshot/"
HDFS="hdfs://weywot1.hbz-nrw.de:8020/user/hduser/extlod/gnd/"
function concat() {
  FN=$1
  echo $FN
  echo "going to concat $FN"
  # remove old file
  rm $FN.nt
  find $FN -name "*.nt" -type f | xargs -n10000 cat >> $FN.nt
  hadoop fs -rm $HDFS$(basename $FN).nt; hadoop fs -copyFromLocal $FN.nt $HDFS
}
echo "going to concat, please abort now if you like,  30 seconds left"
sleep 30

for i in $(find $DIR -maxdepth 1 -mindepth 1 -type d); do
  concat $i &
  sleep 2
  echo "waiting for load to be sanitized"
  while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 3 ]; do
    printf "."
    sleep 60
  done
done
