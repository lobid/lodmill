#!/bin/bash
# description: start with parameters "update" or "fulldump".
# "update" will load data via OAI-PMH and spli records and store these
# into filesystem and concatenate to bigger files and copy these
# into hdfs.
# "fulldump" only necessary at the beginning, when filesystem is blank
# and the fulldump as a base is to be splitted and stored as single files.

# duration: 12 h alone for concat and copying

LODMILL_RD_JAR=../../target/lodmill-rd-1.1.0-SNAPSHOT-jar-with-dependencies.jar
# TODO:  copy lobid's flux-commands over metafacture's using maven
jar uf $LODMILL_RD_JAR flux-commands.properties

function update() {
	# the beginning is the former end
	DATE_FROM=$(grep dateUntil  xmlSplitterRdfWriter.flux | cut -d '"' -f4 )
	DATE_UNTIL=$(date +%Y-%m-%d)
	# update the flux
	FLUX_LINE="open-oaipmh(dateFrom=\"$DATE_FROM\",dateUntil=\"$DATE_UNTIL\",metadataPrefix=\"RDFxml\",setSpec=\"authorities\") |"
	sed -i "s#.*dateUntil=\".*#$FLUX_LINE#" xmlSplitterRdfWriter.flux 
	# updates:
	java -classpath classes:$LODMILL_RD_JAR:src/main/resources org.culturegraph.mf.Flux xmlSplitterRdfWriter.flux
}

function fulldump() {
	java -classpath classes:$LODMILL_RD_JAR org.culturegraph.mf.Flux xmlSplitterRdfWriterFulldump.flux
}

case $1 in
	update) update ;;
	fulldump) fulldump ;;
esac 

DIR="/files/open_data/closed/gnd/gnd_snapshot/"
HDFS="hdfs://weywot1.hbz-nrw.de:8020/user/hduser/extlod/gnd/"
function concat() {
  FN=$1
  echo $FN
  echo "going to concat $FN"
  # remove old file
  rm $FN.nt
  find $FN -name "*.nt" -type f | xargs -n10000 cat >> $FN.nt
  ssh hduser@weywot5 ". .bash_profile  ; hadoop fs -rm $HDFS$(basename $FN).nt; hadoop fs -copyFromLocal $FN.nt $HDFS"
}
echo "going tp concat, please abort now if you like,  30 seconds left"
sleep 30

for i in $(find $DIR -maxdepth 1 -mindepth 1 -type d); do
  concat $i &
  sleep 2
  # echo "waiting for load to be sanitized"
  while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 3 ]; do
    printf "."
    sleep 60
  done
done
