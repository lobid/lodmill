#!/bin/bash
# description: start with parameters "update" or "fulldump".
# "update" will load data via OAI-PMH and split records and store these
# into filesystem and transform them into ntriples and store these also.
# "fulldump" only necessary at the beginning, when filesystem is blank
# and the fulldump as a base is to be splitted and stored as single files.

# get the newest code and build it
cd ../../.. ; git pull;  mvn assembly:assembly; cd -

LODMILL_RD_JAR=../../../target/lodmill-rd-1.1.0-SNAPSHOT-jar-with-dependencies.jar
# TODO:  copy lobid's flux-commands over metafacture's using maven
cp ../../../src/main/resources/flux-commands.properties ./
jar uf $LODMILL_RD_JAR flux-commands.properties

function updateAndStartFlux() {
	DATE_FROM=$1
	DATE_UNTIL=$2
	echo "$DATE_FROM zu $DATE_UNTIL"
  # update the flux
  FLUX_LINE="open-oaipmh(dateFrom=\"$DATE_FROM\",dateUntil=\"$DATE_UNTIL\",metadataPrefix=\"RDFxml\",setSpec=\"authorities\") |"
  sed -i "s#.*dateUntil=\".*#$FLUX_LINE#" xmlSplitterRdfWriter.flux 
	echo "####################"
	echo "Flux:"
	cat xmlSplitterRdfWriter.flux
	echo "you have 15 seconds to cancel the script ...."
	sleep 15
  # updates:
  java -classpath classes:$LODMILL_RD_JAR:src/main/resources org.culturegraph.mf.Flux xmlSplitterRdfWriter.flux
}

function update() {
	SEC_WEEK=604800
	SEC_NOW=$(date +%s)
	# the beginning is the former end
	dateFrom=$(grep dateUntil  xmlSplitterRdfWriter.flux | cut -d '"' -f4 )
	SEC_DATE_FROM=$(date --date="$dateFrom" +%s)

	while [ $(expr $SEC_DATE_FROM + $SEC_WEEK) -le $SEC_NOW ]; do
		DATE_FROM=$(date --date="@$SEC_DATE_FROM" +%Y-%m-%d)
		SEC_DATE_FROM=$(expr $SEC_DATE_FROM + $SEC_WEEK)
		DATE_UNTIL=$(date --date="@$SEC_DATE_FROM" +%Y-%m-%d)
		updateAndStartFlux $DATE_FROM $DATE_UNTIL
	done

	DATE_FROM=$(date --date="@$SEC_DATE_FROM" +%Y-%m-%d)
	DATE_NOW=$(date +%Y-%m-%d)
	if [ $(echo $DATE_UNTIL) != $(echo $DATE_NOW) ]; then
		updateAndStartFlux $DATE_UNTIL $DATE_NOW
	fi
}

function fulldump() {
	java -classpath classes:$LODMILL_RD_JAR org.culturegraph.mf.Flux xmlSplitterRdfWriterFulldump.flux
}

case $1 in
	update) update ;;
	fulldump) fulldump ;;
esac 

echo "DONE !"
