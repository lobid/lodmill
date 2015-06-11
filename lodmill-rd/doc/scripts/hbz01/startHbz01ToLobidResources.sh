#!/bin/bash

BRANCH=$1
FILE=$2
INDEX_NAME=$3
INDEX_ALIAS_SUFFIX=$4
ES_NODE=$5
ES_CLUSTER_NAME=$6
UPDATE_NEWEST_INDEX=$7

echo "You specified:
$1 $2 $3 $4 $5 $6 $7"

if [ $# -lt 7 ]
	then
	echo "Usage: <BRANCH> <FILE> <INDEX_NAME> <INDEX_ALIAS_SUFFIX> <ES_NODE> <ES_CLUSTER_NAME> <UPDATE_NEWEST_INDEX>

example parameter:

master
files/open_data/closed/hbzvk/index.hbz-nrw.de/alephxml/clobs/baseline/DE-605-aleph-2015050310.tar.gz 
lobid-resources 
-staging  
quaoar2.hbz-nrw.de 
quaoar 
update

<INDEX_ALIAS_SUFFIX> may be:
	'NOALIAS': will not set a new index alias

<UPDATE_NEWEST_INDEX> may be:
	'update' : most recent index will be updated
	'exact' : a timestamp suffix in the index name isn't mandatory (it would be created if not already set in the index name)
	All other values will result in creating an index if the giving one does not exist.  If it should be existent already, it will be updated.

You may give as 8th parameter a file containing a list of filenames. These will be transformed and indexed the same way as described above.
"
	exit 65
fi

cd ../../../
git checkout $BRANCH
mvn clean assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests=true -DskipIntegrationTests

function indexFile() {
	mvn exec:java -Dexec.mainClass="org.lobid.lodmill.run.MabXml2lobidJsonEs" -Dexec.args="$1 $INDEX_NAME $INDEX_ALIAS_SUFFIX $ES_NODE $ES_CLUSTER_NAME $UPDATE_NEWEST_INDEX"
}

indexFile $FILE

# optionally a file with a list of file names
if [ -n "$8" ]; then
	echo "Take you file $8 with the list into account"
	for i in $(cat $8); do
		indexFile $i
	done
fi

exit 0
