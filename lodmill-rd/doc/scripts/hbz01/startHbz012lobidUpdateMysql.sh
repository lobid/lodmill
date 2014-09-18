#!/bin/bash
# parameters:
#	1.: the branch to be checked out. Defaults to master
#	2.: suffix (case sensitive!) to hdfs location, discriminate:
#		"Test", "Updates" and "All".
#
# Caution if using with a test set:
# If you want to transform a test set, make sure to use another
# mysql instance - otherwise the test data will be feeded into production system.

# if in "Test" or "All" mode:
# Transform xml MAB2 Clobs single files in parallel.
# Because the single xml files represents a snapshot, this is fine.
# - transforms MAB2 XML Clobs into  NTriples

# if in "Updates" mode:
# - Transform xml-MAB2-Update-Tar-Clobs sequentially. It is important to not do
# this in parallel when having multiple files, because older updates could
# overwrite newer ones.
# - XMLs are splitted and lonely xml entities copied into fs. There, they reside
# as base for a new snapshotted update

# for every mode:
# - sink is a mysql db
# - mysql dumps into hdfs

function usage(){
	printf "Usage: `basename $0` BRANCH {Test|Updates|All}\n"
	exit 65
}

if [ $# -ne 2 ]
then
	usage
fi
BRANCH=$1

export PATH="$PATH:/opt/hadoop/hadoop/bin/"

echo "Going checkout $BRANCH ..."
#git stash # to avoid possible conflicts
cd ../../.. ; git checkout $BRANCH ; git pull
mvn clean assembly:assembly -DdescriptorId=jar-with-dependencies  -Dwith-DskipTests=true -Dmysql.classifier=linux-amd64 -Dmysql.port=33061
JAR=$(basename $(ls target/lodmill-rd-*jar-with-dependencies.jar))
LODMILL_RD_JAR=../../../target/$JAR
cd -
cp ../../../src/main/resources/hbz01-to-lobid.flux ./
# TODO should be done with maven: overwrite the flux-command from metafacture with that from lodmill 
cd ../../../target/ ; cp ../src/main/resources/flux-commands.properties ./; jar uf $JAR flux-commands.properties; cp ../src/main/resources/morph-functions.properties ./ ; jar uf $JAR morph-functions.properties ; mkdir schemata; cp ../src/main/resources/schemata/* schemata/ ; jar uf $JAR schemata/* ; cd -
cp ../../../src/test/resources/sigel2isilMap.csv ./
cp ../../../src/main/resources/iso639xToIso639-3-Map.tsv ./

function wait_load() {
# wait if load exceeds maximum
# ATTENTION: not safe enough - consider yesteradys yesterdays files! A fifo is needed!
while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 7 ]; do
	printf "."
	sleep 60
done
}

function all() {
FLUX=hbz01-to-lobid.flux
TMP_FLUX_PARENT="tmpFlux/files/open_data/closed/hbzvk/snapshot/"
if [ ! -d tmpFlux/$SNAPSHOT_PATH ]; then 
        echo "mkdir tmpFlux/$SNAPSHOT_PATH"
        mkdir -p tmpFlux/$SNAPSHOT_PATH 
fi 
# find all snapshot XML bz2 clobs directories and make a flux for them
# first, remove if old are theres:
rm $TMP_FLUX_PARENT/*.flux
mkdir $TMP_FLUX_PARENT
find $SNAPSHOT_PATH -maxdepth 1  -type d  -name "$FILE_PATTERN" | parallel --gnu --load 20 "echo {}; sed 's#/files/open_data/closed/hbzvk/snapshot/.*#{}\"\|#g' $FLUX > tmpFlux/{}.$FLUX"
cp ../../../src/main/resources/*.xml  tmpFlux/$SNAPSHOT_PATH
#always use the newest morph. TODO: should be copied via maven.
jar xf $LODMILL_RD_JAR  ../../../src/main/resources/morph-hbz01-to-lobid.xml
# drop old table => no framgmentation
echo "DROP TABLE resources" | mysql -udebian-sys-maint -ptzSblDEUGC1XhJB7 lobid
find tmpFlux -type f -name "*.flux"| parallel --gnu --load 20 "java -classpath classes:$LODMILL_RD_JAR:src/main/resources org.culturegraph.mf.Flux {}" # does not work: -Djava.util.logging.config.file=/home/lod/lobid-resources/logging.properties 
}

function update(){
FLUX=updatesHbz01ToXmlSnapshot.flux
UPDATE_FILES_LIST=toBeUpdateFilesXmlClobs.txt
# drop table first so we will have only the updates
echo "DROP TABLE resourcesUpdates" | mysql -udebian-sys-maint -ptzSblDEUGC1XhJB7 lobid
for i in $(cat $UPDATE_FILES_LIST); do
	echo "going to work on $i..."
	sed -i s#/home/data/demeter/alephxml/clobs/update/.*\"#$i\"#g $FLUX ;
	sed -i s#tmp.stats.csv.*#tmp.stats.csv.$(basename $i .tar.bz2)\"\)#g $FLUX;
	# wait when load >20
	wait_load
	java -classpath classes:$LODMILL_RD_JAR:src/main/resources org.culturegraph.mf.Flux $FLUX
	# delete the first line in the update files list if everything was ok, else exit
	if [ $? -eq "0" ]; then
		echo "Status: success. Going to remove the line from $UPDATE_FILES_LIST"
		sed -i '1 d' $UPDATE_FILES_LIST
	else
		echo "Status: failed. Not removing the line from $UPDATE_FILES_LIST"
		exit
	fi
done
}

wait_load

SNAPSHOT_PATH=/files/open_data/closed/hbzvk/snapshot/
FILE_PATTERN="[0123456789]*"
if [ "$2" = "Test" ]; then
	SNAPSHOT_PATH=/files/open_data/closed/hbzvk
	FILE_PATTERN=test
	all
else if [ "$2" = "Updates" ]; then
	update
else if [ "$2" = "All" ]; then
	all
else usage
fi; fi; fi

wait_load
HDFS_FILE="hbzlod/lobid-resources-$2/resources-dump.nt"
echo "Dumping to $HDFS_FILE ..."

hadoop fs -rm $HDFS_FILE
date
time bash -x mysql_bash.sh $2 | hadoop dfs -put - $HDFS_FILE