#!/bin/bash
# parameters:
#		1.: the branch to be checked out. Defaults to master
#		2.: suffix to hdfs location, useful if testing. Defaults to nothing.

# Caution if using with a test set:
# If you want to transform a test set, make sure to give "test" as
# second parameter. Also make sure to use another mysql instance -
# otherwise the test data will be feeded into production system.

# description: Transform xml MAB2 Clobs single files in parallel.
# Because the single xml files represents a snapshot, this is fine.
# - transformes MAB2 XML Clobs into  NTriples
# - sink is a mysql db
# - mysql dump into hdfs

export PATH="$PATH:/opt/hadoop/hadoop/bin/"

FLUX=hbz01-to-lobid.flux
# get the newest code and build it
BRANCH=$1
if [ -z $BRANCH ]; then
        BRANCH="master"
fi

echo "Going checkout $BRANCH ..."
git stash # to avoid possible conflicts
cd ../../.. ; git checkout $BRANCH ; git pull
mvn assembly:assembly -DdescriptorId=jar-with-dependencies  -Dwith-DskipTests=true -Dmysql.classifier=linux-amd64 -Dmysql.port=33061

JAR=$(basename $(ls target/lodmill-rd-*jar-with-dependencies.jar))
echo $JAR
cd -
# TODO should be done with maven: overwrite the flux-command from metafacture with that from lodmill 
cd ../../../target/ ; cp ../src/main/resources/flux-commands.properties ./; jar uf $JAR flux-commands.properties; cp ../src/main/resources/morph-functions.properties ./ ; jar uf $JAR morph-functions.properties ; mkdir schemata; cp ../src/main/resources/schemata/* schemata/ ; jar uf $JAR schemata/* ; cd -

LODMILL_RD_JAR=../../../target/$JAR

function wait_load() {
# wait if load >1 , that is: wait until the machine has finished e.g. yesterdays updates
# ATTENTION: not safe enough - consider yesteradys yesterdays files! A fifo is needed!
while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 7 ]; do
  printf "."
  sleep 60
done
}

cp ../../../src/test/resources/sigel2isilMap.csv ./
cp ../../../src/main/resources/iso639xToIso639-3-Map ./

wait_load

SNAPSHOT_PATH=/files/open_data/closed/hbzvk/snapshot/
FILE_PATTERN="[0123456789]*"
if [ "$2" = "test" ]; then
	SNAPSHOT_PATH=/files/open_data/closed/hbzvk
	FILE_PATTERN=test
fi

if [ ! -d tmpFlux/$SNAPSHOT_PATH ]; then
	echo "mkdir tmpFlux/$SNAPSHOT_PATH"
	mkdir -p tmpFlux/$SNAPSHOT_PATH 
fi

# find all snapshot XML bz2 clobs directories and make a flux for them
# first, remove if old are theres:
rm tmpFlux/files/open_data/closed/hbzvk/snapshot/*.flux
find $SNAPSHOT_PATH -maxdepth 1  -type d  -name "$FILE_PATTERN" | parallel --gnu --load 20 "echo {}; sed 's#/files/open_data/closed/hbzvk/snapshot/.*#{}\"\|#g' $FLUX > tmpFlux/{}.$FLUX"

cp ../../../src/main/resources/*.xml  tmpFlux/$SNAPSHOT_PATH
#always use the newest morph. TODO: should be copied via maven.
jar xf $LODMILL_RD_JAR  ../../../src/main/resources/morph-hbz01-to-lobid.xml

echo "starting in 50 seconds .. break now if ever!"
sleep 50
# drop old table => no framgmentation
echo "DROP TABLE resources" | mysql -udebian-sys-maint -ptzSblDEUGC1XhJB7 lobid

find tmpFlux -type f -name "*.flux"| parallel --gnu --load 20 "java -classpath classes:$LODMILL_RD_JAR:src/main/resources org.culturegraph.mf.Flux {}" # does not work: -Djava.util.logging.config.file=/home/lod/lobid-resources/logging.properties 

wait_load
HDFS_FILE="hbzlod/lobid-resources$2/resources-dump.nt"
hadoop fs -rm $HDFS_FILE
date
time bash -x mysql_bash.sh | hadoop dfs -put - $HDFS_FILE
