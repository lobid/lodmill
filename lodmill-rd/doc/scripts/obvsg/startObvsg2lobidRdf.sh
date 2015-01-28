#!/bin/bash

FLUX_MAIN="org.culturegraph.mf.runner.Flux"

cd ../../..
git pull
mvn clean assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests=true -DskipIntegrationTests
JAR=$(basename $(ls target/lodmill-rd-*jar-with-dependencies.jar))
LODMILL_RD_JAR=../../../target/$JAR
cd -
# Doing some setup stuff. TODO should be done with maven: overwrite the flux-command from metafacture with that from lodmill
cd ../../../target/ ; cp ../src/main/resources/flux-commands.properties ./; jar uf $JAR flux-commands.properties; cp ../src/main/resources/morph-functions.properties ./ ; jar uf $JAR morph-functions.properties ; mkdir schemata; cp ../src/main/resources/schemata/* schemata/ ; jar uf $JAR schemata/* ; cd -
cp ../../../src/main/resources/sigel2isilMap.csv ./
cp ../../../src/main/resources/iso639xToIso639-3-Map.tsv ./
cp ../../../src/main/resources/*.xml ./

# using the hbz01 morph, which is nice for a starter. Must be adjusted properly
# to obsvg, e.g. holding information etc.
jar xf $LODMILL_RD_JAR  ../../../src/main/resources/morph-hbz01-to-lobid.xml

# start transformation using the obvsg flux
java -classpath classes:$LODMILL_RD_JAR:src/main/resources $FLUX_MAIN obvsg-to-lobidRdf.flux
