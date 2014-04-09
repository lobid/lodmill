#!/bin/bash
. prerequisites.sh
java -classpath classes:$TARGET$JAR:src/main/resources org.culturegraph.mf.Flux zdbXmlSnapshot2lobidOrganisations.flux >  zdbXmlSnapshot2lobidOrganisations.log 2>&1

TARGET_PATH=/files/open_data/closed/lobid-organisation/
TARGET_FN=${TARGET_PATH}/lobid-organisationZDB.nt
rm $TARGET_FN
find  ${TARGET_PATH}/snapshot/ -name "*.nt" | xargs cat >> $TARGET_FN

bash moveToHadoop.sh
