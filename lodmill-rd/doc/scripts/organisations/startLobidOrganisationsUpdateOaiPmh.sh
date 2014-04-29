#!/bin/bash
# the beginning is the former end
DATE_FROM=$(grep dateUntil  oaipmh-zdbIsil2ld.flux | cut -d '"' -f4 )
DATE_UNTIL=$(date +%Y-%m-%d)

# source some prerequisites
. prerequisites.sh

# update the flux
FLUX_LINE="open-oaipmh(dateFrom=\"$DATE_FROM\",dateUntil=\"$DATE_UNTIL\",metadataPrefix=\"PicaPlus-xml\",setSpec=\"bib\") |"
sed -i "s#.*dateUntil=\".*#$FLUX_LINE#" oaipmh-zdbIsil2ld.flux

java -classpath classes:$TARGET$JAR:src/main/resources org.culturegraph.mf.Flux oaipmh-zdbIsil2ld.flux > startLobidOrganisationsUpdateOaiPmh.sh.log 2>&1

QRCODE_PATH="/files/open_data/closed/lobid-organisation/qrcodes/"
# copy only files which are max 24 hours old
for i in $(find $QRCODE_PATH -mtime 0); do
    scp $i lobid@emphytos:src/lobid.org/media/$(basename $i)
done

TARGET_PATH=/files/open_data/closed/lobid-organisation/
TARGET_FN=${TARGET_PATH}/lobid-organisationZDB.nt
rm $TARGET_FN
find  ${TARGET_PATH}/snapshot/lod/ | xargs cat >> $TARGET_FN

bash moveToHadoop.sh
