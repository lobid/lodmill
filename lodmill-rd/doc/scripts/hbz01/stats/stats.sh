#!/bin/bash
# Make some statistics about the nwbib dataset got via
# https://github.com/hbz/lobid/blob/master/test/tests/SampleUsage.java
# Create the beacons and copy them onto webserver, see
# http://lobid.org/download/beacons/

# see https://github.com/hbz/nwbib/issues/102
# see https://github.com/hbz/nwbib/issues/106

NWBIB_CONTRIBUTORS="nwbibContributors.bf"
TIMESTAMP="$(date +%Y-%m-%d)"

echo " #FORMAT: BEACON
#VERSION: 0.1
#PREFIX: http://d-nb.info/gnd/
#TARGET: http://lobid.org/nwbib/search?person={ID}
#FEED: http://lobid.org/download/beacons/$NWBIB_CONTRIBUTORS
#CONTACT: lobid-Team im hbz <semweb@hbz-nrw.de>
#NAME: Nordrhein-Westfälische Bibliographie (NWBib)
#INSTITUTION: Hochschulbibliothekszentrum des Landes Nordrhein-Westfalen (hbz)
#MESSAGE Literatur dieses Beitragenden in der Nordrhein-Westfälischen Bibliographie (NWBib)
#DESCRIPTION: This is an automatically generated BEACON file for all contributors of works catalogued in the Northrhine-Westphalian Bibliography.
#TIMESTAMP: $TIMESTAMP
#UPDATE: unknown
#LINK: http://www.w3.org/2000/01/rdf-schema#seeAlso
#EXAMPLES: 118515470
" > $NWBIB_CONTRIBUTORS

grep "ulary/relators\|bibo/transl\|bibo/editor\|terms/cont\|terms/crea" NWBib_all.nt |sed 's#.*info/gnd/\(.*\)>.*#\1#'|sort | uniq -c | awk '{print $2 "|" $1}' >> $NWBIB_CONTRIBUTORS

# see https://github.com/hbz/nwbib/issues/106
NWBIB_SUBJECTS="nwbibSubjects.bf"
echo "#FORMAT: BEACON
#VERSION: 0.1
#PREFIX: http://d-nb.info/gnd/
#TARGET: http://lobid.org/nwbib/search?subject={ID}
#FEED: http://lobid.org/download/beacons/$NWBIB_SUBJECTS
#CONTACT: lobid-Team im hbz <semweb@hbz-nrw.de>
#NAME: Nordrhein-Westfälische Bibliographie (NWBib)
#INSTITUTION: Hochschulbibliothekszentrum des Landes Nordrhein-Westfalen (hbz)
#MESSAGE Literatur über diese Ressource in der Nordrhein-Westfälischen Bibliographie (NWBib)
#DESCRIPTION: This is an automatically generated BEACON file for all contributors of works catalogued in the Northrhine-Westphalian Bibliography.
#TIMESTAMP: $TIMESTAMP
#UPDATE: unknown
#LINK: http://xmlns.com/foaf/0.1/isPrimaryTopicOf
#EXAMPLES: 118515470
" > $NWBIB_SUBJECTS

grep "terms/subject.*d-nb" NWBib_all.nt |sed 's#.*info/gnd/\(.*\)>.*#\1#'|sort | uniq -c | awk '{print $2 "|" $1}' >> $NWBIB_SUBJECTS

scp *.bf emphytos:src/lobid.org/download/beacons/
