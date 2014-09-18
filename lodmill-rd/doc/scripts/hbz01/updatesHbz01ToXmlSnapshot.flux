default files = FLUX_DIR;
morphDirectory = files+"../../../src/main/resources/";

"/home/data/demeter/alephxml/clobs/update/20140818_20140819.tar.bz2"|
open-file(compression="BZIP2") |
open-tar|
decode-xml |
xml-tee | {
        split-xml(entityname="ListRecords") |
        write-xml(encoding="utf8",filesuffix="",compression="bz2",startindex="2", endindex="7",target="/files/open_data/closed/hbzvk/snapshot",property="/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']")
        } {
        handle-mabxml |
        morph(morphDirectory+"morph-hbz01-to-lobid.xml") |
stream-tee | {
        encode-stats(filename="tmp.stats.csv")
}{
        stream-tee | {
                encode-ntriples |
                triples-to-rdfmodel(input="N-TRIPLE") |
                write-rdfmodel-mysql(property="http://purl.org/lobid/lv#hbzID",  dbname="lobid", tablename="resourcesUpdates", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
        }{
                morph(morphDirectory+"morph-nwbibhbz01-buildGeoOsmUrl.xml") |
                extract-literals |
                open-http |
                decode-json |
                stream-tee | {
                        morph(morphDirectory+"morph-osmResult-buildGeonamesLatLonUrl.xml") |
                        extract-literals |
                        open-http |
                        decode-json |
                        morph(morphDirectory+"morph-jsonGeonames2mysqlRow.xml") |
                        write-mysql(dbname="lobid", tablename="NrwPlacesGeonamesId", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
                }{
                        morph(morphDirectory+"morph-jsonOsm2mysqlRow.xml") |
                        write-mysql(dbname="lobid", tablename="NrwPlacesOsmUrl", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
                }
        }
  }
};
