default files = FLUX_DIR;
morphDirectory = files+"../../../src/main/resources/";

"/files/open_data/open/DE-605/mabxml/DE-605-aleph-update-marcxchange-20141201-20141202.tar.gz"|
open-file |
open-tar|
decode-xml |
handle-mabxml |
morph(morphDirectory+"morph-hbz01-to-lobid.xml") |
stream-tee | {
        encode-stats(filename="logStats/tmp.stats.csv.DE-605-aleph-update-marcxchange-20150202-20150203.tar.gz")
        }{
        encode-ntriples |
        triples-to-rdfmodel(input="N-TRIPLE") |
        write-rdfmodel-mysql(property="http://purl.org/lobid/lv#hbzID",  dbname="lobid", tablename="resourcesUpdates", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
        }
};
