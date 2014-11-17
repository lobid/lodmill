default files = FLUX_DIR;

"/files/open_data/closed/hbzvk/snapshot/"|
read-dir(recursive="true")|
open-file(compression="BZIP2") |
decode-xml |
handle-mabxml |
morph(files+"morph-hbz01-to-lobid.xml") |
stream-tee | {
	encode-stats(filename="tmp.stats.csv")
}{
	encode-ntriples |
	triples-to-rdfmodel(input="N-TRIPLE") |
	write-rdfmodel-mysql(property="http://purl.org/lobid/lv#hbzID",  dbname="lobid", tablename="resourcesAll", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
};
