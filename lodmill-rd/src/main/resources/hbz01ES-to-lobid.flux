default files = FLUX_DIR;

"" |
read-elasticsearch(clustername="quaoar", hostname="193.30.112.172", indexname="hbz01", batchsize="1000", shards="0,1,2,3,4") |
decode-xml |
handle-mabxml |
morph(files+"morph-hbz01-to-lobid.xml") |
stream-tee | {
	encode-stats(filename="tmp.stats.csv")
}{
	stream-tee | {
		encode-ntriples |
		triples-to-rdfmodel(input="N-TRIPLE") |
		write-rdfmodel-mysql(property="http://purl.org/lobid/lv#hbzID",  dbname="lobid", tablename="resources", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
	}{
		morph(files+"morph-nwbibhbz01-buildGeoOsmUrl.xml") |
		extract-literals |
		open-http |
		decode-json |
		stream-tee | {
			morph(files+"morph-osmResult-buildGeonamesLatLonUrl.xml") |
			extract-literals |
			open-http |
			decode-json |
			morph(files+"morph-jsonGeonames2mysqlRow.xml") |
			write-mysql(dbname="lobid", tablename="NrwPlacesGeonamesId", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
		}{
			morph(files+"morph-jsonOsm2mysqlRow.xml") |
			write-mysql(dbname="lobid", tablename="NrwPlacesOsmUrl", username="debian-sys-maint", password="tzSblDEUGC1XhJB7", dbprotocolandadress="jdbc:mysql://localhost:3306/")
		}
	}
};
