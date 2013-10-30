default files = FLUX_DIR;

files+"Bibdat1303pp_sample1.xml" |
open-file |
decode-xml |
xml-tee | {
	handle-picaxml |	
	morph(files+"morph_zdb-isil-file-pica2ld.xml") |
	encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE.csv", qrfilepath="tmp") |
	triples-to-rdfmodel(input="TURTLE")|
	write-rdfmodel(property="http://purl.org/lobid/lv#isil",endIndex="2",startIndex="0", fileSuffix="nt",serialization="N-TRIPLE",target="tmp")
}{
	split-xml(entityname="metadata") |
	write-xml(startindex="0", endindex="2",target="tmp",property="/harvest/metadata/*[local-name() = 'record']/*[local-name() = 'global']/*[local-name() = 'tag'][@id='008H']/*[local-name() = 'subf'][@id='e']")
};