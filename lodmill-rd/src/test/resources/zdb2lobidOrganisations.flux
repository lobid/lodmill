default files = FLUX_DIR;
// use these values when processing the ZDB OAI-PMH dumps:
entityname_var="metadata";
property_var="/harvest/metadata/*[local-name() = 'record']/*[local-name() = 'global']/*[local-name() = 'tag'][@id='008H']/*[local-name() = 'subf'][@id='e']";
// use these values when processing the ftp fulldumps:
//entityname_var="record";
//property_var="/collection/*[local-name() = 'record']/*[local-name() = 'global']/*[local-name() = 'tag'][@id='008H']/*[local-name() = 'subf'][@id='e']";

files+"Bibdat1303pp_sample1.xml" |
open-file |
decode-xml |
xml-tee | {
	handle-picaxml |	
	morph(files+"morph_zdb-isil-file-pica2ld.xml") |
	encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE.csv", qrfilepath="tmp/") |
	triples-to-rdfmodel(input="TURTLE")|
	write-rdfmodel(property="http://purl.org/lobid/lv#isil",endIndex="2",startIndex="0", fileSuffix="nt",serialization="N-TRIPLE",target="tmp")
}{
	split-xml(entityname=entityname_var) |
	write-xml(startindex="0", endindex="2",target="tmp",property=property_var)
};