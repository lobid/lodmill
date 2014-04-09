default files = FLUX_DIR;
default target="/files/open_data/closed/lobid-organisation/snapshot/";

"http://gnd-proxy.lobid.org/oai/repository" |
open-oaipmh(dateFrom="2014-04-09",dateUntil="2014-04-09",metadataPrefix="PicaPlus-xml",setSpec="bib") |
decode-xml |
xml-tee | {
	handle-picaxml |	
	morph(files+"morph_zdb-isil-file-pica2ld.xml") |
	encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE.csv",qrfilepath="/files/open_data/closed/lobid-organisation/qrcodes/",doapilookup="true") |
	triples-to-rdfmodel(input="TURTLE")|
	write-rdfmodel(property="http://purl.org/lobid/lv#isil",endIndex="2",startIndex="0", fileSuffix="nt",serialization="N-TRIPLE",target=target+"lod/")
}{
	split-xml(entityname="metadata") |
	write-xml(startindex="0", endindex="2",target=target+"xml/",property="/harvest/metadata/*[local-name() = 'record']/*[local-name() = 'global']/*[local-name() = 'tag'][@id='008H']/*[local-name() = 'subf'][@id='e']")
};
