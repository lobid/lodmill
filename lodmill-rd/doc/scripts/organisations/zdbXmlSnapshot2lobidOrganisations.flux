// Attention! Don't set "DoApiLookup" to "true" without a reason when dispatching full dumps, because the OSM api will get queried for every single 40k entry

default files = FLUX_DIR;
default source="/files/open_data/closed/lobid-organisation/snapshot/";
default target=source;
qrpath="/files/open_data/closed/lobid-organisation/qrcodes/";

source+"/xml" |
read-dir(recursive="true")|
open-file |
decode-xml |
handle-picaxml |	
morph(files+"morph_zdb-isil-file-pica2ld.xml") |
encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE.csv",qrfilepath=qrpath,doapilookup="false") |
triples-to-rdfmodel(input="TURTLE")|
write-rdfmodel(property="http://purl.org/lobid/lv#isil",endIndex="2",startIndex="0", fileSuffix="nt",serialization="N-TRIPLE",target=target+"/lod/")
;
