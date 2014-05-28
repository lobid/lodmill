// Attention! Don't set "DoApiLookup" to "true" too often when dispatching full dumps, because the OSM api will get queried for all records which don't have already geo location.
// If you want to be sure that OSM-API lookups will be done for every of the 40k records, delete also the "latlon.ser" file, whhere the geo locations are stored.

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
