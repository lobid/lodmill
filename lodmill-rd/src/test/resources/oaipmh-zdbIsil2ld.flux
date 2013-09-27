default files = FLUX_DIR;

"http://services.d-nb.de/oai/repository" |
open-oaipmh(dateFrom="2013-08-11",dateUntil="2013-08-12",metadataPrefix="PicaPlus-xml",setSpec="bib") |
decode-xml |
handle-picaxml |
morph(files+"morph_zdb-isil-file-pica2ld.xml") |
encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE_sample.csv") |
triples-to-rdfmodel(serialization="TURTLE")|
write-rdfmodel(property="http://purl.org/dc/terms/identifier",endIndex="2",startIndex="0", fileSuffix="nt",serialization="N-TRIPLE",target=files+"tmp");