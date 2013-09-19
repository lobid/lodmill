default files = FLUX_DIR;

"http://services.d-nb.de/oai/repository" |
open-oaipmh(dateFrom="2013-08-11",dateUntil="2013-08-12",metadataPrefix="PicaPlus-xml",setSpec="bib") |
decode-xml |
handle-picaxml |
morph(files+"morph_zdb-isil-file-pica2ld.xml") |
encode-triples-enrich-lobid-organisation(serialization="TURTLE",geonameFilename="geonames_DE_sample.csv") |
write("update_zdb-isil-file2lobid-organisations1.ttl");